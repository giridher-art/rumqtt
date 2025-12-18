use std::{
    collections::HashMap,
    net::SocketAddr,
    option,
    sync::Arc,
    thread::{self, JoinHandle},
    time::Duration,
};

use futures_util::{StreamExt, io};
use tokio::net::{TcpSocket, TcpStream, lookup_host};

use crate::{
    ClientState, ConnectReturnCode, Control, IOEvent, Message, MqttOptions, NetworkOptions, Packet,
    PubAck, Publish, SubAck, SubscribeReasonCode, UnsubAck, UnsubAckReason,
    client::Client,
    connection::Connection,
    events::{EventsRx, EventsTx},
    message::{self, PubComp, PubRec, QoS},
    protocol::{
        self, Protocol,
        v4::{V4, subscribe},
    },
    state::{
        self,
        v4::{MqttState, OutgoingPacket, StateError},
    },
    xchg::{XchgPipeA, XchgPipeB},
};
#[derive(Debug)]
pub struct ConnectionHandle<P: Protocol> {
    protocol: P,
    inflight_incoming: Vec<u8>,
    connection_to_io: XchgPipeB<u8>,
    io_to_connection: XchgPipeA<u8>,
    max_incoming_packet_size: usize,
    max_outgoing_packet_size: usize,
    control_tx: flume::Sender<Control>,
    packet_buffer: Vec<Packet>,
}

impl<P: Protocol> ConnectionHandle<P> {
    pub fn new(
        event_tx: EventsTx<IOEvent>,
        max_incoming_packet_size: usize,
        max_outgoing_packet_size: usize,
    ) -> (Self, Connection) {
        let (control_tx, control_rx) = flume::bounded(1);
        let (connection, io_to_conn, conn_to_io) = Connection::new(event_tx, control_rx, 100);
        (
            Self {
                protocol: P::new(),
                inflight_incoming: Vec::new(),
                connection_to_io: conn_to_io,
                io_to_connection: io_to_conn,
                control_tx,
                max_incoming_packet_size,
                max_outgoing_packet_size,
                packet_buffer: Vec::new(),
            },
            connection,
        )
    }
    pub fn read_packets(&mut self) -> Result<Vec<Packet>, protocol::Error> {
        // if let Ok(mut buf) = self.connection_to_io.try_recv() {
        //     self.inflight_incoming.append(&mut buf);
        //     self.connection_to_io.ack(buf);
        // }

        // let mut packets = Vec::new();
        // // parse the packet from buf
        // loop {
        //     match self.protocol.read(
        //         &mut &self.inflight_incoming[..],
        //         self.max_incoming_packet_size,
        //     ) {
        //         Ok(packet) => {
        //             packets.push(packet);
        //         }
        //         Err(e) => {
        //             break;
        //         }
        //     };
        // }

        // return Ok(packets);

        // For simulating the connection
        let packets = self.packet_buffer.drain(..).collect::<Vec<_>>();
        println!("ConnectionHandle: read_packets: {:?}", packets);
        Ok(packets)
    }

    pub fn write_packet(&mut self, packet: Packet) -> Result<(), protocol::Error> {
        match packet {
            Packet::Publish(publish) => {
                self.packet_buffer.push(Packet::PubAck(PubAck {
                    pkid: publish.pkid,
                    reason: crate::PubAckReason::Success,
                    properties: None,
                }));
            }

            Packet::PingReq(pinreq) => {
                self.packet_buffer.push(Packet::PingResp(crate::PingResp));
            }

            Packet::Subscribe(sub) => {
                self.packet_buffer.push(Packet::SubAck(SubAck {
                    pkid: sub.pkid,
                    return_codes: vec![
                        SubscribeReasonCode::Success(QoS::AtMostOnce);
                        sub.filters.len()
                    ],
                    properties: None,
                }));

                self.packet_buffer.push(Packet::Publish(Publish::new(
                    sub.filters.get(0).unwrap().path.clone(),
                    QoS::AtLeastOnce,
                    b"hello",
                    false,
                )));
            }
            Packet::Unsubscribe(unsub) => {
                self.packet_buffer.push(Packet::UnsubAck(UnsubAck {
                    pkid: unsub.pkid,
                    reasons: vec![UnsubAckReason::Success; unsub.filters.len()],
                    properties: None,
                }));
            }
            _ => {}
        }
        Ok(())
    }
}

pub struct Eventloop<P: Protocol> {
    // COnnection
    connection: Option<Connection>,
    // Main Event Receiver
    pub event_rx: EventsRx<IOEvent>,
    /// Options of the current mqtt connection
    pub mqtt_options: MqttOptions,
    /// Current state of the connection
    pub state: MqttState<P>,
    //network options
    pub network_options: NetworkOptions,
}

impl<P: Protocol> Eventloop<P> {
    pub fn new(
        options: MqttOptions,
        network_options: NetworkOptions,
        event_rx: EventsRx<IOEvent>,
        clients: Vec<ClientState>,
    ) -> Eventloop<P> {
        let event_tx = event_rx.producer(0);
        let (connection_handle, connection) = ConnectionHandle::<P>::new(
            event_tx,
            options.max_incoming_packet_size,
            options.max_outgoing_packet_size,
        );
        Eventloop {
            connection: Some(connection),
            event_rx,
            state: MqttState::<P>::new(
                options.inflight,
                options.manual_acks.clone(),
                connection_handle,
                clients,
            ),
            network_options,
            mqtt_options: options,
        }
    }

    pub fn add_keep_alive(&mut self, keep_alive: u64) {
        self.event_rx.add_timer(Duration::from_secs(keep_alive));
    }

    pub async fn start(&mut self) -> Result<(), ConnectionError> {
        if let Some(mut connection) = self.connection.take() {
            tokio::spawn(async move {
                connection.start().await;
            });
        }
        while let Some((source_id, event)) = self.event_rx.next().await {
            println!("Eventloop: Received Event: {:?}", event);
            match event {
                IOEvent::ConnectionData => {
                    println!("Eventloop: Connection Data");
                    self.state.read_incoming();
                }
                IOEvent::Refresh => {
                    self.state
                        .handle_outgoing_packet(source_id, Message::Ping)?;
                }
                IOEvent::ClientMessage(msg) => {
                    println!("Eventloop: Client Message: {:?}", msg);
                    self.state.handle_outgoing_packet(source_id, msg);
                }
                IOEvent::Shutdown => {}
                _ => {}
            }
        }
        Ok(())
    }
}
#[derive(Debug, thiserror::Error)]
pub enum ConnectionError {
    #[error("Mqtt state: {0}")]
    MqttState(#[from] StateError),
    #[error("Network timeout")]
    NetworkTimeout,
    #[error("Flush timeout")]
    FlushTimeout,
    // #[cfg(feature = "websocket")]
    // #[error("Websocket: {0}")]
    // Websocket(#[from] async_tungstenite::tungstenite::error::Error),
    // #[cfg(feature = "websocket")]
    // #[error("Websocket Connect: {0}")]
    // WsConnect(#[from] http::Error),
    // #[cfg(any(feature = "use-rustls-no-provider", feature = "use-native-tls"))]
    // #[error("TLS: {0}")]
    // Tls(#[from] tls::Error),
    #[error("I/O: {0}")]
    Io(#[from] io::Error),
    #[error("Connection refused, return code: `{0:?}`")]
    ConnectionRefused(ConnectReturnCode),
    #[error("Expected ConnAck packet, received: {0:?}")]
    NotConnAck(Packet),
    #[error("Requests done")]
    RequestsDone,
    // #[cfg(feature = "websocket")]
    // #[error("Invalid Url: {0}")]
    // InvalidUrl(#[from] UrlError),
    // #[cfg(feature = "proxy")]
    // #[error("Proxy Connect: {0}")]
    // Proxy(#[from] ProxyError),
    // #[cfg(feature = "websocket")]
    // #[error("Websocket response validation error: ")]
    // ResponseValidation(#[from] crate::websockets::ValidationError),
}

// pub(crate) async fn socket_connect(
//     host: String,
//     network_options: NetworkOptions,
// ) -> std::io::Result<TcpStream> {
//     let addrs = lookup_host(host).await?;
//     let mut last_err = None;

//     for addr in addrs {
//         let socket = match addr {
//             SocketAddr::V4(_) => TcpSocket::new_v4()?,
//             SocketAddr::V6(_) => TcpSocket::new_v6()?,
//         };

//         socket.set_nodelay(network_options.tcp_nodelay)?;

//         if let Some(send_buff_size) = network_options.tcp_send_buffer_size {
//             socket.set_send_buffer_size(send_buff_size).unwrap();
//         }
//         if let Some(recv_buffer_size) = network_options.tcp_recv_buffer_size {
//             socket.set_recv_buffer_size(recv_buffer_size).unwrap();
//         }

//         #[cfg(any(target_os = "android", target_os = "fuchsia", target_os = "linux"))]
//         {
//             if let Some(bind_device) = &network_options.bind_device {
//                 // call the bind_device function only if the bind_device network option is defined
//                 // If binding device is None or an empty string it removes the binding,
//                 // which is causing PermissionDenied errors in AWS environment (lambda function).
//                 socket.bind_device(Some(bind_device.as_bytes()))?;
//             }
//         }

//         match socket.connect(addr).await {
//             Ok(s) => return Ok(s),
//             Err(e) => {
//                 last_err = Some(e);
//             }
//         };
//     }

//     Err(last_err.unwrap_or_else(|| {
//         io::Error::new(
//             io::ErrorKind::InvalidInput,
//             "could not resolve to any address",
//         )
//     }))
// }

// async fn network_connect(
//     options: &MqttOptions,
//     network_options: NetworkOptions,
// ) -> Result<impl AsyncReadWrite, ConnectionError> {
//     // Process Unix files early, as proxy is not supported for them.
//     #[cfg(unix)]
//     if matches!(options.transport(), Transport::Unix) {
//         use tokio::net::UnixStream;

//         let file = options.broker_addr.as_str();
//         let socket = UnixStream::connect(Path::new(file)).await?;

//         return Ok(socket);
//     }

//     // For websockets domain and port are taken directly from `broker_addr` (which is a url).
//     let (domain, port) = match options.transport() {
//         #[cfg(feature = "websocket")]
//         Transport::Ws => split_url(&options.broker_addr)?,
//         #[cfg(all(feature = "use-rustls-no-provider", feature = "websocket"))]
//         Transport::Wss(_) => split_url(&options.broker_addr)?,
//         _ => options.broker_address(),
//     };

//     let addr = format!("{domain}:{port}");
//     let stream = socket_connect(addr, network_options).await?;
//     Ok(stream)

// let tcp_stream = {
//     // #[cfg(feature = "proxy")]
//     // match options.proxy() {
//     //     Some(proxy) => proxy.connect(&domain, port, network_options).await?,
//     //     None => {
//     //         let addr = format!("{domain}:{port}");
//     //         socket_connect(addr, network_options).await?
//     //     }
//     // }
//     #[cfg(not(feature = "proxy"))]
//     {
//         let addr = format!("{domain}:{port}");
//         socket_connect(addr, network_options).await
//     }
// };

// let network = match options.transport() {
//     Transport::Tcp => tcp_stream,
//     #[cfg(any(feature = "use-rustls-no-provider", feature = "use-native-tls"))]
//     Transport::Tls(tls_config) => {
//         use crate::tls;

//         let socket =
//             tls::tls_connect(&options.broker_addr, options.port, &tls_config, tcp_stream)
//                 .await?;
//         socket
//     }
//     #[cfg(unix)]
//     Transport::Unix => unreachable!(),
//     #[cfg(feature = "websocket")]
//     Transport::Ws => {
//         let mut request = options.broker_addr.as_str().into_client_request()?;
//         request
//             .headers_mut()
//             .insert("Sec-WebSocket-Protocol", "mqtt".parse().unwrap());

//         if let Some(request_modifier) = options.request_modifier() {
//             request = request_modifier(request).await;
//         }

//         let (socket, response) =
//             async_tungstenite::tokio::client_async(request, tcp_stream).await?;
//         validate_response_headers(response)?;

//         WsStream::new(socket)
//     }
//     #[cfg(all(feature = "use-rustls-no-provider", feature = "websocket"))]
//     Transport::Wss(tls_config) => {
//         let mut request = options.broker_addr.as_str().into_client_request()?;
//         request
//             .headers_mut()
//             .insert("Sec-WebSocket-Protocol", "mqtt".parse().unwrap());

//         if let Some(request_modifier) = options.request_modifier() {
//             request = request_modifier(request).await;
//         }

//         let connector = tls::rustls_connector(&tls_config).await?;

//         let (socket, response) = async_tungstenite::tokio::client_async_tls_with_connector(
//             request,
//             tcp_stream,
//             Some(connector),
//         )
//         .await?;
//         validate_response_headers(response)?;

//         WsStream::new(socket)
//     }
// };
// }
