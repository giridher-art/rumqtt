use flume::Receiver;
use std::{
    io::{self, ErrorKind},
    time::Duration,
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    select,
    time::timeout,
};

use crate::{
    eventloop::ConnectionError,
    xchg::{pipe, XchgPipeA, XchgPipeB},
    AsyncReadWrite, Control, EventsTx, IOEvent,
};

pub struct Connection {
    timeout: usize,
    active: bool,
    connection_to_io: XchgPipeA<u8>,
    io_to_connection: XchgPipeB<u8>,
    control_rx: Receiver<Control>,
    events_tx: EventsTx<IOEvent>,
}

impl Connection {
    pub fn new(
        events_tx: EventsTx<IOEvent>,
        control_rx: Receiver<Control>,
        timeout: usize,
    ) -> (Self, XchgPipeA<u8>, XchgPipeB<u8>) {
        // initialize pipes for connection_to_io:
        //  keep the A in Self, return B
        let (conn_to_io_tx, conn_to_io_rx) = pipe(0, 1024);

        // initialize pipes for io_to_connection:
        //  keep the B in Self, return A
        let (io_to_conn_tx, io_to_conn_rx) = pipe(0, 1024);

        let connection = Connection {
            active: false,
            timeout,
            connection_to_io: conn_to_io_tx,
            io_to_connection: io_to_conn_rx,
            control_rx,
            events_tx,
        };

        (connection, io_to_conn_tx, conn_to_io_rx)
    }
    pub async fn start(
        &mut self,
        mut stream: &mut Box<dyn AsyncReadWrite>,
    ) -> Result<(), ConnectionError> {
        loop {
            // TODO(swanx): we shall improve these names!!
            let has_space = self.connection_to_io.active.remaining_space() > 0;
            let (buffer, size) = self.connection_to_io.active.raw_mut();
            self.active = true;

            let res: Result<(), ConnectionError> = select! {
                // Read from network and fill read buffer
                v = read_from_stream(buffer, &mut stream), if has_space  => {
                    if let Err(e) = v{
                        Err(ConnectionError::Io(e))
                    } else {
                        *size += v.unwrap();
                        // dbg!(_n);
                        if self.connection_to_io.try_forward() {
                            self.events_tx.send_async(IOEvent::ConnectionData).await.unwrap();
                        }
                        Ok(())
                    }
                }
                _ = self.connection_to_io.recycler.wait() => {
                    // dbg!("received back incoming_tx buffer");
                    let standby = self.connection_to_io.recycler.standby().unwrap();
                    match tokio::time::timeout(Duration::from_secs(self.timeout as u64), stream.write_all(standby)).await {
                        Ok(Ok(())) => {
                            self.connection_to_io.recycler.clear();
                            // try to send to active buffer to other end
                            if self.connection_to_io.try_forward() {
                                self.events_tx.send_async(IOEvent::ConnectionData).await.unwrap();
                            }
                            Ok(())
                        }
                        Ok(Err(io_err)) => Err(ConnectionError::Io(io_err)),
                        Err(_elapsed) => {
                            Err(ConnectionError::NetworkTimeout)
                        }
                    }


                }

                data = self.io_to_connection.incoming.recv_async() => {
                   let mut data = data.expect("use ? here");
                    match tokio::time::timeout(Duration::from_secs(self.timeout as u64), stream.write_all(&data)).await {
                        Ok(Ok(())) => {
                            data.clear();
                            self.io_to_connection.ack(data);
                            self.events_tx.send_async(IOEvent::OutgoingDataAck).await.unwrap();
                            Ok(())
                        }
                        Ok(Err(io_err)) => Err(ConnectionError::Io(io_err)),
                        Err(_elapsed) => {
                            Err(ConnectionError::NetworkTimeout)
                        }
                    }
                }
                signal = self.control_rx.recv_async() => {
                    match signal.expect("should recv") {
                        Control::Terminate => return Ok(()),
                        Control::Cleanup => Err(ConnectionError::RequestsDone),

                    }
                }
            };
            if let Err(e) = res {
                self.cleanup(stream).await;
                return Err(e);
            }
        }

        Ok(())
    }

    pub async fn cleanup(&mut self, stream: &mut Box<dyn AsyncReadWrite>) {
        if !self.active {
            self.connection_to_io.clear();
            self.active = false;
            self.control_rx.drain();
            return;
        }

        self.events_tx
            .send_async(IOEvent::ConnectionCleanup)
            .await
            .unwrap();

        self.connection_to_io.recycler.recv_async().await;
        let standby = self.connection_to_io.recycler.standby().unwrap();
        let _ = stream.write_all(standby).await;

        // forward active buffer incase there is still some data.
        if self.connection_to_io.try_forward() {
            self.events_tx
                .send_async(IOEvent::ConnectionData)
                .await
                .unwrap();
        }

        // wait for acks again from previous exchange and write acks to client
        self.connection_to_io.recycler.recv_async().await;
        let standby = self.connection_to_io.recycler.standby().unwrap();
        let _ = stream.write_all(standby).await;

        self.connection_to_io.clear();
        self.active = false;
        self.control_rx.drain();
        self.events_tx.send(IOEvent::ConnectionTerminated).unwrap();
    }
}

async fn read_from_stream<T>(read: &mut Vec<u8>, stream: &mut T) -> Result<usize, std::io::Error>
where
    T: AsyncReadExt + Unpin,
{
    // On the first iteration, `read_buf` fills only up to the remaining capacity
    // of the buffer (`capacity - len`). If there is still spare capacity, it will
    // read as much data as fits into that free space.
    // On subsequent iterations, if the buffer becomes completely full
    // (i.e. `len == capacity`), calling `read_buf` would by default try to grow
    // the Vec’s capacity to accommodate more data. To avoid this automatic
    // reallocation (for better control over memory usage), we return early when
    // the buffer has no remaining capacity.
    if read.capacity() == read.len() {
        return Ok(0);
    }

    match stream.read_buf(read).await {
        Ok(0) => Err(std::io::Error::new(
            std::io::ErrorKind::ConnectionAborted,
            "connection closed by peer",
        )),
        Ok(n) => Ok(n),
        Err(e) => Err(e),
    }
}
