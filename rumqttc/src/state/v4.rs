use super::*;

use crate::eventloop::ConnectionHandle;
use crate::protocol::v4::{self, subscribe};
use crate::ClientState;
use crate::Incoming;
use crate::{protocol, Router};
use fixedbitset::FixedBitSet;
use std::collections::{HashMap, VecDeque};
use std::ops::Sub;
use std::{io, time::Instant};

/// Errors during state handling
#[derive(Debug, thiserror::Error)]
pub enum StateError {
    /// Io Error while state is passed to network
    #[error("Io error: {0:?}")]
    Io(#[from] io::Error),
    /// Invalid state for a given operation
    #[error("Invalid state for a given operation")]
    InvalidState,
    /// Received a packet (ack) which isn't asked for
    #[error("Received unsolicited ack pkid: {0}")]
    Unsolicited(u16),
    /// Last pingreq isn't acked
    #[error("Last pingreq isn't acked")]
    AwaitPingResp,
    /// Received a wrong packet while waiting for another packet
    #[error("Received a wrong packet while waiting for another packet")]
    WrongPacket,
    #[error("Timeout while waiting to resolve collision")]
    CollisionTimeout,
    #[error("A Subscribe packet must contain atleast one filter")]
    EmptySubscription,
    #[error("Mqtt serialization/deserialization error: {0}")]
    Deserialization(#[from] protocol::Error),
    #[error("Connection closed by peer abruptly")]
    ConnectionAborted,
}

#[derive(Debug, Clone)]
pub struct OutgoingPacket {
    pub client_id: usize,
    pub token_id: usize,
    pub packet: Packet,
}

/// State of the mqtt connection.
// Design: Methods will just modify the state of the object without doing any network operations
// Design: All inflight queues are maintained in a pre initialized vec with index as packet id.
// This is done for 2 reasons
// Bad acks or out of order acks aren't O(n) causing cpu spikes
// Any missing acks from the broker are detected during the next recycled use of packet ids
#[derive(Debug)]
pub struct MqttState<P: Protocol> {
    /// Connection handle
    pub connection_handle: ConnectionHandle<P>,
    /// Status of last ping
    pub await_pingresp: bool,
    /// Collision ping count. Collisions stop user requests
    /// which inturn trigger pings. Multiple pings without
    /// resolving collisions will result in error
    pub collision_ping_count: usize,
    /// Last incoming packet time
    last_incoming: Instant,
    /// Last outgoing packet time
    last_outgoing: Instant,
    /// Packet id of the last outgoing packet
    pub(crate) last_pkid: u16,
    /// Packet id of the last acked packet
    pub(crate) last_ack: u16,
    /// Packet ids of released QoS 2 publishes
    pub(crate) outgoing_rel: FixedBitSet,
    /// Packet ids on incoming QoS 2 publishes
    pub(crate) incoming_pub: FixedBitSet,
    /// Number of outgoing inflight publishes
    pub(crate) inflight: u16,
    /// Maximum number of allowed inflight
    pub(crate) max_inflight: u16,
    /// Outgoing publishes and subscribes which havent recieved acks
    pub(crate) outgoing_packet: Vec<Option<OutgoingPacket>>,
    /// Last collision due to broker not acking in order
    pub collision: Option<(u16, OutgoingPacket)>,
    /// Indicates if acknowledgements should be send immediately
    pub manual_acks: bool,
    // Packets
    pub packets: Vec<Incoming>,
    // Router
    pub router: Router,
}

impl<P: Protocol> MqttState<P> {
    /// Creates new mqtt state. Same state should be used during a
    /// connection for persistent sessions while new state should
    /// instantiated for clean sessions
    pub fn new(
        max_inflight: u16,
        manual_acks: bool,
        connection_handle: ConnectionHandle<P>,
        clients: Vec<ClientState>,
    ) -> Self {
        MqttState {
            connection_handle,
            await_pingresp: false,
            collision_ping_count: 0,
            outgoing_rel: FixedBitSet::with_capacity(max_inflight as usize),
            incoming_pub: FixedBitSet::with_capacity(u16::MAX as usize + 1),
            last_incoming: Instant::now(),
            last_outgoing: Instant::now(),
            last_pkid: 0,
            last_ack: 0,
            inflight: 0,
            max_inflight,
            packets: Vec::new(),
            // index 0 is wasted as 0 is not a valid packet id
            outgoing_packet: vec![None; max_inflight as usize + 1],
            collision: None,
            manual_acks,
            router: Router::new(clients),
        }
    }

    pub fn read_incoming(&mut self) {
        if let Ok(()) = self.connection_handle.read_packets(&mut self.packets) {
            let packets = self.packets.drain(..).collect::<Vec<_>>();
            for packet in packets {
                self.handle_incoming_packet(packet);
            }
        }
    }

    // Implement clean for according to new architecture of the connection
    /// Returns inflight outgoing packets and clears internal queues
    // fn clean(&mut self)  {
    //     let mut pending = Vec::with_capacity(100);
    //     let (first_half, second_half) = self
    //         .outgoing_pub
    //         .split_at_mut(self.last_puback as usize + 1);

    //     for publish in second_half.iter_mut().chain(first_half) {
    //         if let Some(publish) = publish.take() {
    //             let request = ::Publish(publish);
    //             pending.push(request);
    //         }
    //     }

    //     // remove and collect pending releases
    //     for pkid in self.outgoing_rel.ones() {
    //         let request = Request::PubRel(PubRel::new(pkid as u16));
    //         pending.push(request);
    //     }
    //     self.outgoing_rel.clear();

    //     // remove packet ids of incoming qos2 publishes
    //     self.incoming_pub.clear();

    //     self.await_pingresp = false;
    //     self.collision_ping_count = 0;
    //     self.inflight = 0;
    //     pending
    // }

    fn inflight(&self) -> u16 {
        self.inflight
    }

    /// Consolidates handling of all outgoing mqtt packet logic. Returns a packet which should
    /// be put on to the network by the eventloop
    pub fn handle_outgoing_packet(
        &mut self,
        client_id: usize,
        message: Message,
    ) -> Result<(), StateError> {
        let packet = match message {
            Message::Publish(pubreq) => self.outgoing_publish(client_id, pubreq),
            Message::Ping => self.outgoing_ping(),
            Message::PublishAck(pubresp) => self.outgoing_puback(pubresp),
            Message::PublishRec(pubrec) => self.outgoing_pubrec(pubrec),
            Message::Subscribe(subreq) => self.outgoing_subscribe(client_id, subreq),
            Message::UnSub(unsub) => self.outgoing_unsubscribe(client_id, unsub),

            _ => unimplemented!(),
        };

        self.last_outgoing = Instant::now();
        if let Ok(out_packet) = packet {
            if let Some(pac) = out_packet {
                // println!("Outgoing Packet: {:?}", pac);
                self.connection_handle.write_packet(pac);
            }
        }
        Ok(())
    }

    /// Consolidates handling of all incoming mqtt packets. Returns a `Notification` which for the
    /// user to consume and `Packet` which for the eventloop to put on the network
    /// E.g For incoming QoS1 publish packet, this method returns (Publish, Puback). Publish packet will
    /// be forwarded to user and Pubck packet will be written to network
    pub fn handle_incoming_packet(&mut self, packet: Incoming) -> Result<(), StateError> {
        let outgoing = match &packet {
            Incoming::PingResp(_pingresp) => self.handle_incoming_pingresp()?,
            Incoming::Publish(publish) => self.handle_incoming_publish(publish)?,
            Incoming::SubAck(suback) => self.handle_incoming_suback(suback)?,
            Incoming::UnsubAck(unsuback) => self.handle_incoming_unsuback(unsuback)?,
            Incoming::PubAck(puback) => self.handle_incoming_puback(puback)?,
            Incoming::PubRec(pubrec) => self.handle_incoming_pubrec(pubrec)?,
            Incoming::PubRel(pubrel) => self.handle_incoming_pubrel(pubrel)?,
            Incoming::PubComp(pubcomp) => self.handle_incoming_pubcomp(pubcomp)?,
            _ => {
                error!("Invalid incoming packet = {:?}", packet);
                return Err(StateError::WrongPacket);
            }
        };
        self.last_incoming = Instant::now();

        if let Some(packet) = outgoing {
            self.connection_handle.write_packet(packet);
        }
        Ok(())
    }

    fn handle_incoming_suback(&mut self, suback: &SubAck) -> Result<Option<Packet>, StateError> {
        let subscribe_packet = self
            .outgoing_packet
            .get_mut(suback.pkid as usize)
            .ok_or(StateError::Unsolicited(suback.pkid))?;

        self.last_ack = suback.pkid;

        if let Some(outgoing_packet) = subscribe_packet.take() {
            match outgoing_packet.packet {
                Packet::Subscribe(sub) => {
                    let res = sub
                        .filters
                        .iter()
                        .zip(suback.return_codes.iter())
                        .collect::<Vec<_>>();
                    for (filter, allowed) in res {
                        match allowed {
                            SubscribeReasonCode::QoS0
                            | SubscribeReasonCode::QoS1
                            | SubscribeReasonCode::QoS2
                            | SubscribeReasonCode::Success(_) => {
                                if self.manual_acks {
                                    self.router.add_log(
                                        filter.path.clone(),
                                        crate::SubscritptionStrategy::RoundRobin,
                                    );
                                } else {
                                    self.router.add_log(
                                        filter.path.clone(),
                                        crate::SubscritptionStrategy::Broadcast,
                                    );
                                }
                            }
                            _ => {}
                        }
                    }
                }
                _ => return Err(StateError::Unsolicited(suback.pkid)),
            }
            let msg = SubscribeResp {
                token_id: outgoing_packet.token_id,
                suback: suback.return_codes.clone(),
                properties: None,
            };
            self.router
                .ack(outgoing_packet.client_id, Message::SubscribeAck(msg));
        } else {
            error!("Unsolicited puback packet: {:?}", suback.pkid);
            return Err(StateError::Unsolicited(suback.pkid));
        }

        self.inflight -= 1;
        let packet = self.check_collision(suback.pkid).map(|outgoing_packet| {
            let packet = outgoing_packet.packet.clone();
            self.outgoing_packet[suback.pkid as usize] = Some(outgoing_packet);
            self.inflight += 1;

            self.collision_ping_count = 0;

            packet
        });

        Ok(None)
    }

    fn handle_incoming_unsuback(
        &mut self,
        unsuback: &UnsubAck,
    ) -> Result<Option<Packet>, StateError> {
        let subscribe_packet = self
            .outgoing_packet
            .get_mut(unsuback.pkid as usize)
            .ok_or(StateError::Unsolicited(unsuback.pkid))?;
        self.last_ack = unsuback.pkid;
        if let Some(outgoing_packet) = subscribe_packet.take() {
            let msg = UnSubscribeResp {
                token_id: outgoing_packet.token_id,
                reason_codes: unsuback.reasons.clone(),
            };
            self.router
                .ack(outgoing_packet.client_id, Message::UnSubAck(msg));
            match outgoing_packet.packet {
                Packet::Unsubscribe(unsub) => {
                    let res = unsub
                        .filters
                        .iter()
                        .zip(unsuback.reasons.iter())
                        .collect::<Vec<_>>();
                    for (filter, reason) in res {
                        match reason {
                            UnsubAckReason::Success => {
                                self.router
                                    .remove_subscription(outgoing_packet.client_id, filter);
                            }
                            _ => {}
                        }
                    }
                }
                _ => {}
            }

            let msg = UnSubscribeResp {
                token_id: outgoing_packet.token_id,
                reason_codes: unsuback.reasons.clone(),
            };

            self.router
                .ack(outgoing_packet.client_id, Message::UnSubAck(msg));
        }
        Ok(None)
    }

    /// Results in a publish notification in all the QoS cases. Replys with an ack
    /// in case of QoS1 and Replys rec in case of QoS while also storing the message
    fn handle_incoming_publish(&mut self, publish: &Publish) -> Result<Option<Packet>, StateError> {
        let qos = publish.qos;
        self.router.publish(publish.clone());
        match qos {
            QoS::AtMostOnce => Ok(None),
            QoS::AtLeastOnce => {
                if !self.manual_acks {
                    let puback = PublishResp {
                        token_id: 0,
                        puback: PubAck {
                            pkid: publish.pkid,
                            reason: PubAckReason::Success,
                            properties: None,
                        },
                    };
                    return self.outgoing_puback(puback);
                }
                Ok(None)
            }
            QoS::ExactlyOnce => {
                let pkid = publish.pkid;
                self.incoming_pub.insert(pkid as usize);

                if !self.manual_acks {
                    let pubrec = PublishRec {
                        token_id: 0,
                        pubrec: PubRec {
                            pkid,
                            reason: PubRecReason::Success,
                            properties: None,
                        },
                    };
                    return self.outgoing_pubrec(pubrec);
                }
                Ok(None)
            }
        }
    }

    fn handle_incoming_puback(&mut self, puback: &PubAck) -> Result<Option<Packet>, StateError> {
        let publish_packet = self
            .outgoing_packet
            .get_mut(puback.pkid as usize)
            .ok_or(StateError::Unsolicited(puback.pkid))?;

        self.last_ack = puback.pkid;

        if let Some(outgoing_packet) = publish_packet.take() {
            let msg = PublishResp {
                token_id: outgoing_packet.token_id,
                puback: puback.clone(),
            };
            self.router
                .ack(outgoing_packet.client_id, Message::PublishAck(msg));
        } else {
            error!("Unsolicited puback packet: {:?}", puback.pkid);
            return Err(StateError::Unsolicited(puback.pkid));
        }

        self.inflight -= 1;
        let packet = self.check_collision(puback.pkid).map(|outgoing_packet| {
            let packet = outgoing_packet.packet.clone();
            self.outgoing_packet[puback.pkid as usize] = Some(outgoing_packet);
            self.inflight += 1;

            self.collision_ping_count = 0;

            packet
        });

        Ok(packet)
    }

    fn handle_incoming_pubrec(&mut self, pubrec: &PubRec) -> Result<Option<Packet>, StateError> {
        let publish = self
            .outgoing_packet
            .get_mut(pubrec.pkid as usize)
            .ok_or(StateError::Unsolicited(pubrec.pkid))?;

        if publish.is_none() {
            error!("Unsolicited pubrec packet: {:?}", pubrec.pkid);
            return Err(StateError::Unsolicited(pubrec.pkid));
        }

        // NOTE: Inflight - 1 for qos2 in comp
        self.outgoing_rel.insert(pubrec.pkid as usize);
        let pubrel = PubRel {
            pkid: pubrec.pkid,
            reason: PubRelReason::Success,
            properties: None,
        };

        Ok(Some(Packet::PubRel(pubrel)))
    }

    fn handle_incoming_pubrel(&mut self, pubrel: &PubRel) -> Result<Option<Packet>, StateError> {
        if !self.incoming_pub.contains(pubrel.pkid as usize) {
            error!("Unsolicited pubrel packet: {:?}", pubrel.pkid);
            return Err(StateError::Unsolicited(pubrel.pkid));
        }

        self.incoming_pub.set(pubrel.pkid as usize, false);
        let pubcomp = PubComp {
            pkid: pubrel.pkid,
            reason: PubCompReason::Success,
            properties: None,
        };

        Ok(Some(Packet::PubComp(pubcomp)))
    }

    fn handle_incoming_pubcomp(&mut self, pubcomp: &PubComp) -> Result<Option<Packet>, StateError> {
        if !self.outgoing_rel.contains(pubcomp.pkid as usize) {
            error!("Unsolicited pubcomp packet: {:?}", pubcomp.pkid);
            return Err(StateError::Unsolicited(pubcomp.pkid));
        }
        let publish_packet = self
            .outgoing_packet
            .get_mut(pubcomp.pkid as usize)
            .ok_or(StateError::Unsolicited(pubcomp.pkid))?;

        if let Some(outgoing_packet) = publish_packet.take() {
            let msg = PublishComp {
                token_id: outgoing_packet.token_id,
                pubcomp: pubcomp.clone(),
            };
            self.router
                .ack(outgoing_packet.client_id, Message::PublishComp(msg));
        } else {
            error!("Unsolicited puback packet: {:?}", pubcomp.pkid);
            return Err(StateError::Unsolicited(pubcomp.pkid));
        }

        self.outgoing_rel.set(pubcomp.pkid as usize, false);
        self.inflight -= 1;
        let packet = self.check_collision(pubcomp.pkid).map(|outgoing_packet| {
            let packet = outgoing_packet.packet.clone();
            self.outgoing_packet[pubcomp.pkid as usize] = Some(outgoing_packet);
            self.inflight += 1;

            self.collision_ping_count = 0;

            packet
        });

        Ok(packet)
    }

    fn handle_incoming_pingresp(&mut self) -> Result<Option<Packet>, StateError> {
        self.await_pingresp = false;

        Ok(None)
    }

    /// Adds next packet identifier to QoS 1 and 2 publish packets and returns
    /// it buy wrapping publish in packet
    fn outgoing_publish(
        &mut self,
        client_id: usize,
        mut publishreq: PublishReq,
    ) -> Result<Option<Packet>, StateError> {
        let mut publish = publishreq.publish;
        let outgoing_packet = OutgoingPacket {
            client_id,
            token_id: publishreq.token_id,
            packet: Packet::Publish(publish.clone()),
        };

        if publish.qos != QoS::AtMostOnce {
            if publish.pkid == 0 {
                publish.pkid = self.next_pkid();
            }
            let pkid = publish.pkid;
            if self
                .outgoing_packet
                .get(publish.pkid as usize)
                .ok_or(StateError::Unsolicited(publish.pkid))?
                .is_some()
            {
                info!("Collision on packet id = {:?}", publish.pkid);

                self.collision = Some((pkid, outgoing_packet));

                return Ok(None);
            }

            // if there is an existing publish at this pkid, this implies that broker hasn't acked this
            // packet yet. This error is possible only when broker isn't acking sequentially
            self.outgoing_packet[pkid as usize] = Some(outgoing_packet);
            self.inflight += 1;
        };

        debug!(
            "Publish. Topic = {}, Pkid = {:?}, Payload Size = {:?}",
            publish.topic,
            publish.pkid,
            publish.payload.len()
        );
        println!(
            "Publish. Topic = {}, Pkid = {:?}, Payload Size = {:?}",
            publish.topic,
            publish.pkid,
            publish.payload.len()
        );
        Ok(Some(Packet::Publish(publish)))
    }

    fn outgoing_pubrel(&mut self, pubrel: PublishRel) -> Result<Option<Packet>, StateError> {
        let pubrel = self.save_pubrel(pubrel.pubrel)?;
        debug!("Pubrel. Pkid = {}", pubrel.pkid);
        Ok(Some(Packet::PubRel(pubrel)))
    }

    fn outgoing_puback(&mut self, puback: PublishResp) -> Result<Option<Packet>, StateError> {
        Ok(Some(Packet::PubAck(puback.puback)))
    }

    fn outgoing_pubrec(&mut self, pubrec: PublishRec) -> Result<Option<Packet>, StateError> {
        Ok(Some(Packet::PubRec(pubrec.pubrec)))
    }

    /// check when the last control packet/pingreq packet is received and return
    /// the status which tells if keep alive time has exceeded
    /// NOTE: status will be checked for zero keepalive times also
    fn outgoing_ping(&mut self) -> Result<Option<Packet>, StateError> {
        let elapsed_in = self.last_incoming.elapsed();
        let elapsed_out = self.last_outgoing.elapsed();

        if self.collision.is_some() {
            self.collision_ping_count += 1;
            if self.collision_ping_count >= 2 {
                return Err(StateError::CollisionTimeout);
            }
        }

        // raise error if last ping didn't receive ack
        if self.await_pingresp {
            return Err(StateError::AwaitPingResp);
        }

        self.await_pingresp = true;

        debug!(
            "Pingreq,
            last incoming packet before {} millisecs,
            last outgoing request before {} millisecs",
            elapsed_in.as_millis(),
            elapsed_out.as_millis()
        );

        Ok(Some(Packet::PingReq(PingReq)))
    }

    fn outgoing_subscribe(
        &mut self,
        client_id: usize,
        mut subreq: SubscribeReq,
    ) -> Result<Option<Packet>, StateError> {
        let mut subscribe = subreq.subscribe.clone();
        let outgoing_packet = OutgoingPacket {
            client_id,
            token_id: subreq.token_id,
            packet: Packet::Subscribe(subreq.subscribe),
        };

        if subscribe.pkid == 0 {
            subscribe.pkid = self.next_pkid();
        }

        let pkid = subscribe.pkid;
        if self
            .outgoing_packet
            .get(subscribe.pkid as usize)
            .ok_or(StateError::Unsolicited(pkid))?
            .is_some()
        {
            info!("Collision on packet id = {:?}", pkid);
            self.collision = Some((pkid, outgoing_packet));

            return Ok(None);
        }

        // if there is an existing publish at this pkid, this implies that broker hasn't acked this
        // packet yet. This error is possible only when broker isn't acking sequentially
        self.outgoing_packet[pkid as usize] = Some(outgoing_packet);
        self.inflight += 1;

        debug!(
            "Subscribe. Topics = {:?}, Pkid = {:?}",
            subscribe.filters, pkid
        );

        Ok(Some(Packet::Subscribe(subscribe)))
    }

    fn outgoing_unsubscribe(
        &mut self,
        client_id: usize,
        mut unsubreq: UnSubscribeReq,
    ) -> Result<Option<Packet>, StateError> {
        let mut unsub = unsubreq.unsub.clone();
        let outgoing_packet = OutgoingPacket {
            client_id,
            token_id: unsubreq.token_id,
            packet: Packet::Unsubscribe(unsubreq.unsub),
        };

        if unsub.pkid == 0 {
            unsub.pkid = self.next_pkid();
        }

        let pkid = unsub.pkid;
        if self
            .outgoing_packet
            .get(pkid as usize)
            .ok_or(StateError::Unsolicited(pkid))?
            .is_some()
        {
            info!("Collision on packet id = {:?}", pkid);

            self.collision = Some((pkid, outgoing_packet));

            return Ok(None);
        }

        // if there is an existing publish at this pkid, this implies that broker hasn't acked this
        // packet yet. This error is possible only when broker isn't acking sequentially
        self.outgoing_packet[pkid as usize] = Some(outgoing_packet);
        self.inflight += 1;

        debug!(
            "Unsubscribe. Topics = {:?}, Pkid = {:?}",
            unsub.filters, unsub.pkid
        );

        Ok(Some(Packet::Unsubscribe(unsub)))
    }

    fn outgoing_disconnect(&mut self) -> Result<Option<Packet>, StateError> {
        debug!("Disconnect");
        let disconnet = Disconnect {
            reason_code: DisconnectReasonCode::NormalDisconnection,
            properties: None,
        };
        Ok(Some(Packet::Disconnect(disconnet)))
    }

    fn check_collision(&mut self, pkid: u16) -> Option<OutgoingPacket> {
        if let Some((packetkid, publish)) = &self.collision {
            if !*packetkid == pkid {
                return None;
            }
            if let Some((id, packet)) = self.collision.take() {
                return Some(packet);
            }
        }

        None
    }

    fn save_pubrel(&mut self, mut pubrel: PubRel) -> Result<PubRel, StateError> {
        let pubrel = match pubrel.pkid {
            // consider PacketIdentifier(0) as uninitialized packets
            0 => {
                pubrel.pkid = self.next_pkid();
                pubrel
            }
            _ => pubrel,
        };

        self.outgoing_rel.insert(pubrel.pkid as usize);
        self.inflight += 1;
        Ok(pubrel)
    }

    /// http://stackoverflow.com/questions/11115364/mqtt-messageid-practical-implementation
    /// Packet ids are incremented till maximum set inflight messages and reset to 1 after that.
    ///
    fn next_pkid(&mut self) -> u16 {
        let next_pkid = self.last_pkid + 1;

        // When next packet id is at the edge of inflight queue,
        // set await flag. This instructs eventloop to stop
        // processing requests until all the inflight publishes
        // are acked
        if next_pkid == self.max_inflight {
            self.last_pkid = 0;
            return next_pkid;
        }

        self.last_pkid = next_pkid;
        next_pkid
    }
}

#[cfg(test)]
mod test {
    use super::{MqttState, StateError};
    use crate::eventloop::ConnectionHandle;
    use crate::protocol::v4::*;
    use crate::{connection, message::*};
    use crate::{EventsRx, IOEvent, Incoming};

    fn build_outgoing_publish(q: QoS) -> PublishReq {
        let topic = "hello/world".to_owned();
        let payload = vec![1, 2, 3];

        let mut publish = Publish::new(topic, QoS::AtLeastOnce, payload, false);
        publish.qos = q;
        PublishReq {
            token_id: 0,
            publish,
        }
    }

    fn build_incoming_publish(q: QoS, pkid: u16) -> Publish {
        let topic = "hello/world".to_owned();
        let payload = vec![1, 2, 3];

        let mut publish = Publish::new(topic, QoS::AtLeastOnce, payload, false);
        publish.pkid = pkid;
        publish.qos = q;
        publish
    }

    fn build_mqttstate() -> MqttState<V4> {
        let event_rx: EventsRx<IOEvent> = EventsRx::new(128);
        let event_tx = event_rx.producer(0);
        let (connection, _) = ConnectionHandle::new(event_tx, 1024, 1024);
        MqttState::new(100 as u16, false, connection, Vec::new())
    }

    #[test]
    fn next_pkid_increments_as_expected() {
        let mut mqtt = build_mqttstate();

        for i in 1..=100 {
            let pkid = mqtt.next_pkid();

            // loops between 0-99. % 100 == 0 implies border
            let expected = i % 100;
            if expected == 0 {
                break;
            }

            assert_eq!(expected, pkid);
        }
    }

    #[test]
    fn outgoing_publish_should_set_pkid_and_add_publish_to_queue() {
        let mut mqtt = build_mqttstate();

        // QoS0 Publish
        let publish = build_outgoing_publish(QoS::AtMostOnce);

        // QoS 0 publish shouldn't be saved in queue
        mqtt.outgoing_publish(0, publish).unwrap();
        assert_eq!(mqtt.last_pkid, 0);
        assert_eq!(mqtt.inflight, 0);

        // QoS1 Publish
        let publish = build_outgoing_publish(QoS::AtLeastOnce);

        // Packet id should be set and publish should be saved in queue
        mqtt.outgoing_publish(0, publish).unwrap();
        assert_eq!(mqtt.last_pkid, 1);
        assert_eq!(mqtt.inflight, 1);
        // QoS1 Publish
        let publish = build_outgoing_publish(QoS::AtLeastOnce);
        // Packet id should be incremented and publish should be saved in queue
        mqtt.outgoing_publish(0, publish).unwrap();
        assert_eq!(mqtt.last_pkid, 2);
        assert_eq!(mqtt.inflight, 2);

        // QoS1 Publish
        let publish = build_outgoing_publish(QoS::ExactlyOnce);

        // Packet id should be set and publish should be saved in queue
        mqtt.outgoing_publish(0, publish).unwrap();
        assert_eq!(mqtt.last_pkid, 3);
        assert_eq!(mqtt.inflight, 3);

        // QoS1 Publish
        let publish = build_outgoing_publish(QoS::ExactlyOnce);
        // Packet id should be incremented and publish should be saved in queue
        mqtt.outgoing_publish(0, publish).unwrap();
        assert_eq!(mqtt.last_pkid, 4);
        assert_eq!(mqtt.inflight, 4);
    }

    #[test]
    fn incoming_publish_should_be_added_to_queue_correctly() {
        let mut mqtt = build_mqttstate();

        // QoS0, 1, 2 Publishes
        let publish1 = build_incoming_publish(QoS::AtMostOnce, 1);
        let publish2 = build_incoming_publish(QoS::AtLeastOnce, 2);
        let publish3 = build_incoming_publish(QoS::ExactlyOnce, 3);

        mqtt.handle_incoming_publish(&publish1).unwrap();
        mqtt.handle_incoming_publish(&publish2).unwrap();
        mqtt.handle_incoming_publish(&publish3).unwrap();

        // only qos2 publish should be add to queue
        assert!(mqtt.incoming_pub.contains(3));
    }

    #[test]
    fn incoming_publish_should_be_acked() {
        let mut mqtt = build_mqttstate();

        // QoS0, 1, 2 Publishes
        let publish1 = build_incoming_publish(QoS::AtMostOnce, 1);
        let publish2 = build_incoming_publish(QoS::AtLeastOnce, 2);
        let publish3 = build_incoming_publish(QoS::ExactlyOnce, 3);
        let mut packets = Vec::new();
        packets.push(mqtt.handle_incoming_publish(&publish1).unwrap());
        packets.push(mqtt.handle_incoming_publish(&publish2).unwrap());
        packets.push(mqtt.handle_incoming_publish(&publish3).unwrap());

        let packet_count = packets.iter().filter(|p| p.is_some()).count();
        assert_eq!(packet_count, 2);
    }

    #[test]
    fn incoming_publish_should_not_be_acked_with_manual_acks() {
        let mut mqtt = build_mqttstate();
        mqtt.manual_acks = true;

        // QoS0, 1, 2 Publishes
        let publish1 = build_incoming_publish(QoS::AtMostOnce, 1);
        let publish2 = build_incoming_publish(QoS::AtLeastOnce, 2);
        let publish3 = build_incoming_publish(QoS::ExactlyOnce, 3);
        let mut packets = Vec::new();
        packets.push(mqtt.handle_incoming_publish(&publish1).unwrap());
        packets.push(mqtt.handle_incoming_publish(&publish2).unwrap());
        packets.push(mqtt.handle_incoming_publish(&publish3).unwrap());

        let packet_count = packets.iter().filter(|p| p.is_some()).count();

        assert_eq!(packet_count, 0);
    }

    #[test]
    fn incoming_qos2_publish_should_send_rec_to_network_and_publish_to_user() {
        let mut mqtt = build_mqttstate();
        let publish = build_incoming_publish(QoS::ExactlyOnce, 1);

        let packet = mqtt.handle_incoming_publish(&publish).unwrap().unwrap();
        match packet {
            Packet::PubRec(pubrec) => assert_eq!(pubrec.pkid, 1),
            _ => panic!("Invalid network request: {:?}", packet),
        }
    }

    #[test]
    fn incoming_puback_should_remove_correct_publish_from_queue() {
        let mut mqtt = build_mqttstate();

        let publish1 = build_outgoing_publish(QoS::AtLeastOnce);
        let publish2 = build_outgoing_publish(QoS::ExactlyOnce);

        mqtt.outgoing_publish(0, publish1).unwrap();
        mqtt.outgoing_publish(0, publish2).unwrap();
        assert_eq!(mqtt.inflight, 2);

        mqtt.handle_incoming_puback(&PubAck {
            pkid: 1,
            reason: PubAckReason::Success,
            properties: None,
        })
        .unwrap();
        assert_eq!(mqtt.inflight, 1);

        mqtt.handle_incoming_puback(&PubAck {
            pkid: 2,
            reason: PubAckReason::Success,
            properties: None,
        })
        .unwrap();
        assert_eq!(mqtt.inflight, 0);

        assert!(mqtt.outgoing_packet[1].is_none());
        assert!(mqtt.outgoing_packet[2].is_none());
    }

    #[test]
    fn incoming_puback_with_pkid_greater_than_max_inflight_should_be_handled_gracefully() {
        let mut mqtt = build_mqttstate();

        let got = mqtt
            .handle_incoming_puback(&PubAck {
                pkid: 101,
                reason: PubAckReason::Success,
                properties: None,
            })
            .unwrap_err();

        match got {
            StateError::Unsolicited(pkid) => assert_eq!(pkid, 101),
            e => panic!("Unexpected error: {}", e),
        }
    }

    #[test]
    fn incoming_pubrec_should_release_publish_from_queue_and_add_relid_to_rel_queue() {
        let mut mqtt = build_mqttstate();

        let publish1 = build_outgoing_publish(QoS::AtLeastOnce);
        let publish2 = build_outgoing_publish(QoS::ExactlyOnce);

        let _publish_out = mqtt.outgoing_publish(0, publish1);
        let _publish_out = mqtt.outgoing_publish(0, publish2);

        mqtt.handle_incoming_pubrec(&PubRec {
            pkid: 2,
            reason: PubRecReason::Success,
            properties: None,
        })
        .unwrap();
        assert_eq!(mqtt.inflight, 2);

        // check if the remaining element's pkid is 1
        let backup = mqtt.outgoing_packet[1].clone();
        match backup.unwrap().packet {
            Packet::Publish(_) => {}
            _ => {
                panic!("Received un expected packet")
            }
        }

        // check if the qos2 element's release pkid is 2
        assert!(mqtt.outgoing_rel.contains(2));
    }

    #[test]
    fn incoming_pubrec_should_send_release_to_network_and_nothing_to_user() {
        let mut mqtt = build_mqttstate();

        let publish = build_outgoing_publish(QoS::ExactlyOnce);
        let packet = mqtt.outgoing_publish(0, publish).unwrap().unwrap();
        match packet {
            Packet::Publish(publish) => assert_eq!(publish.pkid, 1),
            packet => panic!("Invalid network request: {:?}", packet),
        }

        let packet = mqtt
            .handle_incoming_pubrec(&PubRec {
                pkid: 1,
                reason: PubRecReason::Success,
                properties: None,
            })
            .unwrap()
            .unwrap();
        match packet {
            Packet::PubRel(pubrel) => assert_eq!(pubrel.pkid, 1),
            packet => panic!("Invalid network request: {:?}", packet),
        }
    }

    #[test]
    fn incoming_pubrel_should_send_comp_to_network_and_nothing_to_user() {
        let mut mqtt = build_mqttstate();
        let publish = build_incoming_publish(QoS::ExactlyOnce, 1);

        let packet = mqtt.handle_incoming_publish(&publish).unwrap().unwrap();
        match packet {
            Packet::PubRec(pubrec) => assert_eq!(pubrec.pkid, 1),
            packet => panic!("Invalid network request: {:?}", packet),
        }

        let packet = mqtt
            .handle_incoming_pubrel(&PubRel {
                pkid: 1,
                reason: PubRelReason::Success,
                properties: None,
            })
            .unwrap()
            .unwrap();
        match packet {
            Packet::PubComp(pubcomp) => assert_eq!(pubcomp.pkid, 1),
            packet => panic!("Invalid network request: {:?}", packet),
        }
    }

    #[test]
    fn incoming_pubcomp_should_release_correct_pkid_from_release_queue() {
        let mut mqtt = build_mqttstate();
        let publish = build_outgoing_publish(QoS::ExactlyOnce);

        mqtt.outgoing_publish(0, publish).unwrap();
        mqtt.handle_incoming_pubrec(&PubRec {
            pkid: 1,
            reason: PubRecReason::Success,
            properties: None,
        })
        .unwrap();

        mqtt.handle_incoming_pubcomp(&PubComp {
            pkid: 1,
            reason: PubCompReason::Success,
            properties: None,
        })
        .unwrap();
        assert_eq!(mqtt.inflight, 0);
    }

    #[test]
    fn outgoing_ping_handle_should_throw_errors_for_no_pingresp() {
        let mut mqtt = build_mqttstate();
        mqtt.outgoing_ping().unwrap();

        // network activity other than pingresp
        let publish = build_outgoing_publish(QoS::AtLeastOnce);
        mqtt.handle_outgoing_packet(0, Message::Publish(publish))
            .unwrap();
        mqtt.handle_incoming_packet(Incoming::PubAck(PubAck {
            pkid: 1,
            reason: PubAckReason::Success,
            properties: None,
        }))
        .unwrap();

        // should throw error because we didn't get pingresp for previous ping
        match mqtt.outgoing_ping() {
            Ok(_) => panic!("Should throw pingresp await error"),
            Err(StateError::AwaitPingResp) => (),
            Err(e) => panic!("Should throw pingresp await error. Error = {:?}", e),
        }
    }

    #[test]
    fn outgoing_ping_handle_should_succeed_if_pingresp_is_received() {
        let mut mqtt = build_mqttstate();

        // should ping
        mqtt.outgoing_ping().unwrap();
        mqtt.handle_incoming_packet(Incoming::PingResp(PingResp))
            .unwrap();

        // should ping
        mqtt.outgoing_ping().unwrap();
    }

    // #[test]
    // fn clean_is_calculating_pending_correctly() {
    //     let mut mqtt = build_mqttstate();

    //     fn build_outgoing_pub() -> Vec<Option<Publish>> {
    //         vec![
    //             None,
    //             Some(Publish {
    //                 dup: false,
    //                 qos: QoS::AtMostOnce,
    //                 retain: false,
    //                 topic: "test".to_string(),
    //                 pkid: 1,
    //                 payload: "".into(),
    //                 properties: None,
    //             }),
    //             Some(Publish {
    //                 dup: false,
    //                 qos: QoS::AtMostOnce,
    //                 retain: false,
    //                 topic: "test".to_string(),
    //                 pkid: 2,
    //                 payload: "".into(),
    //                 properties: None,
    //             }),
    //             Some(Publish {
    //                 dup: false,
    //                 qos: QoS::AtMostOnce,
    //                 retain: false,
    //                 topic: "test".to_string(),
    //                 pkid: 3,
    //                 payload: "".into(),
    //                 properties: None,
    //             }),
    //             None,
    //             None,
    //             Some(Publish {
    //                 dup: false,
    //                 qos: QoS::AtMostOnce,
    //                 retain: false,
    //                 topic: "test".to_string(),
    //                 pkid: 6,
    //                 payload: "".into(),
    //                 properties: None,
    //             }),
    //         ]
    //     }

    //     mqtt.outgoing_pub = build_outgoing_pub();
    //     mqtt.last_puback = 3;
    //     let requests = mqtt.clean();
    //     let res = vec![6, 1, 2, 3];
    //     for (req, idx) in requests.iter().zip(res) {
    //         if let Request::Publish(publish) = req {
    //             assert_eq!(publish.pkid, idx);
    //         } else {
    //             unreachable!()
    //         }
    //     }

    //     mqtt.outgoing_pub = build_outgoing_pub();
    //     mqtt.last_puback = 0;
    //     let requests = mqtt.clean();
    //     let res = vec![1, 2, 3, 6];
    //     for (req, idx) in requests.iter().zip(res) {
    //         if let Request::Publish(publish) = req {
    //             assert_eq!(publish.pkid, idx);
    //         } else {
    //             unreachable!()
    //         }
    //     }

    //     mqtt.outgoing_pub = build_outgoing_pub();
    //     mqtt.last_puback = 6;
    //     let requests = mqtt.clean();
    //     let res = vec![1, 2, 3, 6];
    //     for (req, idx) in requests.iter().zip(res) {
    //         if let Request::Publish(publish) = req {
    //             assert_eq!(publish.pkid, idx);
    //         } else {
    //             unreachable!()
    //         }
    //     }
    // }
}
