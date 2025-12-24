use std::{marker::PhantomData, sync::Arc};

use flume::{r#async::RecvStream, IntoIter, Iter, Receiver, RecvError, SendError, TrySendError};
use futures_util::{future::ok, StreamExt};
use tokio_rustls::client;

use crate::{
    events::{EventError, EventsTx},
    protocol::{
        v4::{subscribe, V4},
        Protocol,
    },
    valid_filter, valid_topic, Disconnect, Filter, IOEvent, Message, PubAck, PubRec, Publish,
    PublishReq, QoS, Subscribe, SubscribeReq, UnSubscribeReq, Unsubscribe,
};

use bytes::Bytes;
pub struct AsyncMode;
pub struct SyncMode;

/// Client Error
#[derive(Debug, thiserror::Error)]
pub enum ClientError {
    #[error("Failed to send mqtt requests to eventloop")]
    EventError(EventError),
    #[error("Failed to send mqtt requests to eventloop")]
    Request(Message),
    #[error("Failed to send mqtt requests to eventloop")]
    TryRequest(Message),
}

impl From<EventError> for ClientError {
    fn from(value: EventError) -> Self {
        Self::EventError(value)
    }
}

impl From<SendError<Message>> for ClientError {
    fn from(e: SendError<Message>) -> Self {
        Self::Request(e.into_inner())
    }
}

impl From<TrySendError<Message>> for ClientError {
    fn from(e: TrySendError<Message>) -> Self {
        Self::TryRequest(e.into_inner())
    }
}
/// MQTT Client
pub struct Client<P: Protocol, M> {
    pub id: usize,
    protocol: P,
    token_id: usize,
    pub event_tx: EventsTx<IOEvent>,
    acks_receiver: Receiver<Message>,
    subcriptions_chan: Receiver<Publish>,
    _maker: PhantomData<M>,
}

impl<P: Protocol, M> Client<P, M> {
    // Create a new MQTT client
    pub fn new(
        id: usize,
        protocol: P,
        event_tx: EventsTx<IOEvent>,
        acks_receiver: flume::Receiver<Message>,
        subcription_chan: Receiver<Publish>,
    ) -> Self {
        Client {
            protocol: protocol,
            id,
            subcriptions_chan: subcription_chan,
            token_id: 0,
            acks_receiver,
            event_tx,
            _maker: PhantomData,
        }
    }

    pub fn into_mode<N>(self) -> Client<P, N> {
        Client {
            protocol: self.protocol,
            id: self.id,
            token_id: self.token_id,
            event_tx: self.event_tx,
            acks_receiver: self.acks_receiver,
            subcriptions_chan: self.subcriptions_chan,
            _maker: PhantomData,
        }
    }

    // get ack message for a publish
    pub fn get_ack_req(&mut self, publish: &Publish) -> Option<Message> {
        match publish.qos {
            QoS::AtMostOnce => {
                return None;
            }
            QoS::AtLeastOnce => Some(Message::PublishAck(crate::PublishResp {
                token_id: self.next_token_id(),
                puback: PubAck {
                    pkid: publish.pkid,
                    reason: crate::PubAckReason::Success,
                    properties: None,
                },
            })),
            QoS::ExactlyOnce => Some(Message::PublishRec(crate::PublishRec {
                token_id: self.next_token_id(),
                pubrec: PubRec {
                    pkid: publish.pkid,
                    reason: crate::PubRecReason::Success,
                    properties: None,
                },
            })),
        }
    }

    // for next token_id
    fn next_token_id(&mut self) -> usize {
        self.token_id += 1;
        self.token_id
    }

    /// Attempts to send a MQTT Publish to the `EventLoop`.
    pub fn try_publish<S, V>(
        &mut self,
        topic: S,
        qos: QoS,
        retain: bool,
        payload: V,
    ) -> Result<usize, ClientError>
    where
        S: Into<String>,
        V: Into<Vec<u8>>,
    {
        let topic = topic.into();
        let mut publish = Publish::new(topic, qos, payload, false);
        publish.retain = retain;
        let token_id = self.next_token_id();
        let publish_msg = PublishReq {
            token_id: token_id,
            publish,
        };
        self.event_tx
            .try_send(IOEvent::ClientMessage(Message::Publish(publish_msg)))?;
        Ok(token_id)
    }

    /// Attempts to send a MQTT PubAck to the `EventLoop`. Only needed in if `manual_acks` flag is set.
    pub fn try_ack(&mut self, publish: &Publish) -> Result<(), ClientError> {
        if let Some(msg) = self.get_ack_req(publish) {
            self.event_tx.try_send(IOEvent::ClientMessage(msg))?;
        }
        Ok(())
    }

    /// Attempts to send a MQTT Subscribe to the `EventLoop`
    pub fn try_subscribe<S: Into<String>>(
        &mut self,
        topic: S,
        qos: QoS,
    ) -> Result<usize, ClientError> {
        let topic = topic.into();
        let filters = Filter::new(topic.clone(), qos);
        let subscribe = Subscribe::new(vec![filters]);
        let token_id = self.next_token_id();
        let sub_msg = SubscribeReq {
            token_id,
            subscribe,
        };
        if !valid_topic(topic.as_str()) {
            return Err(ClientError::Request(Message::Subscribe(sub_msg)));
        }
        self.event_tx
            .try_send(IOEvent::ClientMessage(Message::Subscribe(sub_msg)))?;

        Ok(token_id)
    }

    pub fn try_subscribe_many<T>(&mut self, topics: T) -> Result<usize, ClientError>
    where
        T: IntoIterator<Item = Filter>,
    {
        let subscribe = Subscribe::new_many(topics);
        let token_id = self.next_token_id();
        let subreq = SubscribeReq {
            token_id,
            subscribe,
        };
        if !subscribe_has_valid_filters(&subreq.subscribe) {
            return Err(ClientError::Request(Message::Subscribe(subreq)));
        }
        self.event_tx
            .try_send(IOEvent::ClientMessage(Message::Subscribe(subreq)))?;

        Ok(token_id)
    }

    /// Attempts to send a MQTT Unsubscribe to the `EventLoop`
    pub fn try_unsubscribe<S: Into<String>>(&mut self, topic: S) -> Result<usize, ClientError> {
        let unsub = Unsubscribe {
            pkid: 0,
            filters: vec![topic.into()],
            properties: None,
        };
        let token_id = self.next_token_id();
        let unsub = UnSubscribeReq { token_id, unsub };

        self.event_tx
            .try_send(IOEvent::ClientMessage(Message::UnSub(unsub)))?;
        Ok(token_id)
    }

    // try disconnect
    pub fn try_disconnect(&self) -> Result<(), ClientError> {
        self.event_tx.try_send(IOEvent::Shutdown)?;
        Ok(())
    }
}

impl Client<V4, SyncMode> {
    // Waits on the acks messages
    pub fn wait(&mut self) -> Result<Message, flume::RecvError> {
        self.acks_receiver.recv()
    }

    // Returns the next publish message from the subscriptions
    pub fn next(&mut self) -> Result<Publish, RecvError> {
        self.subcriptions_chan.recv()
    }

    // returns subscription iterations
    pub fn subcrition_iter(&mut self) -> Iter<Publish> {
        self.subcriptions_chan.iter()
    }

    // returns acks iterator
    pub fn acks_iter(&mut self) -> Iter<Message> {
        self.acks_receiver.iter()
    }

    // Publish a message synchronously
    pub fn publish<S, V>(&mut self, topic: S, qos: QoS, retain: bool, payload: V) -> usize
    where
        S: Into<String>,
        V: Into<Vec<u8>>,
    {
        let topic = topic.into();
        let mut publish = Publish::new(topic, qos, payload, false);
        publish.retain = retain;
        let token_id = self.next_token_id();
        let publish_msg = PublishReq {
            token_id: token_id,
            publish,
        };
        self.event_tx
            .send(IOEvent::ClientMessage(Message::Publish(publish_msg)));
        token_id
    }

    /// Sends a MQTT PubAck to the `EventLoop`. Only needed in if `manual_acks` flag is set.
    pub fn ack(&mut self, publish: &Publish) -> Result<(), ClientError> {
        if let Some(msg) = self.get_ack_req(publish) {
            self.event_tx.send(IOEvent::ClientMessage(msg))?;
        }
        Ok(())
    }

    /// Sends a MQTT Publish to the `EventLoop`
    pub fn publish_bytes<S>(
        &mut self,
        topic: S,
        qos: QoS,
        retain: bool,
        payload: Bytes,
    ) -> Result<usize, ClientError>
    where
        S: Into<String>,
    {
        let topic = topic.into();
        let mut publish = Publish::new(topic, qos, payload, false);
        publish.retain = retain;
        let token_id = self.next_token_id();
        let publish_msg = PublishReq {
            token_id: token_id,
            publish,
        };
        self.event_tx
            .send(IOEvent::ClientMessage(Message::Publish(publish_msg)))?;
        Ok(token_id)
    }

    /// Sends a MQTT Subscribe to the `EventLoop`
    pub fn subscribe<S: Into<String>>(&mut self, topic: S, qos: QoS) -> Result<usize, ClientError> {
        let topic = topic.into();
        let subscribe = Subscribe::new(vec![Filter::new(topic.clone(), qos)]);
        if !valid_topic(topic.as_str()) {
            return Err(ClientError::Request(Message::Subscribe(SubscribeReq {
                token_id: 0,
                subscribe: subscribe,
            })));
        }
        let token_id = self.next_token_id();
        let sub_msg = SubscribeReq {
            token_id,
            subscribe,
        };

        self.event_tx
            .send(IOEvent::ClientMessage(Message::Subscribe(sub_msg)));

        Ok(token_id)
    }

    /// Sends a MQTT Subscribe for multiple topics to the `EventLoop`
    pub fn subscribe_many<T>(&mut self, topics: T) -> Result<usize, ClientError>
    where
        T: IntoIterator<Item = Filter>,
    {
        let subscribe = Subscribe::new_many(topics);

        let token_id = self.next_token_id();
        let subreq = SubscribeReq {
            token_id,
            subscribe,
        };

        if !subscribe_has_valid_filters(&subreq.subscribe) {
            return Err(ClientError::Request(Message::Subscribe(subreq)));
        }

        self.event_tx
            .send(IOEvent::ClientMessage(Message::Subscribe(subreq)))?;

        Ok(token_id)
    }

    pub fn unsubscribe<S: Into<String>>(&mut self, topic: S) -> Result<usize, ClientError> {
        let unsub = Unsubscribe {
            pkid: 0,
            filters: vec![topic.into()],
            properties: None,
        };
        let token_id = self.next_token_id();
        let unsub = UnSubscribeReq { token_id, unsub };

        self.event_tx
            .send(IOEvent::ClientMessage(Message::UnSub(unsub)))?;
        Ok(token_id)
    }

    /// Sends a MQTT disconnect to the `EventLoop`
    pub fn disconnect(&mut self) -> Result<(), ClientError> {
        self.event_tx.send(IOEvent::Shutdown)?;
        Ok(())
    }
}

impl Client<V4, AsyncMode> {
    // Waits on the acks messages
    pub async fn wait(&mut self) -> Result<Message, flume::RecvError> {
        self.acks_receiver.recv_async().await
    }

    // Returns the next publish message from the subscriptions
    pub async fn next(&mut self) -> Result<Publish, RecvError> {
        self.subcriptions_chan.recv_async().await
    }

    // returns subscription stream
    pub fn subcrition_stream(&mut self) -> RecvStream<Publish> {
        self.subcriptions_chan.stream()
    }

    // returns acks stream
    pub fn acks_stream(&mut self) -> RecvStream<Message> {
        self.acks_receiver.stream()
    }

    // Publish a message synchronously
    pub async fn publish<S, V>(
        &mut self,
        topic: S,
        qos: QoS,
        retain: bool,
        payload: V,
    ) -> Result<usize, ClientError>
    where
        S: Into<String>,
        V: Into<Vec<u8>>,
    {
        let topic = topic.into();
        let mut publish = Publish::new(topic, qos, payload, false);
        publish.retain = retain;
        let token_id = self.next_token_id();
        let publish_msg = PublishReq {
            token_id: token_id,
            publish,
        };
        self.event_tx
            .send_async(IOEvent::ClientMessage(Message::Publish(publish_msg)))
            .await?;
        Ok(token_id)
    }

    /// Sends a MQTT Publish to the `EventLoop`
    pub async fn publish_bytes<S>(
        &mut self,
        topic: S,
        qos: QoS,
        retain: bool,
        payload: Bytes,
    ) -> Result<usize, ClientError>
    where
        S: Into<String>,
    {
        let topic = topic.into();
        let mut publish = Publish::new(topic, qos, payload, false);
        publish.retain = retain;
        let token_id = self.next_token_id();
        let publish_msg = PublishReq {
            token_id: token_id,
            publish,
        };
        self.event_tx
            .send_async(IOEvent::ClientMessage(Message::Publish(publish_msg)))
            .await?;
        Ok(token_id)
    }

    /// Sends a MQTT PubAck to the `EventLoop`. Only needed in if `manual_acks` flag is set.
    pub async fn ack(&mut self, publish: &Publish) -> Result<(), ClientError> {
        if let Some(msg) = self.get_ack_req(publish) {
            self.event_tx
                .send_async(IOEvent::ClientMessage(msg))
                .await?;
        }
        Ok(())
    }

    /// Sends a MQTT Subscribe to the `EventLoop`
    pub async fn subscribe<S: Into<String>>(
        &mut self,
        topic: S,
        qos: QoS,
    ) -> Result<usize, ClientError> {
        let topic = topic.into();
        let filters = Filter::new(topic.clone(), qos);
        let subscribe = Subscribe::new(vec![filters]);
        let token_id = self.next_token_id();
        let sub_msg = SubscribeReq {
            token_id,
            subscribe,
        };
        if !valid_topic(topic.as_str()) {
            return Err(ClientError::Request(Message::Subscribe(sub_msg)));
        }
        self.event_tx
            .send_async(IOEvent::ClientMessage(Message::Subscribe(sub_msg)))
            .await;

        Ok(token_id)
    }

    /// Sends a MQTT Subscribe for multiple topics to the `EventLoop`
    pub async fn subscribe_many<T>(&mut self, topics: T) -> Result<usize, ClientError>
    where
        T: IntoIterator<Item = Filter>,
    {
        let subscribe = Subscribe::new_many(topics);

        let token_id = self.next_token_id();
        let subreq = SubscribeReq {
            token_id,
            subscribe,
        };
        if !subscribe_has_valid_filters(&subreq.subscribe) {
            return Err(ClientError::Request(Message::Subscribe(subreq)));
        }
        self.event_tx
            .send_async(IOEvent::ClientMessage(Message::Subscribe(subreq)))
            .await?;

        Ok(token_id)
    }

    pub async fn unsubscribe<S: Into<String>>(&mut self, topic: S) -> Result<usize, ClientError> {
        let unsub = Unsubscribe {
            pkid: 0,
            filters: vec![topic.into()],
            properties: None,
        };
        let token_id = self.next_token_id();
        let unsub = UnSubscribeReq { token_id, unsub };

        self.event_tx
            .send_async(IOEvent::ClientMessage(Message::UnSub(unsub)))
            .await?;
        Ok(token_id)
    }

    /// Sends a MQTT disconnect to the `EventLoop`
    pub async fn disconnect(&self) -> Result<(), ClientError> {
        self.event_tx.send_async(IOEvent::Shutdown).await?;
        Ok(())
    }
}

fn subscribe_has_valid_filters(subscribe: &Subscribe) -> bool {
    !subscribe.filters.is_empty()
        && subscribe
            .filters
            .iter()
            .all(|filter| valid_filter(&filter.path))
}
