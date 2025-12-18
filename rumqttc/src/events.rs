use futures_util::{Stream, StreamExt};
use std::{future::Future, io, pin::Pin, task::Poll, time::Duration};
use tokio::{
    net::TcpStream,
    time::{Instant, Sleep},
};
use tokio_util::time::{DelayQueue, delay_queue::Key};

use crate::IOEvent;

use flume::{Receiver, Sender, r#async::RecvStream, bounded};
use tokio::runtime::{self, Runtime};

/// An Event Bus that allows consumers to poll it for events
/// and allows publishers to send events through it.
#[derive(Debug)]
pub struct EventsRx<T: 'static> {
    timers: Option<Pin<Box<Sleep>>>,
    tx: Sender<(usize, T)>,
    rx: RecvStream<'static, (usize, T)>,
}

impl<T: 'static> EventsRx<T> {
    /// Create a new Event Bus
    pub fn new(max_events: usize) -> Self {
        let (tx, rx) = bounded(max_events);
        let rx = rx.into_stream();
        EventsRx {
            timers: None,
            tx,
            rx,
        }
    }

    pub fn add_timer(&mut self, duration: Duration) {
        let sleep = tokio::time::sleep_until(Instant::now() + duration);
        self.timers = Some(Box::pin(sleep))
    }

    pub fn remove_timer(&mut self) {
        self.timers = None;
    }

    pub fn reset_timer(&mut self, timeout: Duration) {
        if let Some(timer) = self.timers.as_mut() {
            timer.as_mut().reset(Instant::now() + timeout);
        } else {
            let sleep = tokio::time::sleep_until(Instant::now() + timeout);
            self.timers = Some(Box::pin(sleep));
        }
    }

    pub async fn recv_non_timer_events(&mut self) -> (usize, T) {
        self.rx.next().await.unwrap()
    }

    pub fn producer(&self, id: usize) -> EventsTx<T> {
        EventsTx {
            id,
            tx: self.tx.clone(),
        }
    }
}

impl Stream for EventsRx<IOEvent> {
    type Item = (usize, IOEvent);

    fn poll_next(
        mut self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        if let Some(ref mut timer) = self.timers {
            if timer.as_mut().poll(cx).is_ready() {
                return Poll::Ready(Some((0, IOEvent::Refresh)));
            }
        }

        Pin::new(&mut self.rx).poll_next(cx)
    }
}

#[derive(Debug, thiserror::Error)]
pub enum EventError {
    #[error("I/O: {0}")]
    Io(#[from] io::Error),
    #[error("Timer stream done")]
    TimerStreamDone,
    #[error("Send error")]
    SendError,
    #[error("Try send error")]
    TrySendError,
    #[error("Recv error")]
    RecvError,
    #[error("Try recv error")]
    TryRecvError,
}

/// An Event Bus that allows consumers to poll it for events
/// and allows publishers to send events through it.
#[derive(Clone, Debug)]
pub struct EventsTx<T> {
    pub id: usize,
    pub tx: Sender<(usize, T)>,
}

impl<T> EventsTx<T> {
    pub fn new_with_id(&self, id: usize) -> Self {
        Self {
            id,
            tx: self.tx.clone(),
        }
    }
    pub fn send(&self, event: T) -> Result<(), EventError> {
        self.tx
            .send((self.id, event))
            .map_err(|_e| EventError::SendError)
    }

    pub fn try_send(&self, event: T) -> Result<(), EventError> {
        // NOTE: we should ideally return the failed value in TrySendError
        // flume::TrySendError(..) returns the value but we are ignoring
        // it here to avoid type mess
        self.tx
            .try_send((self.id, event))
            .map_err(|_e| EventError::TrySendError)
    }

    pub async fn send_async(&self, event: T) -> Result<(), EventError> {
        self.tx
            .send_async((self.id, event))
            .await
            .map_err(|_e| EventError::SendError)
    }
}
