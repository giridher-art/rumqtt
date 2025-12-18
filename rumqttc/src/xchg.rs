use flume::{Receiver, Sender};
use std::mem;

use crate::Size;

#[derive(Debug)]
pub struct XchgPipeA<T>
where
    T: Size,
{
    pub id: usize,
    pub active: Active<T>,
    pub recycler: Recycler<T>,
}

impl<T> XchgPipeA<T>
where
    T: Size,
{
    /// Swaps ready buffer with recycled buffer and forwards it. Ready buffer
    /// is empty after this operation
    pub fn try_forward(&mut self) -> bool {
        // If current read buffer is empty, there is nothing to forward
        if self.active.is_empty() {
            return false;
        }

        if let Some(recycled) = self.recycler.standby.take() {
            // debug_assert!(recycled.is_empty());
            let ready = self.active.swap(recycled);
            self.active.tx.try_send(ready).unwrap();
            return true;
        }

        false
    }

    pub fn standby(&mut self) -> Option<&mut Vec<T>> {
        self.recycler.standby()
    }

    /// Forwards empty buffers to the other side.
    pub fn force_forward(&mut self) -> bool {
        if let Some(recycled) = self.recycler.standby.take() {
            let ready = self.active.swap(recycled);
            self.active.tx.try_send(ready).unwrap();
            return true;
        }

        false
    }

    pub async fn flush(&mut self) -> bool {
        if self.recycler.standby.is_none() {
            self.recycler.wait().await;
            self.recycler.clear();
        }

        self.try_forward()
    }

    pub async fn shutdown(&mut self) -> Option<&mut Vec<T>> {
        if self.recycler.standby.is_none() {
            self.recycler.wait().await;
        }

        self.recycler.standby()
    }

    pub fn clear(&mut self) {
        self.active.clear();
        self.recycler.clear();
    }
}

#[derive(Debug)]
pub struct XchgPipeB<T>
where
    T: Size,
{
    pub incoming: Receiver<Vec<T>>,
    pub incoming_recycle: Sender<Vec<T>>,
}

impl<T> XchgPipeB<T>
where
    T: Size,
{
    pub fn recv(&self) -> Result<Vec<T>, Error> {
        let v = self.incoming.recv()?;
        Ok(v)
    }

    pub fn try_recv(&self) -> Result<Vec<T>, Error> {
        let v = self.incoming.try_recv()?;
        Ok(v)
    }

    pub fn ack(&self, buf: Vec<T>) {
        self.incoming_recycle.try_send(buf).ok().unwrap();
    }
}

#[derive(Debug)]
pub struct Active<T>
where
    T: Size,
{
    buffer: Vec<T>,
    tx: Sender<Vec<T>>,
    current_size: usize,
    max_size: usize,
}

impl<T> Active<T>
where
    T: Size,
{
    pub fn push(&mut self, item: T) {
        let item_size = item.size();
        self.buffer.push(item);
        self.current_size += item_size;
    }

    pub fn extend<I: IntoIterator<Item = T>>(&mut self, items: I) {
        for item in items {
            self.push(item);
        }
    }

    pub fn clear(&mut self) {
        self.buffer.clear();
        self.current_size = 0;
    }

    pub fn max_size(&self) -> usize {
        self.max_size
    }

    pub fn size(&self) -> usize {
        self.current_size
    }

    pub fn len(&self) -> usize {
        self.buffer.len()
    }

    pub fn is_empty(&self) -> bool {
        self.buffer.is_empty()
    }

    pub fn remaining_space(&self) -> usize {
        if self.max_size < self.current_size {
            return 0;
        }

        self.max_size - self.current_size
    }

    pub fn swap(&mut self, replacement: Vec<T>) -> Vec<T> {
        self.current_size = 0;
        mem::replace(&mut self.buffer, replacement)
    }

    /// Raw buffer access. User of this method should manually update the current size.
    pub fn raw_mut(&mut self) -> (&mut Vec<T>, &mut usize) {
        (&mut self.buffer, &mut self.current_size)
    }

    /// Reference of buffer
    pub fn raw_ref(&self) -> &Vec<T> {
        &self.buffer
    }
}

#[derive(Debug)]
pub struct Recycler<T> {
    standby: Option<Vec<T>>,
    rx: Receiver<Vec<T>>,
}

impl<T> Recycler<T> {
    /// Wait for notification and buffer to be recycled
    pub async fn wait(&mut self) {
        let v = self.rx.recv_async().await.unwrap();
        self.standby = Some(v);
    }

    /// Receive buffer. Notification would have been sent in global events queue
    pub fn recv(&mut self) {
        if self.standby.is_none() {
            let v = self.rx.recv().unwrap();
            self.standby = Some(v);
        }
    }

    /// Async Receive buffer. Notification would have been sent in global events queue
    /// Using async to avoid blocking in runtime
    pub async fn recv_async(&mut self) {
        if self.standby.is_none() {
            let v = self.rx.recv_async().await.unwrap();
            self.standby = Some(v);
        }
    }

    /// Receive buffer. Notification would have been sent in global events queue
    pub fn try_recv(&mut self) {
        let Some(v) = self.rx.try_recv().ok() else {
            return;
        };
        self.standby = Some(v);
    }

    pub fn standby(&mut self) -> Option<&mut Vec<T>> {
        self.standby.as_mut()
    }

    pub fn clear(&mut self) {
        if let Some(ref mut buf) = self.standby {
            buf.clear();
        }
    }
}

pub fn pipe<T: Size>(id: usize, buf_size: usize) -> (XchgPipeA<T>, XchgPipeB<T>) {
    let (incoming_tx, incoming_rx) = flume::bounded(1);
    let (incoming_recycle_tx, incoming_recycle_rx) = flume::bounded(1);

    // Calculate optimal buffer capacity based on the size of type T in bytes.
    // This determines how many elements of type T each buffer will hold.
    let t_size = std::mem::size_of::<T>();

    // Memory allocation strategy based on type characteristics:
    let capacity = if t_size == 1 {
        // Single-byte types (u8, i8, bool):
        // - Use user-specified buf_size directly as element capacity
        // - Memory usage: buf_size * 1 byte = buf_size bytes per buffer
        // - Example: buf_size=4096 means 4KB buffer for u8 data
        buf_size
    } else {
        // Multi-byte types (u16, u32, u64, f32, f64, structs, etc.):
        // - Fixed capacity of 256 elements regardless of buf_size parameter
        // - Memory usage per buffer: 256 * t_size bytes
        // - Examples:
        //   * u32 (4 bytes):    256 * 4 = 1,024 bytes (1 KB)
        //   * u64 (8 bytes):    256 * 8 = 2,048 bytes (2 KB)
        //   * u128 (16 bytes):  256 * 16 = 4,096 bytes (4 KB)
        //   * String (24 bytes): 256 * 24 = 6,144 bytes (6 KB) on stack
        //     (plus heap allocations for each String's content)
        //
        // IMPORTANT: This hardcoded value means:
        // - buf_size parameter is IGNORED for non-byte types
        // - Total memory = capacity * t_size * 2 (two buffers: active + standby)
        // - For heap-allocated types (String, Vec, Box), actual memory usage
        //   will be much higher due to nested heap allocations
        256
    };

    // Internal tx is used to send data to remote while
    // rx is use to recv previous buffer from remote to
    // maintain memory boundedness with buffer recycling
    let a = XchgPipeA {
        id,
        active: Active {
            buffer: Vec::with_capacity(capacity),
            tx: incoming_tx,
            current_size: 0,
            max_size: buf_size,
        },
        recycler: Recycler {
            standby: Some(Vec::with_capacity(capacity)),
            rx: incoming_recycle_rx,
        },
    };

    // Internal rx is used in remote to receive data from remote
    // while internal tx is used to send acks and to recycle buffer
    let b = XchgPipeB {
        incoming: incoming_rx,
        incoming_recycle: incoming_recycle_tx,
    };

    (a, b)
}

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("TryRecv: {0}")]
    TryRecv(#[from] flume::TryRecvError),
    #[error("Recv: {0}")]
    Recv(#[from] flume::RecvError),
}
