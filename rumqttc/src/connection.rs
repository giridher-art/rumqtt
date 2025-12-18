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
    Control, EventsTx, IOEvent,
    xchg::{XchgPipeA, XchgPipeB, pipe},
};

use super::CleanupMethod;

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

    pub async fn start(&mut self) -> Result<(), std::io::Error>
// where
    //     T: AsyncReadExt + AsyncWriteExt + Unpin,
    {
        loop {
            tokio::time::sleep(Duration::from_millis(5)).await;
            self.events_tx
                .send_async(IOEvent::ConnectionData)
                .await
                .unwrap();
            println!("Connection: heartbeat");
        }
        // we should get some data from stream within timeout!
        //     let (buffer, size) = self.connection_to_io.active.raw_mut();
        //     let read = timeout(
        //         Duration::from_secs(self.timeout as u64),
        //         read_from_stream(buffer, stream),
        //     )
        //     .await
        //     .map_err(|_| io::Error::from(ErrorKind::StaleNetworkFileHandle))??;

        //     *size += read;

        //     self.active = true;

        //     if self.connection_to_io.try_forward() {
        //         self.events_tx
        //             .send_async(IOEvent::ConnectionData)
        //             .await
        //             .unwrap();
        //     }

        //     loop {
        //         // TODO(swanx): we shall improve these names!!
        //         let has_space = self.connection_to_io.active.remaining_space() > 0;
        //         let (buffer, size) = self.connection_to_io.active.raw_mut();

        //         select! {
        //             // Read from network and fill read buffer
        //             v = read_from_stream(buffer, stream), if has_space => {
        //                 *size += v?;

        //                 // dbg!(_n);
        //                 if self.connection_to_io.try_forward() {
        //                     self.events_tx.send_async(IOEvent::ConnectionData).await.unwrap();
        //                 }
        //             }
        //             _ = self.connection_to_io.recycler.wait() => {
        //                 // dbg!("received back incoming_tx buffer");
        //                 let standby = self.connection_to_io.recycler.standby().unwrap();
        //                 stream.write_all(standby).await?;
        //                 self.connection_to_io.recycler.clear();
        //                 // try to send to active buffer to other end
        //                 if self.connection_to_io.try_forward() {
        //                     self.events_tx.send_async(IOEvent::ConnectionData).await.unwrap();
        //                 }
        //             }
        //             data = self.io_to_connection.incoming.recv_async() => {
        //                 let mut data = data.expect("use ? here");
        //                 stream.write_all(&data).await?;
        //                 data.clear();
        //                 self.io_to_connection.ack(data);
        //                 self.events_tx.send_async(IOEvent::OutgoingDataAck).await.unwrap();
        //             }
        //             signal = self.control_rx.recv_async() => {
        //                 match signal.expect("should recv") {
        //                     Control::Terminate => return Ok(()),
        //                     Control::CleanupAck => unreachable!(),
        //                     Control::Stats => self.print_stats()
        //                 }
        //             }
        //         }
        //     }
    }

    pub async fn cleanup<T>(&mut self, stream: &mut T, method: CleanupMethod)
    where
        T: AsyncReadExt + AsyncWriteExt + Unpin,
    {
        if !self.active {
            self.connection_to_io.clear();
            self.active = false;
            self.control_rx.drain();
            return;
        }

        // CRITICAL FIX: Send ConnectionCleanup BEFORE waiting for buffers to prevent deadlock
        //
        // Previous deadlock scenario (especially with DISCONNECT packets):
        // 1. Connection sends buffer to IO via pipe for processing packets
        // 2. IO's Framer stores buffer as inflight_acks (incoming_hub.rs:231)
        // 3. DISCONNECT packet triggers IOError::ReceivedDisconnect (incoming_hub.rs:400)
        // 4. Error return bypasses normal ack flow in io/src/lib.rs:342
        //    - Normally, line 372-375 would process acks and return buffer
        //    - But error propagation skips this, leaving buffer in Framer
        // 5. IO sends Control::Terminate to Connection (io/src/lib.rs:267)
        // 6. Connection's cleanup() tries to collect buffer via recycler.recv_async()
        // 7. But buffer is stuck in IO's Framer.inflight_acks, never returned
        // 8. Result: Connection waits forever for buffer that IO never returns
        //
        // Why the previous order caused deadlock:
        // - Connection waited for buffer BEFORE sending ConnectionCleanup
        // - IO only returns buffer when processing ConnectionCleanup
        // - Circular dependency: each component waiting for the other
        //
        // Solution: Send ConnectionCleanup FIRST to break the cycle:
        // 1. ConnectionCleanup sent to IO immediately
        // 2. IO processes cleanup (io/src/lib.rs:526-568)
        // 3. Calls incoming_hub.cleanup_connection() (line 471)
        // 4. Returns inflight_acks buffer via pipe.ack() (incoming_hub.rs:185)
        // 5. Connection's recycler.recv_async() now receives buffer
        // 6. Cleanup completes successfully
        //
        // This fix is essential for MQTT DISCONNECT packet handling and any
        // other error path that bypasses the normal buffer return flow.
        self.events_tx
            .send_async(IOEvent::ConnectionCleanup)
            .await
            .unwrap();

        match method {
            // Collect inflight bufers and drop inflight data
            CleanupMethod::Terminate => {
                // Wait for buffer to be returned from IO after cleanup
                self.connection_to_io.recycler.recv_async().await;
            }
            // Collect inflight bufers and send inflight data to IO and client
            CleanupMethod::Graceful => {
                // Wait for buffer to be returned from IO after cleanup
                self.connection_to_io.recycler.recv_async().await;
                let standby = self.connection_to_io.recycler.standby().unwrap();
                let _ = stream.write_all(standby).await;

                // forward active buffer incase there is still some data.
                //
                // because, on EoF, we return error but some data might
                // already be there in buffer. Hence, forward it first,
                // then handle rest.
                if self.connection_to_io.try_forward() {
                    self.events_tx
                        .send_async(IOEvent::ConnectionData)
                        .await
                        .unwrap();
                }

                // wait for acks again from previous exchange
                // and write acks to client
                self.connection_to_io.recycler.recv_async().await;
                let standby = self.connection_to_io.recycler.standby().unwrap();
                let _ = stream.write_all(standby).await;
            }
        };

        // Collect any inflight buffer from io_to_connection pipe
        // and return it back to IO via recycler
        // But don't send IOEvent::OutgoingDataAck to keep io inert and not send data via outgoinghub
        // At the end, outgoinghub.cleanup_connection() will collect the buffer back via recycler
        if let Ok(mut buffer) = self.io_to_connection.try_recv() {
            buffer.clear();
            self.io_to_connection.ack(buffer);
        }

        // Wait for CleanupAck from IO to complete the cleanup flow
        let event = self.control_rx.recv_async().await.unwrap();
        debug_assert!(matches!(event, Control::CleanupAck));

        self.connection_to_io.clear();
        self.active = false;
        self.control_rx.drain();
    }

    pub fn print_stats(&mut self) {
        println!(
            "Connection => active buffer: {}, standby buf: {:?}, max: {}",
            self.connection_to_io.active.len(),
            self.connection_to_io.recycler.standby().map(|b| b.len()),
            self.connection_to_io.active.max_size()
        )
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
