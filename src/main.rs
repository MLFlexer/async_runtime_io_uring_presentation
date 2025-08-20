#![feature(mpmc_channel)]

mod lib;

use io_uring::IoUring;
use lib::{
    net::{
        accept_multi::AcceptMultiFuture, send::SendFuture, shuwdown_close::ShutdownAndCloseFuture,
    },
    reader::ReaderFuture,
    util::{
        executor::{set_sqe_sender, spawn, Executor},
        thread_pool::ThreadPool,
    },
    writer::WriterFuture,
};
use std::{
    net::{SocketAddr, TcpListener},
    os::fd::AsRawFd,
    sync::{mpmc, mpsc::sync_channel, Arc, Mutex},
};

fn setup() -> Arc<Mutex<Executor>> {
    const MAX_QUEUED_TASKS: usize = 2usize.pow(16);
    let (sqe_sender, sqe_rx_queue) = sync_channel(MAX_QUEUED_TASKS / 2);
    set_sqe_sender(sqe_sender);
    let ring = IoUring::builder()
        // .setup_single_issuer()
        // .setup_iopoll()
        .setup_sqpoll(2_000)
        .build(2u32.pow(15) as u32)
        .unwrap();

    let ex = Arc::new(Mutex::new(Executor::new(sqe_rx_queue, ring)));
    let (worker_tx, worker_rx) = mpmc::sync_channel(MAX_QUEUED_TASKS / 2);
    let _ = ThreadPool::init(worker_rx, worker_tx, ex.clone());
    ex
}

fn main() {
    let executor = setup();

    #[cfg(feature = "atomic_shared_cell_check")]
    println!("USING ATOMIC SHARED CELL CHECK!");

    spawn(async {
        // let addr: SocketAddr = "127.0.0.1:8080".parse().unwrap();
        let addr: SocketAddr = "192.168.0.8:8080".parse().unwrap();
        let listener = TcpListener::bind(addr).unwrap();

        println!("starting multi");
        let result = AcceptMultiFuture::new(listener.as_raw_fd(), |fd| async move {
            let buf: [u8; 128] = [0u8; 128];
            let _ = ReaderFuture::new(buf, fd).await;
            let response =
                "HTTP/1.1 200 OK\r\nContent-Length: 13\r\nConnection: close\r\n\r\nHello, World!";
            let _ = WriterFuture::new(response, fd).await;

            let _ = ShutdownAndCloseFuture::new(fd).await;
        })
        .await;
        println!("EXITTING! {result}");
        std::process::exit(2);
    });

    ThreadPool::join();
    println!("EXITTING!");
}
