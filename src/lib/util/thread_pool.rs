use std::{
    num::NonZero,
    sync::{
        mpmc::{Receiver, Sender},
        Arc, Mutex, OnceLock,
    },
};

use super::{executor::Executor, helpers::Task, worker::Worker};

pub struct ThreadPool {
    rx: Receiver<Arc<Task>>,
    tx: Sender<Arc<Task>>,
    size: NonZero<usize>,
    workers: Vec<Worker>,
}

impl ThreadPool {
    pub fn init(
        rx: Receiver<Arc<Task>>,
        tx: Sender<Arc<Task>>,
        executor: Arc<Mutex<Executor>>,
    ) -> Result<(), ThreadPool> {
        let mut pool = Self::new(rx.clone(), tx);
        for _i in 0..pool.size.into() {
            pool.workers.push(Worker::new(rx.clone(), executor.clone()));
        }
        THREAD_POOL.set(pool)
    }

    fn new(rx: Receiver<Arc<Task>>, tx: Sender<Arc<Task>>) -> Self {
        // let size = std::thread::available_parallelism().unwrap();
        let size = NonZero::new(4).unwrap();
        Self {
            rx: rx.clone(),
            tx: tx.clone(),
            size,
            workers: Vec::with_capacity(size.into()),
        }
    }

    pub fn get_tx() -> Option<Sender<Arc<Task>>> {
        Some(THREAD_POOL.get()?.tx.clone())
    }

    pub fn recv_task() -> Result<Arc<Task>, std::sync::mpsc::RecvError> {
        THREAD_POOL.wait().rx.recv()
    }

    pub fn send_task(task: Arc<Task>) -> Result<(), std::sync::mpsc::SendError<Arc<Task>>> {
        THREAD_POOL.wait().tx.send(task)
    }

    pub fn try_send_task(task: Arc<Task>) -> Result<(), std::sync::mpsc::TrySendError<Arc<Task>>> {
        THREAD_POOL.wait().tx.try_send(task)
    }

    pub fn join() {
        for w in THREAD_POOL.wait().workers.iter() {
            (unsafe { w.handle.get().read() }).join();
        }
    }
}

pub static THREAD_POOL: OnceLock<ThreadPool> = OnceLock::new();
