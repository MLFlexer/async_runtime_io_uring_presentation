use std::{
    collections::HashMap,
    future::Future,
    sync::{
        mpsc::{Receiver, SyncSender},
        Arc, Mutex, OnceLock,
    },
};

use io_uring::{cqueue, squeue};

use super::{helpers::Task, thread_pool::ThreadPool};

pub static SQE_SENDER: OnceLock<SyncSender<Arc<dyn FutureState + Send + Sync>>> = OnceLock::new();

pub fn set_sqe_sender(sender: SyncSender<Arc<dyn FutureState + Send + Sync>>) {
    SQE_SENDER.set(sender).expect("SQE SENDER ALREADY SET");
}

pub trait FutureState {
    fn get_sqes(&self) -> &[squeue::Entry];
    fn get_id(&self) -> Option<u64>;
    fn push_cqe(&self, cqe: cqueue::Entry);
    fn wake(&self);
    fn is_complete(&self) -> bool;
    fn print(&self);
}

pub fn spawn(future: impl Future<Output = ()> + 'static + Send) {
    let future = Box::pin(future);
    let task = Arc::new(Task {
        future: Mutex::new(Some(future)),
        task_sender: ThreadPool::get_tx().unwrap(),
    });
    ThreadPool::send_task(task).expect("COULD NOT SEND SPAWNED TASK!");
}

pub struct Executor {
    sqe_rx_queue: Receiver<Arc<dyn FutureState + Send + Sync>>,
    task_map: HashMap<u64, Arc<dyn FutureState + Send + Sync>>,
    ring: io_uring::IoUring,
}

impl Executor {
    pub fn new(
        sqe_rx_queue: Receiver<Arc<dyn FutureState + Send + Sync + 'static>>,
        ring: io_uring::IoUring,
    ) -> Executor {
        Executor {
            sqe_rx_queue,
            task_map: HashMap::new(),
            ring,
        }
    }
    pub fn handle_iouring(&mut self) {
        let mut has_submissions = false;
        while let Ok(future_state) = self.sqe_rx_queue.try_recv() {
            unsafe {
                self.ring
                    .submission()
                    .push_multiple(future_state.get_sqes())
            }
            .expect("Could not submit");
            {
                if let Some(x) = self.task_map.insert(
                    future_state.get_id().expect("ID NEEDS TO BE SET!"),
                    future_state.clone(),
                ) {
                    panic!("ID ALDREADY PRESENT!: {:?}", x.get_id())
                };
            }
            has_submissions = true;
        }

        if has_submissions {
            self.ring.submit().expect("COULD NOT SUBMIT!");
        }

        for cqe in self.ring.completion() {
            let is_complete = {
                let task_lock = self
                    .task_map
                    .get_mut(&cqe.user_data())
                    .expect(format!("Ccould not get cqe: {:?}", cqe).as_str());
                task_lock.push_cqe(cqe.clone());
                task_lock.is_complete()
            };

            if is_complete {
                if let Some(task) = self.task_map.remove(&cqe.user_data()) {
                    task.wake();
                };
            }
        }
    }
}
