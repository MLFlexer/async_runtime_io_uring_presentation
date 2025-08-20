use std::{
    any::Any,
    cell::UnsafeCell,
    sync::{mpmc::Receiver, Arc, Mutex},
    task::Context,
    thread::{self},
};

use super::{executor::Executor, helpers::Task};

pub struct Worker {
    pub handle: UnsafeCell<thread::JoinHandle<()>>,
    // task_rx: Receiver<Arc<Task>>,
    // executor: Arc<Mutex<Executor>>,
}

unsafe impl Send for Worker {}
unsafe impl Sync for Worker {}

impl Worker {
    pub fn new(rx: Receiver<Arc<Task>>, executor: Arc<Mutex<Executor>>) -> Self {
        let rx_closure = rx.clone();
        let ex_closure = executor.clone();
        Self {
            handle: UnsafeCell::new(thread::spawn(move || {
                Self::execution_loop(&rx_closure, ex_closure.clone()).expect("Could not get work!")
            })),
            // task_rx: rx.clone(),
            // executor: executor.clone(),
        }
    }

    fn execution_loop(
        rx: &Receiver<Arc<Task>>,
        executor: Arc<Mutex<Executor>>,
    ) -> Result<(), Box<dyn std::error::Error>> {
        loop {
            while let Ok(task) = rx.try_recv() {
                let mut future_opt = task.future.lock().expect("MUTEX COULD NOT GET!");
                match future_opt.as_mut() {
                    None => {
                        panic!(
                            "IS FUTURE NONE: {:?}, taskid: {:?}",
                            future_opt.is_none(),
                            task.future.type_id()
                        );
                    }
                    Some(future) => {
                        let waker = Task::into_waker(task.clone());
                        let mut context = Context::from_waker(&waker);

                        let pinned = future.as_mut();
                        pinned.poll(&mut context);
                    }
                }
            }

            if let Ok(mut guard) = executor.try_lock() {
                guard.handle_iouring();
            };
        }
    }
}
