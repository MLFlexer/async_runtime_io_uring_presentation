use std::{
    cell::OnceCell,
    future::Future,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};

use futures::task::AtomicWaker;
use io_uring::{
    cqueue, opcode,
    squeue::{self},
    types,
};

use crate::lib::util::{
    executor::{spawn, FutureState, SQE_SENDER},
    helpers::AtomicSharedCell,
    ring::get_id,
};

pub struct AcceptMultiFuture<F, Fut>
where
    F: Fn(i32) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = ()> + Send + Sync + 'static,
{
    shared_state: Arc<AtomicSharedCell<SharedState<F, Fut>>>,
    fd: i32,
}

struct SharedState<F, Fut>
where
    F: Fn(i32) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = ()> + Send + Sync + 'static,
{
    id: OnceCell<u64>,
    sqes: OnceCell<[squeue::Entry; 1]>,
    cqes: OnceCell<cqueue::Entry>,
    accept_callback: F,

    waker: AtomicWaker,
}

unsafe impl<F, Fut> Send for SharedState<F, Fut>
where
    F: Fn(i32) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = ()> + Send + Sync + 'static,
{
}
unsafe impl<F, Fut> Sync for SharedState<F, Fut>
where
    F: Fn(i32) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = ()> + Send + Sync + 'static,
{
}

impl<F, Fut> Future for AcceptMultiFuture<F, Fut>
where
    F: Fn(i32) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = ()> + Send + Sync + 'static,
{
    type Output = i32;
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let res = self
            .shared_state
            .with_inner(|shared_state| match shared_state.id.get() {
                None => {
                    shared_state.waker.register(cx.waker());
                    {
                        let id = get_id();
                        shared_state.id.set(id);
                        let acc_multi_e = opcode::AcceptMulti::new(types::Fd(self.fd))
                            .build()
                            .user_data(id);
                        shared_state.sqes.set([acc_multi_e]);
                    }

                    return Poll::Pending;
                }
                Some(_) => todo!(),
            });
        if res.is_pending() {
            let shared_state_clone = self.shared_state.clone();
            SQE_SENDER
                .get()
                .unwrap()
                .send(shared_state_clone)
                .expect("Could not send state");
        }
        return res;
    }
}

impl<F, Fut> AcceptMultiFuture<F, Fut>
where
    F: Fn(i32) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = ()> + Send + Sync + 'static,
{
    pub fn new(fd: i32, f: F) -> Self {
        let shared_state = Arc::new(AtomicSharedCell::new(SharedState {
            waker: AtomicWaker::new(),
            cqes: OnceCell::new(),
            sqes: OnceCell::new(),
            id: OnceCell::new(),
            accept_callback: f,
        }));

        AcceptMultiFuture { shared_state, fd }
    }
}

impl<F, Fut> FutureState for SharedState<F, Fut>
where
    F: Fn(i32) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = ()> + Send + Sync + 'static,
{
    fn print(&self) {
        println!("ACC MULTI print!");
    }
    fn get_sqes(&self) -> &[squeue::Entry] {
        match self.sqes.get() {
            None => &[],
            Some(arr) => arr,
        }
    }

    fn get_id(&self) -> Option<u64> {
        self.id.clone().into_inner()
    }

    fn wake(&self) {
        if let Some(cqe) = self.cqes.get() {
            self.waker.wake()
        }
    }

    fn push_cqe(&self, cqe: cqueue::Entry) {
        if io_uring::cqueue::more(cqe.flags()) {
            if cqe.result() < 0 {
                todo!();
            } else {
                let fd = cqe.result();

                spawn((self.accept_callback)(fd));
            }
        } else {
            if cqe.result() < 0 {
                todo!();
            } else {
                todo!();
            }
        }
    }

    fn is_complete(&self) -> bool {
        false
    }
}
