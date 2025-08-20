use std::{
    cell::OnceCell,
    future::Future,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};

use futures::task::AtomicWaker;
use io_uring::{cqueue, opcode, squeue, types};

use crate::lib::util::{executor::SQE_SENDER, ring::get_id};

use super::util::{executor::FutureState, helpers::AtomicSharedCell};

pub struct WriterFuture<T: AsRef<[u8]>> {
    shared_state: Arc<AtomicSharedCell<SharedState<T>>>,
    fd: i32,
}

struct SharedState<T: AsRef<[u8]>> {
    buf: T,
    id: OnceCell<u64>,
    sqes: OnceCell<[squeue::Entry; 1]>,
    cqes: OnceCell<cqueue::Entry>,

    waker: AtomicWaker,
}

unsafe impl<T: AsRef<[u8]>> Send for SharedState<T> {}
unsafe impl<T: AsRef<[u8]>> Sync for SharedState<T> {}

impl<T: AsRef<[u8]> + 'static> Future for WriterFuture<T> {
    type Output = i32;
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let res = self.shared_state.with_inner(|shared_state| {
            match shared_state.cqes.get() {
                Some(cqe) => return Poll::Ready(cqe.result()),
                None => {
                    shared_state.waker.register(cx.waker());
                    {
                        let id = get_id();
                        shared_state.id.set(id);
                        let buf: &[u8] = shared_state.buf.as_ref();
                        let write_e =
                            opcode::Write::new(types::Fd(self.fd), buf.as_ptr(), buf.len() as u32)
                                .build()
                                .user_data(id);
                        shared_state.sqes.set([write_e]);
                    }

                    return Poll::Pending;
                }
            };
        });

        if res.is_pending() {
            let state = self.shared_state.clone();
            SQE_SENDER
                .get()
                .unwrap()
                .send(state)
                .expect("Could not send state");
        }
        return res;
    }
}

impl<T: AsRef<[u8]>> WriterFuture<T> {
    pub fn new(buf: T, fd: i32) -> Self {
        // owned buffer as to now drop or mutate before finishing
        let shared_state = Arc::new(AtomicSharedCell::new(SharedState {
            waker: AtomicWaker::new(),
            buf,
            cqes: OnceCell::new(),
            sqes: OnceCell::new(),
            id: OnceCell::new(),
        }));

        WriterFuture { shared_state, fd }
    }
}

impl<T: AsRef<[u8]>> FutureState for SharedState<T> {
    fn print(&self) {
        println!("WRITER");
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
        self.waker.wake();
    }

    fn push_cqe(&self, cqe: cqueue::Entry) {
        self.cqes.set(cqe);
    }

    fn is_complete(&self) -> bool {
        self.cqes.get().is_some()
    }
}
