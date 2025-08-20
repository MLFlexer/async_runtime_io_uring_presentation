use std::{
    cell::{OnceCell, UnsafeCell},
    future::Future,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};

use futures::task::AtomicWaker;
use io_uring::{cqueue, opcode, squeue, types};

use crate::lib::util::{
    executor::{FutureState, SQE_SENDER},
    helpers::AtomicSharedCell,
    ring::get_id,
};

pub struct SendZCFuture<T: AsRef<[u8]>> {
    shared_state: Arc<AtomicSharedCell<SharedState<T>>>,
    fd: i32,
}

struct SharedState<T: AsRef<[u8]>> {
    buf: T,
    id: OnceCell<u64>,
    sqes: OnceCell<[squeue::Entry; 1]>,
    cqes: UnsafeCell<[Option<cqueue::Entry>; 2]>,

    waker: AtomicWaker,
}

unsafe impl<T: AsRef<[u8]>> Send for SharedState<T> {}
unsafe impl<T: AsRef<[u8]>> Sync for SharedState<T> {}

impl<T: AsRef<[u8]> + 'static> Future for SendZCFuture<T> {
    type Output = i32;
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        // println!("Polling writer!: {:?}", self.fd);
        let res = self
            .shared_state
            .with_inner(|shared_state| match shared_state.cqes.get_mut() {
                [None, None] => {
                    shared_state.waker.register(cx.waker());
                    {
                        let id = get_id();
                        shared_state.id.set(id);
                        let buf: &[u8] = shared_state.buf.as_ref();
                        let send_e =
                            opcode::SendZc::new(types::Fd(self.fd), buf.as_ptr(), buf.len() as u32)
                                .build()
                                .user_data(id);
                        shared_state.sqes.set([send_e]);
                    }

                    Poll::Pending
                }
                [None, Some(cqe1)] => Poll::Ready(cqe1.result()),
                [Some(cqe0), Some(cqe1)] => Poll::Ready(cqe0.result()),
                _ => todo!(),
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

impl<T: AsRef<[u8]>> SendZCFuture<T> {
    pub fn new(buf: T, fd: i32) -> Self {
        // owned buffer as to now drop or mutate before finishing
        let shared_state = Arc::new(AtomicSharedCell::new(SharedState {
            waker: AtomicWaker::new(),
            buf,
            cqes: UnsafeCell::new([None, None]),
            sqes: OnceCell::new(),
            id: OnceCell::new(),
        }));

        SendZCFuture { shared_state, fd }
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
        match unsafe { &*self.cqes.get() } {
            [None, None] => {
                self.waker.wake();
            }
            [_, Some(_)] => {
                self.waker.wake();
            }
            _ => return,
        }
    }

    fn push_cqe(&self, cqe: cqueue::Entry) {
        let cqes = unsafe { &mut *self.cqes.get() };
        if io_uring::cqueue::notif(cqe.flags()) {
            if cqes[0].is_some() {
                panic!("Already set!");
            }
            cqes[0] = Some(cqe);
        } else {
            cqes[1] = Some(cqe);
        }
    }

    fn is_complete(&self) -> bool {
        match unsafe { self.cqes.get().as_ref() }.unwrap() {
            [Some(_), Some(_)] => true,
            _ => false,
        }
    }
}
