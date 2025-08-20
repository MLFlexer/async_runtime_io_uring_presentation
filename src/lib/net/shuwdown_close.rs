use std::{
    cell::{OnceCell, UnsafeCell},
    future::Future,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};

use futures::task::AtomicWaker;
use io_uring::{
    cqueue, opcode,
    squeue::{self, Flags},
    types,
};

use crate::lib::util::{
    executor::{FutureState, SQE_SENDER},
    helpers::AtomicSharedCell,
    ring::get_id,
};

pub struct ShutdownAndCloseFuture {
    shared_state: Arc<AtomicSharedCell<SharedState>>,
    fd: i32,
}

struct SharedState {
    id: OnceCell<u64>,
    sqes: OnceCell<[squeue::Entry; 2]>,
    cqes: UnsafeCell<[Option<cqueue::Entry>; 2]>,

    waker: AtomicWaker,
}
unsafe impl Send for SharedState {}
unsafe impl Sync for SharedState {}

impl Future for ShutdownAndCloseFuture {
    type Output = (i32, i32);
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let res = self.shared_state.with_inner(|shared_state| {
            match unsafe { shared_state.cqes.get().as_ref().unwrap() } {
                [Some(cqe0), Some(cqe1)] => Poll::Ready((cqe0.result(), cqe1.result())),
                [None, None] => {
                    shared_state.waker.register(cx.waker());
                    let id = get_id();
                    shared_state.id.set(id);
                    let shutdown_e = opcode::Shutdown::new(types::Fd(self.fd), libc::SHUT_RDWR)
                        .build()
                        .user_data(id)
                        .flags(Flags::IO_LINK);
                    let close_e = opcode::Close::new(types::Fd(self.fd)).build().user_data(id);
                    shared_state.sqes.set([shutdown_e, close_e]);

                    Poll::Pending
                }
                e => {
                    panic!("COULD NOT MATCH: {:?}", e)
                }
            }
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

impl ShutdownAndCloseFuture {
    pub fn new(fd: i32) -> Self {
        let shared_state = Arc::new(AtomicSharedCell::new(SharedState {
            waker: AtomicWaker::new(),
            cqes: UnsafeCell::new([None, None]),
            sqes: OnceCell::new(),
            id: OnceCell::new(),
        }));

        ShutdownAndCloseFuture { shared_state, fd }
    }
}

impl FutureState for SharedState {
    fn print(&self) {
        println!("SHUTDOWN AND CLOSE!");
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
        match unsafe { self.cqes.get().as_ref().unwrap() } {
            [None, None] => {
                self.waker.wake();
            }
            [Some(_), Some(_)] => {
                self.waker.wake();
            }
            _ => return,
        }
    }

    fn push_cqe(&self, cqe: cqueue::Entry) {
        match unsafe { self.cqes.get().as_ref().unwrap() } {
            [None, None] => {
                unsafe { self.cqes.get().as_mut() }.unwrap()[0] = Some(cqe);
            }
            [Some(_), None] => {
                unsafe { self.cqes.get().as_mut() }.unwrap()[1] = Some(cqe);
            }
            _ => panic!("CQEs are already stored!"),
        }
    }

    fn is_complete(&self) -> bool {
        match unsafe { self.cqes.get().as_ref().unwrap() } {
            [Some(_), Some(_)] => true,
            _ => false,
        }
    }
}
