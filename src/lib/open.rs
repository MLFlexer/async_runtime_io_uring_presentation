use std::{
    cell::OnceCell,
    ffi::CString,
    future::Future,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};

use futures::task::AtomicWaker;
use io_uring::{cqueue, opcode, squeue, types};
use libc::AT_FDCWD;

use crate::lib::util::{executor::SQE_SENDER, ring::get_id};

use super::util::{executor::FutureState, helpers::AtomicSharedCell};

pub struct OpenFuture {
    shared_state: Arc<AtomicSharedCell<SharedState>>,
    path_name: CString,
}

struct SharedState {
    id: OnceCell<u64>,
    sqes: OnceCell<[squeue::Entry; 1]>,
    cqes: OnceCell<cqueue::Entry>,

    waker: AtomicWaker,
}

unsafe impl Send for SharedState {}
unsafe impl Sync for SharedState {}

impl Future for OpenFuture {
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
                        let open_e =
                            opcode::OpenAt::new(types::Fd(AT_FDCWD), self.path_name.as_ptr())
                                .build()
                                .user_data(id);
                        shared_state.sqes.set([open_e]);
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

impl OpenFuture {
    pub fn new(path_name: CString) -> Self {
        // owned buffer as to now drop or mutate before finishing
        let shared_state = Arc::new(AtomicSharedCell::new(SharedState {
            waker: AtomicWaker::new(),
            cqes: OnceCell::new(),
            sqes: OnceCell::new(),
            id: OnceCell::new(),
        }));

        OpenFuture {
            shared_state,
            path_name,
        }
    }
}

impl FutureState for SharedState {
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
