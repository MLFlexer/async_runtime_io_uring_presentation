use std::{
    cell::OnceCell,
    future::Future,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};

use futures::task::AtomicWaker;
use io_uring::{cqueue, opcode, squeue, types};

use crate::lib::util::executor::SQE_SENDER;

use super::util::{executor::FutureState, helpers::AtomicSharedCell, ring::get_id};

pub struct ReaderFuture<T: AsMut<[u8]> + AsRef<[u8]>> {
    shared_state: Arc<AtomicSharedCell<SharedState<T>>>,
    fd: i32,
}

struct SharedState<T: AsMut<[u8]> + AsRef<[u8]>> {
    buf: Option<T>,
    id: OnceCell<u64>,
    sqes: OnceCell<[squeue::Entry; 1]>,
    cqes: OnceCell<cqueue::Entry>,

    waker: AtomicWaker,
}

impl<T: AsMut<[u8]> + AsRef<[u8]> + 'static + std::marker::Sync + std::marker::Send> Future
    for ReaderFuture<T>
{
    type Output = (i32, T);
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let res = self.shared_state.with_inner(|shared_state| {
            if let Some(cqe) = shared_state.cqes.get() {
                if cqe.result() < 1 {
                    println!(
                        "bad result: {:?}",
                        std::io::Error::from_raw_os_error(-cqe.result())
                    );
                }
                let buf = shared_state.buf.take().unwrap();
                Poll::Ready((cqe.result(), buf))
            } else {
                shared_state.waker.register(cx.waker());
                {
                    let id = get_id();
                    shared_state.id.set(id).expect("ALREADY SET!");
                    let buf: &[u8] = shared_state.buf.as_ref().unwrap().as_ref();
                    let read_e = opcode::Read::new(
                        types::Fd(self.fd),
                        buf.as_ptr().cast_mut(),
                        buf.len() as u32,
                    )
                    .build()
                    .user_data(id);
                    shared_state.sqes.set([read_e]);
                }

                Poll::Pending
            }
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

impl<T: AsMut<[u8]> + AsRef<[u8]>> ReaderFuture<T> {
    pub fn new(buf: T, fd: i32) -> Self {
        // owned buffer as to now drop or mutate before finishing
        let shared_state = Arc::new(AtomicSharedCell::new(SharedState {
            waker: AtomicWaker::new(),
            buf: Some(buf),
            cqes: OnceCell::new(),
            sqes: OnceCell::new(),
            id: OnceCell::new(),
        }));

        ReaderFuture { shared_state, fd }
    }
}

impl<T: AsMut<[u8]> + AsRef<[u8]>> FutureState for SharedState<T> {
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
        let x = self.cqes.set(cqe);
        if x.is_err() {
            panic!("cqe already set for read: {:?}", x)
        }
    }

    fn is_complete(&self) -> bool {
        self.cqes.get().is_some()
    }
    fn print(&self) {
        println!("READ");
    }
}
