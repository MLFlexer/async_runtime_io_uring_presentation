use std::{
    cell::{OnceCell, RefCell},
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

pub struct RWCFuture<T: AsRef<[u8]>, F: AsRef<[u8]>> {
    shared_state: Arc<AtomicSharedCell<SharedState<T, F>>>,
    fd: i32,
}

struct SharedState<T: AsRef<[u8]>, F: AsRef<[u8]>> {
    write_buf: Option<T>,
    read_buf: Option<F>,
    id: OnceCell<u64>,
    sqes: OnceCell<[squeue::Entry; 3]>,
    cqes: RefCell<[Option<cqueue::Entry>; 3]>,

    waker: AtomicWaker,
}

impl<
        T: AsRef<[u8]> + 'static + std::marker::Sync + std::marker::Send,
        F: AsRef<[u8]> + 'static + std::marker::Sync + std::marker::Send,
    > Future for RWCFuture<T, F>
{
    type Output = ([i32; 3], F, T);
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let res = self.shared_state.with_inner(|shared_state| {
            let cqes = shared_state.cqes.borrow();

            match cqes.as_slice() {
                [Some(read), Some(write), Some(close)] => {
                    let read_buf = shared_state.read_buf.take().unwrap();
                    let write_buf = shared_state.write_buf.take().unwrap();
                    return Poll::Ready((
                        [read.result(), write.result(), close.result()],
                        read_buf,
                        write_buf,
                    ));
                }
                [None, None, None] => {
                    shared_state.waker.register(cx.waker());
                    let id = get_id();
                    shared_state.id.set(id).expect("ALREADY SET!");
                    let read_buf: &[u8] = shared_state.read_buf.as_ref().unwrap().as_ref();
                    let read_e = opcode::Read::new(
                        types::Fd(self.fd),
                        read_buf.as_ptr().cast_mut(),
                        read_buf.len() as u32,
                    )
                    .build()
                    .user_data(id)
                    .flags(Flags::IO_LINK);
                    let write_buf: &[u8] = shared_state.write_buf.as_ref().unwrap().as_ref();
                    let write_e = opcode::Write::new(
                        types::Fd(self.fd),
                        write_buf.as_ptr(),
                        write_buf.len() as u32,
                    )
                    .build()
                    .user_data(id)
                    .flags(Flags::IO_LINK);
                    let close_e = opcode::Close::new(types::Fd(self.fd)).build().user_data(id);

                    shared_state.sqes.set([read_e, write_e, close_e]);

                    return Poll::Pending;
                }
                _ => {
                    todo!()
                }
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

impl<T: AsRef<[u8]>, F: AsRef<[u8]>> RWCFuture<T, F> {
    pub fn new(read_buf: F, write_buf: T, fd: i32) -> Self {
        // owned buffer as to now drop or mutate before finishing
        let shared_state = Arc::new(AtomicSharedCell::new(SharedState {
            waker: AtomicWaker::new(),
            write_buf: Some(write_buf),
            read_buf: Some(read_buf),
            cqes: RefCell::new([None, None, None]),
            sqes: OnceCell::new(),
            id: OnceCell::new(),
        }));

        RWCFuture { shared_state, fd }
    }
}

impl<T: AsRef<[u8]>, F: AsRef<[u8]>> FutureState for SharedState<T, F> {
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
        match self.cqes.borrow().as_slice() {
            [None, None, None] => {
                self.waker.wake();
            }
            [Some(_), Some(_), Some(_)] => {
                self.waker.wake();
            }
            _ => todo!(),
        }
    }

    fn push_cqe(&self, cqe: cqueue::Entry) {
        // println!("CQE!: {:?}", cqe);
        // if cqe.result() < 1 {
        //     println!(
        //         "bad result: {:?}",
        //         std::io::Error::from_raw_os_error(-cqe.result())
        //     );
        // }
        let mut cqes = self.cqes.borrow_mut();
        match cqes.as_slice() {
            [None, None, None] => {
                cqes[0] = Some(cqe);
            }
            [Some(_), None, None] => {
                cqes[1] = Some(cqe);
            }
            [Some(_), Some(_), None] => {
                cqes[2] = Some(cqe);
            }
            _ => todo!(),
        }
    }

    fn is_complete(&self) -> bool {
        self.cqes.borrow()[2].is_some()
    }
    fn print(&self) {
        println!("READ");
    }
}
