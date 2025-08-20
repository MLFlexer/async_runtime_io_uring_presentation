use std::{
    cell::UnsafeCell,
    future::Future,
    pin::Pin,
    sync::{atomic::AtomicBool, mpmc::Sender, Arc, Mutex},
    task::{RawWaker, RawWakerVTable, Wake, Waker},
};

use super::{executor::FutureState, thread_pool::ThreadPool};

pub type PinBoxFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

/// A future that can reschedule itself to be polled by an `Executor`.
pub struct Task {
    /// In-progress future that should be pushed to completion.
    ///
    /// The `Mutex` is not necessary for correctness, since we only have
    /// one thread executing tasks at once. However, Rust isn't smart
    /// enough to know that `future` is only mutated from one thread,
    /// so we need to use the `Mutex` to prove thread-safety. A production
    /// executor would not need this, and could use `UnsafeCell` instead.
    pub future: Mutex<Option<PinBoxFuture<'static, ()>>>,

    /// Handle to place the task itself back onto the task queue.
    pub task_sender: Sender<Arc<Task>>,
}

impl Task {
    pub fn into_waker(arc_self: Arc<Self>) -> Waker {
        unsafe { Waker::from_raw(Self::raw_waker(arc_self)) }
    }

    unsafe fn raw_waker(arc_self: Arc<Self>) -> RawWaker {
        let data = Arc::into_raw(arc_self) as *const ();
        RawWaker::new(data, &VTABLE)
    }
}

unsafe impl Sync for Task {}

impl Wake for Task {
    fn wake(self: Arc<Self>) {
        ThreadPool::try_send_task(self.clone()).expect("COULD NOT SEND TASK TO THREAD POOL!");
    }
}

static VTABLE: RawWakerVTable = RawWakerVTable::new(clone, wake, wake_by_ref, drop);

unsafe fn clone(ptr: *const ()) -> RawWaker {
    let arc = Arc::<Task>::from_raw(ptr as *const Task);
    let cloned = arc.clone();
    std::mem::forget(arc); // avoid dropping original
    RawWaker::new(Arc::into_raw(cloned) as *const (), &VTABLE)
}

unsafe fn wake(ptr: *const ()) {
    let arc = Arc::<Task>::from_raw(ptr as *const Task);
    Task::wake_by_ref(&arc); // wake consumes self in std Waker, but we own the Arc
                             // Arc is dropped here
}

unsafe fn wake_by_ref(ptr: *const ()) {
    let arc = Arc::<Task>::from_raw(ptr as *const Task);
    Task::wake_by_ref(&arc);
    std::mem::forget(arc); // don't drop
}

unsafe fn drop(ptr: *const ()) {
    std::mem::drop(Arc::<Task>::from_raw(ptr.cast()));
}

pub struct AtomicSharedCell<T> {
    inner: UnsafeCell<T>,
    in_use: AtomicBool,
}

impl<T> AtomicSharedCell<T> {
    pub fn new(inner: T) -> Self {
        Self {
            inner: UnsafeCell::new(inner),
            in_use: AtomicBool::new(false),
        }
    }

    pub fn with_inner<'a, F>(&'a self, f: impl FnOnce(&'a mut T) -> F) -> F {
        #[cfg(feature = "atomic_shared_cell_check")]
        let in_use = self
            .in_use
            .compare_exchange(
                false,
                true,
                std::sync::atomic::Ordering::Acquire,
                std::sync::atomic::Ordering::Relaxed,
            )
            .expect("AtomicSendCell in use!");

        let inner = unsafe { &mut *self.inner.get() };
        let res = f(inner);

        #[cfg(feature = "atomic_shared_cell_check")]
        self.in_use
            .store(false, std::sync::atomic::Ordering::Release);
        res
    }
}

unsafe impl<T> Send for AtomicSharedCell<T> {}
unsafe impl<T> Sync for AtomicSharedCell<T> {}

impl<T: FutureState> FutureState for AtomicSharedCell<T> {
    fn get_sqes(&self) -> &[io_uring::squeue::Entry] {
        self.with_inner(|x| x.get_sqes())
    }

    fn get_id(&self) -> Option<u64> {
        self.with_inner(|x| x.get_id())
    }

    fn push_cqe(&self, cqe: io_uring::cqueue::Entry) {
        self.with_inner(|x| x.push_cqe(cqe))
    }

    fn wake(&self) {
        unsafe { &mut *self.inner.get() }.wake();
    }

    fn is_complete(&self) -> bool {
        self.with_inner(|x| x.is_complete())
    }

    fn print(&self) {
        self.with_inner(|x| x.print())
    }
}
