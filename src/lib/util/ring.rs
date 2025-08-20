use std::sync::atomic::AtomicU64;

static ID: AtomicU64 = AtomicU64::new(0);

pub fn get_id() -> u64 {
    ID.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
}
