use std::ptr::NonNull;

use crate::channel::Channel;

pub struct Blockstore {
    /// Cgo handle to a blockstore instance in Go.
    handle: i32,
    /// Channel to send messages from Rust to Go.
    sender: NonNull<Channel>,
    /// Channel to receive messages in Rust from Go.
    receiver: NonNull<Channel>,
}

const DEFAULT_CAPACITY: u32 = 5;

impl Blockstore {
    pub fn new_memory() -> Self {
        let sender = Channel::with_capacity(DEFAULT_CAPACITY);
        let receiver = Channel::with_capacity(DEFAULT_CAPACITY);
        let mut store = Blockstore {
            handle: 0,
            sender: NonNull::new(Box::into_raw(Box::new(sender))).unwrap(),
            receiver: NonNull::new(Box::into_raw(Box::new(receiver))).unwrap(),
        };

        unsafe {
            store.handle =
                crate::sys::xstore_new_memory(store.sender.as_ptr(), store.receiver.as_ptr());
        }

        store
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_create_memory() {
        let _store = Blockstore::new_memory();
    }
}
