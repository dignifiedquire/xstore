//! Bounded channel implementation based on https://github.com/crossbeam-rs/crossbeam/blob/master/crossbeam-channel/src/flavors/array.rs
//!
//! Modified to be able to be used between both Go and Rust

// Current restrictions:
// - All atomic accesses must be done usin g`Ordering::SeqCst`, as this is the only ordering that `sync/atomics` for go lang supports.

use core::cell::UnsafeCell;
use core::sync::atomic::{AtomicU64, Ordering::SeqCst};
use core::{mem, ptr};

use crossbeam_utils::Backoff;

pub type Message = (*mut u8, u64);

#[derive(Debug, PartialEq)]
pub enum TrySendError {
    Full(Message),
    Disconnected(Message),
}

#[derive(Debug, PartialEq)]
pub enum SendError {
    Disconnected(Message),
}

#[derive(PartialEq, Eq, Clone, Copy, Debug)]
pub enum TryRecvError {
    Empty,
    Disconnected,
}

#[derive(PartialEq, Eq, Clone, Copy, Debug)]
pub enum RecvError {
    Disconnected,
}

pub fn message_from_bytes(buffer: &[u8]) -> Message {
    let len = buffer.len();
    let b = buffer.to_vec().into_boxed_slice();
    message_from_box(b, len)
}

pub fn message_from_box(b: Box<[u8]>, len: usize) -> Message {
    (Box::into_raw(b).cast(), len as _)
}

/// The token type for the array flavor.
#[repr(C)]
#[derive(Debug)]
pub struct Token {
    /// Slot to read from or write to.
    slot: *const u8,

    /// Stamp to store into the slot after reading or writing.
    stamp: u64,
}

impl Default for Token {
    #[inline]
    fn default() -> Self {
        Token {
            slot: ptr::null(),
            stamp: 0,
        }
    }
}

/// A slot in a channel.
#[derive(Debug)]
#[repr(C)]
pub struct Slot {
    /// The current stamp.
    stamp: AtomicU64,

    /// Inlined message, to avoid allocations in go.
    msg_ptr: UnsafeCell<*mut u8>,
    msg_len: UnsafeCell<u64>,
}

impl AsRef<[u8]> for Slot {
    fn as_ref(&self) -> &[u8] {
        unsafe {
            core::slice::from_raw_parts(
                self.msg_ptr.get() as *const _,
                self.msg_len.get().read().try_into().unwrap(),
            )
        }
    }
}

#[repr(C)]
pub struct Channel {
    /// The head of the channel.
    ///
    /// This value is a "stamp" consisting of an index into the buffer, a mark bit, and a lap, but
    /// packed into a single `usize`. The lower bits represent the index, while the upper bits
    /// represent the lap. The mark bit in the head is always zero.
    ///
    /// Messages are popped from the head of the channel.
    // TODO: add cache padding back
    head: AtomicU64,

    /// The tail of the channel.
    ///
    /// This value is a "stamp" consisting of an index into the buffer, a mark bit, and a lap, but
    /// packed into a single `usize`. The lower bits represent the index, while the upper bits
    /// represent the lap. The mark bit indicates that the channel is disconnected.
    ///
    /// Messages are pushed into the tail of the channel.
    // TODO: add cache padding back
    tail: AtomicU64,

    /// The buffer holding slots.
    buffer: *mut Slot,

    /// The channel capacity.
    cap: usize,

    /// A stamp with the value of `{ lap: 1, mark: 0, index: 0 }`.
    one_lap: u64,

    /// If this bit is set in the tail, that means the channel is disconnected.
    mark_bit: u64,
}

impl Channel {
    pub fn with_capacity(cap: u32) -> Self {
        assert!(cap > 0, "capacity must be positive");
        let cap = cap as usize;

        // Compute constants `mark_bit` and `one_lap`.
        let mark_bit = (cap as u64 + 1).next_power_of_two();
        let one_lap = mark_bit * 2;

        // Head is initialized to `{ lap: 0, mark: 0, index: 0 }`.
        let head = 0;
        // Tail is initialized to `{ lap: 0, mark: 0, index: 0 }`.
        let tail = 0;

        // Allocate a buffer of `cap` slots.
        let buffer = {
            let mut v = Vec::<Slot>::with_capacity(cap as usize);
            let ptr = v.as_mut_ptr();
            mem::forget(v);
            ptr
        };

        // Initialize stamps in the slots.
        for i in 0..cap {
            unsafe {
                // Set the stamp to `{ lap: 0, mark: 0, index: i }`.
                let slot = buffer.add(i);
                ptr::write(&mut (*slot).stamp, AtomicU64::new(i as u64));
            }
        }

        Channel {
            buffer,
            cap,
            one_lap,
            mark_bit,
            head: AtomicU64::new(head),
            tail: AtomicU64::new(tail),
        }
    }

    /// Attempts to reserve a slot for sending a message.
    fn start_send(&self, token: &mut Token) -> bool {
        let backoff = Backoff::new();
        let mut tail = self.tail.load(SeqCst);

        loop {
            // Check if the channel is disconnected.
            if tail & self.mark_bit != 0 {
                token.slot = ptr::null();
                token.stamp = 0;
                return true;
            }

            // Deconstruct the tail.
            let index = tail & (self.mark_bit - 1);
            let lap = tail & !(self.one_lap - 1);

            // Inspect the corresponding slot.
            let slot = unsafe { &*self.buffer.add(index.try_into().unwrap()) };
            let stamp = slot.stamp.load(SeqCst);

            // If the tail and the stamp match, we may attempt to push.
            if tail == stamp {
                let new_tail = if index + 1 < self.cap as u64 {
                    // Same lap, incremented index.
                    // Set to `{ lap: lap, mark: 0, index: index + 1 }`.
                    tail + 1
                } else {
                    // One lap forward, index wraps around to zero.
                    // Set to `{ lap: lap.wrapping_add(1), mark: 0, index: 0 }`.
                    lap.wrapping_add(self.one_lap)
                };

                // Try moving the tail.
                match self
                    .tail
                    .compare_exchange_weak(tail, new_tail, SeqCst, SeqCst)
                {
                    Ok(_) => {
                        // Prepare the token for the follow-up call to `write`.
                        token.slot = slot as *const Slot as *const u8;
                        token.stamp = tail + 1;
                        return true;
                    }
                    Err(t) => {
                        tail = t;
                        backoff.spin();
                    }
                }
            } else if stamp.wrapping_add(self.one_lap) == tail + 1 {
                let head = self.head.load(SeqCst);

                // If the head lags one lap behind the tail as well...
                if head.wrapping_add(self.one_lap) == tail {
                    // ...then the channel is full.
                    return false;
                }

                backoff.spin();
                tail = self.tail.load(SeqCst);
            } else {
                // Snooze because we need to wait for the stamp to get updated.
                backoff.snooze();
                tail = self.tail.load(SeqCst);
            }
        }
    }

    /// Writes a message into the channel.
    pub unsafe fn write(&self, token: &mut Token, msg: Message) -> Result<(), Message> {
        // If there is no slot, the channel is disconnected.
        if token.slot.is_null() {
            return Err(msg);
        }

        let slot: &Slot = &*(token.slot as *const Slot);

        // Write the message into the slot and update the stamp.
        slot.msg_ptr.get().write(msg.0);
        slot.msg_len.get().write(msg.1);
        slot.stamp.store(token.stamp, SeqCst);

        Ok(())
    }

    /// Attempts to send a message into the channel.
    pub fn try_send(&self, msg: Message) -> Result<(), TrySendError> {
        let token = &mut Token::default();
        if self.start_send(token) {
            unsafe { self.write(token, msg).map_err(TrySendError::Disconnected) }
        } else {
            Err(TrySendError::Full(msg))
        }
    }

    /// Sends a message into the channel.
    pub fn send(&self, msg: Message) -> Result<(), SendError> {
        let token = &mut Token::default();
        loop {
            // Try sending a message several times.
            let backoff = Backoff::new();
            loop {
                if self.start_send(token) {
                    let res = unsafe { self.write(token, msg) };
                    return res.map_err(SendError::Disconnected);
                }

                if backoff.is_completed() {
                    break;
                } else {
                    backoff.snooze();
                }
            }

            // TODO: can this be better?
            backoff.spin();
        }
    }

    /// Returns `true` if the channel is disconnected.
    pub fn is_disconnected(&self) -> bool {
        self.tail.load(SeqCst) & self.mark_bit != 0
    }

    /// Reads a message from the channel.
    pub unsafe fn read(&self, token: &mut Token) -> Result<Message, ()> {
        if token.slot.is_null() {
            // The channel is disconnected.
            return Err(());
        }

        let slot: &Slot = &*(token.slot as *const Slot);
        // Read the message from the slot and update the stamp.
        let msg = (slot.msg_ptr.get().read(), slot.msg_len.get().read());
        slot.stamp.store(token.stamp, SeqCst);

        Ok(msg)
    }

    /// Attempts to reserve a slot for receiving a message.
    fn start_recv(&self, token: &mut Token) -> bool {
        let backoff = Backoff::new();
        let mut head = self.head.load(SeqCst);
        loop {
            // Deconstruct the head.
            let index = head & (self.mark_bit - 1);
            let lap = head & !(self.one_lap - 1);

            // Inspect the corresponding slot.
            let slot = unsafe { &*self.buffer.add(index.try_into().unwrap()) };
            let stamp = slot.stamp.load(SeqCst);

            // If the the stamp is ahead of the head by 1, we may attempt to pop.
            if head + 1 == stamp {
                let new = if index + 1 < self.cap as u64 {
                    // Same lap, incremented index.
                    // Set to `{ lap: lap, mark: 0, index: index + 1 }`.
                    head + 1
                } else {
                    // One lap forward, index wraps around to zero.
                    // Set to `{ lap: lap.wrapping_add(1), mark: 0, index: 0 }`.
                    lap.wrapping_add(self.one_lap)
                };

                // Try moving the head.
                match self.head.compare_exchange_weak(head, new, SeqCst, SeqCst) {
                    Ok(_) => {
                        // Prepare the token for the follow-up call to `read`.
                        token.slot = slot as *const Slot as *const u8;
                        token.stamp = head.wrapping_add(self.one_lap);
                        return true;
                    }
                    Err(h) => {
                        head = h;
                        backoff.spin();
                    }
                }
            } else if stamp == head {
                let tail = self.tail.load(SeqCst);

                // If the tail equals the head, that means the channel is empty.
                if (tail & !self.mark_bit) == head {
                    // If the channel is disconnected...
                    if tail & self.mark_bit != 0 {
                        // ...then receive an error.
                        token.slot = ptr::null();
                        token.stamp = 0;
                        return true;
                    } else {
                        // Otherwise, the receive operation is not ready.
                        return false;
                    }
                }

                backoff.spin();
                head = self.head.load(SeqCst);
            } else {
                // Snooze because we need to wait for the stamp to get updated.
                backoff.snooze();
                head = self.head.load(SeqCst);
            }
        }
    }

    /// Attempts to receive a message without blocking.
    pub fn try_recv(&self) -> Result<Message, TryRecvError> {
        let token = &mut Token::default();

        if self.start_recv(token) {
            unsafe { self.read(token).map_err(|_| TryRecvError::Disconnected) }
        } else {
            Err(TryRecvError::Empty)
        }
    }

    /// Receives a message from the channel.
    pub fn recv(&self) -> Result<Message, RecvError> {
        let token = &mut Token::default();
        loop {
            // Try receiving a message several times.
            let backoff = Backoff::new();
            loop {
                if self.start_recv(token) {
                    let res = unsafe { self.read(token) };
                    return res.map_err(|_| RecvError::Disconnected);
                }

                if backoff.is_completed() {
                    break;
                } else {
                    backoff.snooze();
                }
            }

            // TODO: improve
            backoff.spin();
        }
    }

    /// Returns the current number of messages inside the channel.
    pub fn len(&self) -> usize {
        loop {
            // Load the tail, then load the head.
            let tail = self.tail.load(SeqCst);
            let head = self.head.load(SeqCst);

            // If the tail didn't change, we've got consistent values to work with.
            if self.tail.load(SeqCst) == tail {
                let hix = head & (self.mark_bit - 1);
                let tix = tail & (self.mark_bit - 1);

                return if hix < tix {
                    usize::try_from(tix - hix).unwrap()
                } else if hix > tix {
                    self.cap - usize::try_from(hix + tix).unwrap()
                } else if (tail & !self.mark_bit) == head {
                    0
                } else {
                    self.cap
                };
            }
        }
    }

    /// Returns the capacity of the channel.
    pub fn capacity(&self) -> Option<usize> {
        Some(self.cap)
    }

    /// Returns `true` if the channel is empty.
    pub fn is_empty(&self) -> bool {
        let head = self.head.load(SeqCst);
        let tail = self.tail.load(SeqCst);

        // Is the tail equal to the head?
        //
        // Note: If the head changes just before we load the tail, that means there was a moment
        // when the channel was not empty, so it is safe to just return `false`.
        (tail & !self.mark_bit) == head
    }

    /// Returns `true` if the channel is full.
    pub fn is_full(&self) -> bool {
        let tail = self.tail.load(SeqCst);
        let head = self.head.load(SeqCst);

        // Is the head lagging one lap behind tail?
        //
        // Note: If the tail changes just before we load the head, that means there was a moment
        // when the channel was not full, so it is safe to just return `false`.
        head.wrapping_add(self.one_lap) == tail & !self.mark_bit
    }
}

impl Drop for Channel {
    fn drop(&mut self) {
        // Get the index of the head.
        let hix = self.head.load(SeqCst) & (self.mark_bit - 1);

        let cap = self.cap as u64;
        // Loop over all slots that hold a message and drop them.
        for i in 0..self.len() as u64 {
            // Compute the index of the next slot holding a message.
            let index = if hix + i < cap {
                hix + i
            } else {
                hix + i - cap
            };

            let index = usize::try_from(index).expect("out of bounds");
            unsafe {
                self.buffer.add(index).drop_in_place();
            }
        }

        // Finally, deallocate the buffer, but don't run any destructors.
        unsafe {
            Vec::from_raw_parts(self.buffer, 0, self.cap);
        }
    }
}

unsafe impl Send for Channel {}
unsafe impl Sync for Channel {}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;

    fn make_message(i: u8) -> Message {
        let len = 10;
        let data = vec![i; len].into_boxed_slice();
        message_from_box(data, len)
    }

    #[test]
    fn message_drop() {
        for i in 0..100 {
            let msg = make_message(i);
            let _msg1 = msg.clone();
        }
    }

    fn msg_slice<'a>(msg: (*mut u8, u64)) -> &'a mut [u8] {
        unsafe { core::slice::from_raw_parts_mut(msg.0, msg.1.try_into().unwrap()) }
    }

    #[test]
    fn test_basics_do() {
        let channel = Channel::with_capacity(10);
        for i in 0..10 {
            let msg = make_message(i);
            channel.send(msg).unwrap();
        }

        for i in 0..10 {
            let msg = channel.recv().unwrap();
            assert_eq!(msg_slice(msg), &vec![i; 10]);
        }
    }

    #[test]
    fn test_basics_par() {
        let channel = Arc::new(Channel::with_capacity(10));

        let sender = Arc::clone(&channel);
        let receiver = Arc::clone(&channel);

        let a = std::thread::spawn(move || {
            for i in 0..100 {
                let msg = make_message(i);
                sender.send(msg).unwrap();
            }
        });

        let b = std::thread::spawn(move || {
            for i in 0..100 {
                let msg = receiver.recv().unwrap();
                assert_eq!(msg_slice(msg), &vec![i; 10]);
            }
        });

        a.join().unwrap();
        b.join().unwrap();
    }

    #[test]
    fn test_basics_try() {
        let channel = Channel::with_capacity(10);
        for i in 0..10 {
            let msg = make_message(i);
            channel.try_send(msg).unwrap();
        }
        let msg = make_message(10);
        let err = channel.try_send(msg).unwrap_err();
        match err {
            TrySendError::Full(msg) => {
                assert_eq!(msg_slice(msg), &vec![10u8; 10]);
            }
            _ => panic!(),
        }

        for i in 0..10 {
            let msg = channel.try_recv().unwrap();
            assert_eq!(msg_slice(msg), &vec![i; 10]);
        }

        assert_eq!(channel.try_recv(), Err(TryRecvError::Empty));
    }
}
