use std::{cell::UnsafeCell, ptr::NonNull};

use cid::Cid;

use crate::channel::{Channel, Message};

pub struct Blockstore {
    /// Cgo handle to a blockstore instance in Go.
    handle: i32,
    /// Channel to send messages from Rust to Go.
    sender: NonNull<Channel>,
    /// Channel to receive messages in Rust from Go.
    receiver: NonNull<Channel>,
    sender_buffers: [UnsafeCell<Box<[u8]>>; DEFAULT_CAPACITY as usize],
    receiver_buffers: [UnsafeCell<Box<[u8]>>; DEFAULT_CAPACITY as usize],
}

const DEFAULT_CAPACITY: u32 = 5;
const MSG_BUFFER_SIZE: usize = 1024;

impl Blockstore {
    pub fn new_memory() -> Self {
        let sender = Channel::with_capacity(DEFAULT_CAPACITY);
        let receiver = Channel::with_capacity(DEFAULT_CAPACITY);

        let sender_buffers = [(); DEFAULT_CAPACITY as usize]
            .map(|_| UnsafeCell::new(vec![0u8; MSG_BUFFER_SIZE].into_boxed_slice()));
        let receiver_buffers = [(); DEFAULT_CAPACITY as usize]
            .map(|_| UnsafeCell::new(vec![0u8; MSG_BUFFER_SIZE].into_boxed_slice()));

        let mut store = Blockstore {
            handle: 0,
            sender: NonNull::new(Box::into_raw(Box::new(sender))).unwrap(),
            receiver: NonNull::new(Box::into_raw(Box::new(receiver))).unwrap(),
            sender_buffers,
            receiver_buffers,
        };

        unsafe {
            store.handle =
                crate::sys::xstore_new_memory(store.sender.as_ptr(), store.receiver.as_ptr());
        }

        store
    }

    pub fn has_cgo(&self, c: &Cid) -> Result<bool, Error> {
        let bytes = c.to_bytes();
        unsafe {
            match crate::sys::xstore_has(self.handle, bytes.as_ptr(), bytes.len() as i32) {
                // We shouldn't get an "error not found" here, but there's no reason to be strict
                // about it.
                0 | -2 => Ok(false),
                1 => Ok(true),
                // Panic on unknown values. There's a bug in the program.
                r @ 2.. => panic!("invalid return value from has: {}", r),
                // Panic if the store isn't registered. This means something _very_ unsafe is going
                // on and there is a bug in the program.
                -1 => panic!("blockstore {} not registered", self.handle),
                // Otherwise, return "other". We should add error codes in the future.
                _ => Err(Error::Other),
            }
        }
    }

    pub fn has(&self, c: &Cid) -> Result<bool, Error> {
        let msg = self.create_has_request(c);
        self.send_req(msg);
        let resp = self.recv_resp()?;

        // assumption, 1-1 comms only for now
        match resp {
            Response::Has(val) => Ok(val),
            _ => Err(Error::UnexpectedResponse),
        }
    }

    fn send_req(&self, req: Message) {
        let sender: &Channel = unsafe { self.sender.as_ref() };
        sender.send(req, Some(self.handle)).unwrap();
    }

    fn recv_resp(&self) -> Result<Response, Error> {
        let receiver: &Channel = unsafe { self.receiver.as_ref() };
        let msg = receiver.recv().unwrap();
        Response::decode(msg)
    }

    fn create_has_request(&self, c: &Cid) -> Message {
        // TODO: use other slots

        let slot: &mut Box<[u8]> = unsafe { &mut *self.sender_buffers[0].get() };
        slot.as_mut()[0] = Request::Has as u8;
        // TODO: Use write_bytes when can get the length
        let bytes = c.to_bytes();
        let len = bytes.len();
        slot.as_mut()
            .get_mut(1..len + 1)
            .expect("message too large")
            .copy_from_slice(&bytes);

        (slot.as_mut_ptr().cast(), (len + 1) as _)
    }
}

#[non_exhaustive]
#[repr(u8)]
enum Request {
    Error = 0,
    Has = 1,
}

#[non_exhaustive]
enum Response {
    Has(bool),
}

impl Response {
    pub fn decode(msg: Message) -> Result<Self, Error> {
        let bytes = unsafe { std::slice::from_raw_parts(msg.0, msg.1.try_into().unwrap()) };

        match bytes[0] {
            0 => {
                if bytes.len() != 1 + 4 {
                    return Err(Error::InvalidResponse);
                }
                Err(Error::from_bytes(&bytes[1..5]).unwrap_or(Error::InvalidResponse))
            }
            1 => {
                if bytes.len() != 2 {
                    return Err(Error::InvalidResponse);
                }
                Ok(Response::Has(bytes[1] == 1))
            }
            _ => Err(Error::InvalidResponse),
        }
    }
}

#[derive(Debug, Copy, Clone, PartialEq)]
#[non_exhaustive]
#[repr(i32)]
pub enum Error {
    NotFound = -1,
    UnexpectedResponse = -2,
    InvalidResponse = -3,
    Other = -4,
}

impl Error {
    pub fn from_bytes(bytes: &[u8]) -> Option<Self> {
        if bytes.len() != 4 {
            return None;
        }

        let bytes: [u8; 4] = bytes.try_into().unwrap();
        let n = i32::from_le_bytes(bytes);
        match n {
            -1 => Some(Error::NotFound),
            -2 => Some(Error::UnexpectedResponse),
            -3 => Some(Error::InvalidResponse),
            -4 => Some(Error::Other),
            _ => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use cid::multihash::{Code, MultihashDigest};
    use cid::Cid;

    #[test]
    fn test_create_memory() {
        let _store = Blockstore::new_memory();
    }

    #[test]
    fn test_has() {
        let bs = Blockstore::new_memory();
        for i in 0..100 {
            let block = format!("thing_{}", i);
            let key = Cid::new_v1(0x55, Code::Sha2_256.digest(block.as_bytes()));
            assert!(!bs.has(&key).unwrap())
        }
    }

    #[test]
    fn test_has_cgo() {
        let bs = Blockstore::new_memory();
        for i in 0..100 {
            let block = format!("thing_{}", i);
            let key = Cid::new_v1(0x55, Code::Sha2_256.digest(block.as_bytes()));
            assert!(!bs.has_cgo(&key).unwrap())
        }
    }
}
