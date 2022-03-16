//! Linked to Go calls.

use crate::channel::Channel;

#[link(name = "xstore", kind = "static")]
extern "C" {
    pub fn xstore_get(
        store: i32,
        k: *const u8,
        k_len: i32,
        block: *mut *mut u8,
        size: *mut i32,
    ) -> i32;
    pub fn xstore_put(
        store: i32,
        k: *const u8,
        k_len: i32,
        block: *const u8,
        block_len: i32,
    ) -> i32;
    pub fn xstore_put_many(
        store: i32,
        ks_bytes: *const u8,
        ks_bytes_len: i32,
        ks_lens: *const i32,
        ks_lens_len: i32,
        blocks: *const u8,
        blocks_len: i32,
        blocks_lens: *const i32,
        blocks_lens_len: i32,
    ) -> i32;
    pub fn xstore_delete(store: i32, k: *const u8, k_len: i32) -> i32;
    pub fn xstore_has(store: i32, k: *const u8, k_len: i32) -> i32;
    pub fn xstore_new_memory(sender: *const Channel, receiver: *const Channel) -> i32;

    pub fn xstore_channel_unpark(store: i32);
}
