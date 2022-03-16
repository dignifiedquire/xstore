package main

import (
	"unsafe"

	"github.com/filecoin-project/lotus/blockstore"
	blocks "github.com/ipfs/go-block-format"
	"github.com/ipfs/go-cid"
)

/*
#include <stdint.h>
#include <stdarg.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdlib.h>

typedef struct Token {
  struct Slot *slot;
  uint64_t stamp;
} Token;

typedef struct Slot {
  uint64_t stamp;
  uint8_t *msg_ptr;
  uint64_t msg_len;

} Slot;

typedef struct Channel {
  uint64_t head;
  uint64_t tail;
  struct Slot *buffer;
  uintptr_t cap;
  uint64_t one_lap;
  uint64_t mark_bit;
} Channel;

typedef const uint8_t* buf_t;
*/
import "C"

import "reflect"

func toCid(k C.buf_t, k_len C.int32_t) cid.Cid {
	s := &struct{ str string }{str: StringN(k, k_len)}
	return *(*cid.Cid)(unsafe.Pointer(s))
}

type RawChannel = C.struct_Channel

//export xstore_new_memory
func xstore_new_memory(sender *RawChannel, receiver *RawChannel) C.int32_t {
	bs := blockstore.NewMemory()
	return C.int32_t(Register(bs, sender, receiver))
}

//export xstore_get
func xstore_get(store C.int32_t, k C.buf_t, k_len C.int32_t, block **C.uint8_t, size *C.int32_t) C.int32_t {
	c := toCid(k, k_len)
	bs := Lookup(int32(store))
	if bs == nil {
		return ErrNoStore
	}
	err := bs.Store.View(c, func(data []byte) error {
		*block = (C.buf_t)(C.CBytes(data))
		*size = C.int32_t(len(data))
		return nil
	})

	switch err {
	case nil:
		return 0
	case blockstore.ErrNotFound:
		return ErrNotFound
	default:
		return ErrIO
	}
}

//export xstore_put
func xstore_put(store C.int32_t, k C.buf_t, k_len C.int32_t, block C.buf_t, block_len C.int32_t) C.int32_t {
	bs := Lookup(int32(store))
	if bs == nil {
		return ErrNoStore
	}

	c := toCid(k, k_len)
	b, _ := blocks.NewBlockWithCid(C.GoBytes(unsafe.Pointer(block), C.int(block_len)), c)

	if bs.Store.Put(b) != nil {
		return ErrIO
	}
	return 0
}

//export xstore_put_many
func xstore_put_many(
	store C.int32_t,
	ks_ptr C.buf_t, ks_len C.int32_t,
	ks_lens_ptr *C.int32_t, ks_lens_len C.int32_t,
	blocks_ptr C.buf_t, blocks_len C.int32_t,
	blocks_lens_ptr *C.int32_t, blocks_lens_len C.int32_t,
) C.int32_t {
	bs := Lookup(int32(store))
	if bs == nil {
		return ErrNoStore
	}

	ks_lens := Int32N((*C.int32_t)(unsafe.Pointer(ks_lens_ptr)), C.int(ks_lens_len))
	blocks_lens := Int32N((*C.int32_t)(unsafe.Pointer(blocks_lens_ptr)), C.int(blocks_lens_len))
	ks_offset := uintptr(0)
	blocks_offset := uintptr(0)
	bss := make([]blocks.Block, len(ks_lens))
	for i := range ks_lens {
		k := uintptr(unsafe.Pointer(ks_ptr)) + ks_offset
		k_len := ks_lens[i]
		c := toCid(C.buf_t(unsafe.Pointer(k)), k_len)
		ks_offset += uintptr(k_len)

		block := uintptr(unsafe.Pointer(blocks_ptr)) + blocks_offset
		block_len := blocks_lens[i]
		b, _ := blocks.NewBlockWithCid(Bytes(C.buf_t(unsafe.Pointer(block)), block_len), c)
		bss[i] = b
		blocks_offset += uintptr(block_len)
	}
	if bs.Store.PutMany(bss) != nil {
		return ErrIO
	}
	return 0
}

//export xstore_delete
func xstore_delete(store C.int32_t, k C.buf_t, k_len C.int32_t) C.int32_t {
	c := toCid(k, k_len)
	bs := Lookup(int32(store))
	if bs == nil {
		return ErrNoStore
	}
	if bs.Store.DeleteBlock(c) != nil {
		return ErrIO
	}
	return 0
}

//export xstore_has
func xstore_has(store C.int32_t, k C.buf_t, k_len C.int32_t) C.int32_t {
	c := toCid(k, k_len)
	bs := Lookup(int32(store))
	if bs == nil {
		return ErrNoStore
	}
	has, err := bs.Store.Has(c)
	switch err {
	case nil:
	case blockstore.ErrNotFound:
		// Some old blockstores still return this.
		return 0
	default:
		return ErrIO
	}
	if has {
		return 1
	}
	return 0
}

func Int32N(ptr *C.int32_t, len C.int32_t) []C.int32_t {
	if ptr == nil {
		return nil
	}

	slice := (*[1 << 30]C.int32_t)(unsafe.Pointer(ptr))[:int(len)]
	sliceHeader := (*reflect.SliceHeader)(unsafe.Pointer(&slice))
	sliceHeader.Cap = int(len)
	return slice
}

func Bytes(ptr C.buf_t, len C.int32_t) []byte {
	if ptr == nil {
		return nil
	}

	slice := (*[1 << 30]byte)(unsafe.Pointer(ptr))[:int(len)]
	sliceHeader := (*reflect.SliceHeader)(unsafe.Pointer(&slice))
	sliceHeader.Cap = int(len)
	return slice
}

func StringN(ptr C.buf_t, len C.int32_t) string {
	bytes := Bytes(ptr, len)
	return string(bytes)
}

// Need an empty main function for building the go-archive
func main() {}
