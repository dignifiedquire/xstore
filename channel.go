package main

/*
#include "xstore.h"

const uintptr_t slot_size = sizeof(Slot);
*/
import "C"

import (
	"encoding/binary"
	"reflect"
	"sync/atomic"
	"unsafe"

	gs "github.com/dignifiedquire/gsysint"
	"github.com/dignifiedquire/gsysint/g"
	"github.com/dignifiedquire/gsysint/trace"
)

var slotSize uintptr

func init() {
	slotSize = uintptr(C.slot_size)
}

func msg_error(msg *Message, err int32) *Message {
	msg.len = 5
	msgBytes := msg.Bytes()
	msgBytes[0] = 0 // error
	binary.LittleEndian.PutUint32(msgBytes[1:5], uint32(err))

	return msg
}

func make_response(msg *Message, bytes []byte) *Message {
	msg.len = C.uint64_t(len(bytes))
	msgBytes := msg.Bytes()
	copy(msgBytes, bytes)

	return msg
}

type RawChannel = C.struct_Channel

func atomicLoadUint64(val *C.uint64_t) uint64 {
	return atomic.LoadUint64((*uint64)(val))
}

func (c *RawChannel) IsFull() bool {
	tail := atomicLoadUint64(&c.tail)
	head := atomicLoadUint64(&c.head)
	return head+uint64(c.one_lap) == tail & ^uint64(c.mark_bit)
}

// startSend attempts to reserve a slot for sending a message.
func (c *RawChannel) startSend(token *C.struct_Token) bool {
	backoff := NewBackoff()
	tail := atomicLoadUint64(&c.tail)

	for {
		// Check if the channel is disconnected.
		if (tail & uint64(c.mark_bit)) != 0 {
			token.slot = nil
			token.stamp = 0
			return true
		}

		// Deconstruct the tail.
		index := tail & (uint64(c.mark_bit - 1))
		lap := tail & ^(uint64(c.one_lap) - 1)

		// Inspect the corresponding slot.
		offset := uintptr(index) * slotSize
		slotPtr := uintptr(unsafe.Pointer(c.buffer)) + offset
		slot := (*C.struct_Slot)(unsafe.Pointer(slotPtr))
		stamp := atomicLoadUint64(&slot.stamp)

		// If the tail and the stamp match, we may attempt to push.
		if tail == stamp {
			var newTail uint64
			if index+1 < uint64(c.cap) {
				// Same lap, incremented index.
				// Set to `{ lap: lap, mark: 0, index: index + 1 }`.
				newTail = tail + 1
			} else {
				// One lap forward, index wraps around to zero.
				// Set to `{ lap: lap.wrapping_add(1), mark: 0, index: 0 }`.

				newTail = lap + uint64(c.one_lap)
			}

			// Try moving the tail.
			if atomic.CompareAndSwapUint64((*uint64)(&c.tail), tail, newTail) {
				// Prepare the token for the folow-up call to `write`.
				token.slot = slot
				token.stamp = C.uint64_t(tail + 1)
				return true
			}
			tail = atomicLoadUint64(&c.tail)
			backoff.Spin()
		} else if stamp+uint64(c.one_lap) == tail+1 {
			head := atomicLoadUint64(&c.head)

			// If the head lags one lap behind the tail as well..
			if head+uint64(c.one_lap) == tail {
				// .. then the chanenl is full.
				return false
			}
			backoff.Spin()
			tail = atomicLoadUint64(&c.tail)
		} else {
			// Snooze because we need to wait for the stamp to get updated
			backoff.Snooze()
			tail = atomicLoadUint64(&c.tail)
		}
	}
}

// Returns `nil` on success, otherwise the original message
func (c *RawChannel) write(token *C.struct_Token, msg *Message) *Message {
	// If there is no slot, the channel is disconnected.
	if token.slot == nil {
		return msg
	}

	token.slot.msg_ptr = msg.ptr
	token.slot.msg_len = msg.len
	atomic.StoreUint64((*uint64)(&token.slot.stamp), uint64(token.stamp))
	return nil
}

type Token = C.struct_Token

func defaultToken() Token {
	return Token{
		slot:  nil,
		stamp: 0,
	}
}

// return nil on success, message on error
func (c *RawChannel) TrySend(msg *Message) *Message {
	token := defaultToken()
	if c.startSend(&token) {
		return c.write(&token, msg)
	}
	return msg
}

func (c *RawChannel) Send(msg *Message) *Message {
	token := defaultToken()
	backoff := NewBackoff()

	for {
		for {
			if c.startSend(&token) {
				return c.write(&token, msg)
			}
			if backoff.IsCompleted() {
				backoff.Reset()
				break
			} else {
				backoff.Snooze()
			}
		}
	}
}

func (c *RawChannel) IsDisconnected() bool {
	return atomicLoadUint64(&c.mark_bit)&uint64(c.mark_bit) != 0
}

func (c *RawChannel) read(token *Token) *Message {
	if token.slot == nil {
		return nil
	}

	slot := token.slot
	msg := Message{
		ptr: slot.msg_ptr,
		len: slot.msg_len,
	}
	atomic.StoreUint64((*uint64)(&slot.stamp), uint64(token.stamp))

	return &msg
}

func (c *RawChannel) startRecv(token *Token) bool {
	backoff := NewBackoff()
	head := atomicLoadUint64(&c.head)

	for {
		// Deconstruct the head.
		index := head & uint64(c.mark_bit-1)
		lap := head & ^uint64(c.one_lap-1)

		// Inspect the corresponding slot.
		offset := uintptr(index) * slotSize
		slotPtr := uintptr(unsafe.Pointer(c.buffer)) + offset
		slot := (*C.struct_Slot)(unsafe.Pointer(slotPtr))
		stamp := atomicLoadUint64(&slot.stamp)

		// If the stamp is ahead of the head by 1, we may attempt to pop.
		if head+1 == stamp {
			var new uint64
			if index+1 < uint64(c.cap) {
				// Same lap, incremented index.
				// Set to `{ lap: lap, mark: 0, index: index + 1 }`.
				new = head + 1
			} else {
				// One lap forward, index wraps around to zero.
				// Set to `{ lap: lap.wrapping_add(1), mark: 0, index: 0 }`.
				new = lap + uint64(c.one_lap)
			}

			// Try moving the head.
			if atomic.CompareAndSwapUint64((*uint64)(&c.head), head, new) {
				// Prepare the token fo the follow-up call to `read`
				token.slot = slot
				token.stamp = C.uint64_t(head + uint64(c.one_lap))
				return true
			}
			head = atomicLoadUint64(&c.head)
			backoff.Spin()
		} else if stamp == head {
			tail := atomicLoadUint64(&c.tail)

			// If the tail equals the head, that means the channel is empty.
			if tail & ^uint64(c.mark_bit) == head {
				// If the channel is disconnected..
				if tail&uint64(c.mark_bit) != 0 {
					// ..then receive an error.
					token.slot = nil
					token.stamp = 0
					return true
				}
				// Otherwise the receive operation is not ready.
				return false
			}

			backoff.Spin()
			head = atomicLoadUint64(&c.head)
		} else {
			// Snooze because we need to wait for the stamp to get updated.
			backoff.Snooze()
			head = atomicLoadUint64(&c.head)
		}
	}
}

func (c *RawChannel) TryRecv() *Message {
	token := defaultToken()

	if c.startRecv(&token) {
		return c.read(&token)
	}

	return nil
}

func (c *RawChannel) Recv(l *g.Mutex) *Message {
	token := defaultToken()
	backoff := NewBackoff()

	for {
		for {
			if c.startRecv(&token) {
				return c.read(&token)
			}

			if backoff.IsCompleted() {
				backoff.Reset()
				break
			} else {
				backoff.Snooze()
			}
		}

		if l != nil {
			// store pointer in the channel, to this goroutine
			atomic.StorePointer(&c.receiver, g.GetG())
			gs.Lock(l)
			// park
			gs.GoParkUnlock(l, g.WaitReasonZero, trace.TraceEvNone, 1)
			// clear out ourselves
			atomic.StorePointer(&c.receiver, nil)
		}
	}

	return nil
}

// Len returns the current number of messages inside the channel.
func (c *RawChannel) Len() uint64 {
	for {
		// Load the tail, then load the head
		tail := atomicLoadUint64(&c.tail)
		head := atomicLoadUint64(&c.head)

		// If the tail didn't change, we've got consistent values to work with.
		if atomicLoadUint64(&c.tail) == tail {
			hix := head & uint64(c.mark_bit-1)
			tix := tail & uint64(c.mark_bit-1)

			if hix < tix {
				return tix - hix
			}
			if hix > tix {
				return uint64(c.cap) - hix + tix
			}
			if tail & ^uint64(c.mark_bit) == head {
				return 0
			}
			return uint64(c.cap)
		}
	}
}

type Message struct {
	ptr *C.uchar
	len C.uint64_t
}

// func NewMessage(bytes []byte) Message {
// 	l := C.uint64_t(len(bytes))
// 	ptr := C.new_message_bytes((*C.uchar)(unsafe.Pointer(&bytes[0])), l)

// 	return Message {
// 		ptr: ptr,
// 		len: l,
// 	}
// }

// func (msg *Message) Drop() {
// 	C.drop_message_bytes(msg.ptr, msg.len)
// 	msg.ptr = nil
// 	msg.len = 0
// }

func (msg *Message) Len() uint64 {
	return uint64(msg.len)
}

func (msg *Message) Bytes() []byte {
	if msg.ptr == nil {
		return nil
	}

	slice := (*[1 << 30]byte)(unsafe.Pointer(msg.ptr))[:msg.len]
	sliceHeader := (*reflect.SliceHeader)(unsafe.Pointer(&slice))
	sliceHeader.Cap = int(msg.len)
	return slice
}
