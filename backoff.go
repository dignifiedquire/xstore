package main

import "runtime"

// import "time"
import "github.com/dignifiedquire/xstore/asm"

const spinLimit = 6
const yieldLimit = 10

// Backoff performs exponential backoff in spin loops.
type Backoff struct {
	step uint
}

var k int = 0

// New creates a new Backoff.
func NewBackoff() Backoff {
	return Backoff{step: 0}
}

func (b *Backoff) Reset() {
	b.step = 0
}

func spin(i int) {
	// time.Sleep(1 * time.Nanosecond)
	//runtime.Gosched()
	asm.MmPause()
}

func yield() {
	runtime.Gosched()
}

func (b *Backoff) Spin() {
	for i := 0; i < 1<<Min(b.step, spinLimit); i++ {
		spin(i)
	}

	if b.step <= spinLimit {
		b.step++
	}
}

func (b *Backoff) Snooze() {
	if b.step <= spinLimit {
		for i := uint(0); i < 1<<b.step; i++ {
			spin(int(i))
		}
	} else {
		yield()
	}

	if b.step <= yieldLimit {
		b.step++
	}
}

func (b *Backoff) IsCompleted() bool {
	return b.step > yieldLimit
}

func Min(a uint, b uint) uint {
	if a < b {
		return a
	}
	return b
}
