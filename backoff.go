package main

import "runtime"
import "time"

const spinLimit = 6
const yieldLimit = 10

// Backoff performs exponential backoff in spin loops.
type Backoff struct {
	step uint
}

// New creates a new Backoff.
func NewBackoff() Backoff {
	return Backoff{step: 0}
}

func (b *Backoff) Reset() {
	b.step = 0
}

func (b *Backoff) Spin() {
	for i := 0; i < 1<<Min(b.step, spinLimit); i++ {
		time.Sleep(1 * time.Nanosecond)
	}

	if b.step <= spinLimit {
		b.step++
	}
}

func (b *Backoff) Snooze() {
	if b.step <= spinLimit {
		for i := uint(0); i < 1<<b.step; i++ {
			time.Sleep(1 * time.Nanosecond)
		}
	} else {
		runtime.Gosched()
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
