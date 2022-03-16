#ifndef XSTORE_H
#define XSTORE_H

#include <stdint.h>
#include <stdarg.h>
#include <stdbool.h>


   
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

#endif
