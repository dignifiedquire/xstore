#include "textflag.h"

// func MmPause()
TEXT ·MmPause(SB),NOSPLIT,$0
    PAUSE
    RET
