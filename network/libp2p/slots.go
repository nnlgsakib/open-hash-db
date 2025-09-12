package libp2p

import "context"

// Slots is a simple semaphore for concurrent outbound dials.
type Slots chan struct{}

func NewSlots(max int) Slots {
    if max <= 0 { max = 1 }
    s := make(Slots, max)
    for i := 0; i < max; i++ { s <- struct{}{} }
    return s
}

func (s Slots) Take(ctx context.Context) bool {
    select {
    case <-ctx.Done():
        return true
    case <-s:
        return false
    }
}

func (s Slots) Release() {
    select {
    case s <- struct{}{}:
    default:
    }
}

