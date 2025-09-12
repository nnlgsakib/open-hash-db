package dial

import (
    "container/heap"
    "context"
    "sync"

    "github.com/libp2p/go-libp2p/core/peer"
)

type DialPriority uint64

const (
    PriorityRequestedDial DialPriority = 1
    PriorityRandomDial    DialPriority = 10
)

// DialQueue is a min-heap based priority queue of dial tasks.
type DialQueue struct {
    sync.Mutex

    heap  dialQueueImpl
    tasks map[peer.ID]*DialTask

    updateCh chan struct{}
    closeCh  chan struct{}
}

func NewDialQueue() *DialQueue {
    return &DialQueue{
        heap:     dialQueueImpl{},
        tasks:    make(map[peer.ID]*DialTask),
        updateCh: make(chan struct{}, 1),
        closeCh:  make(chan struct{}),
    }
}

func (d *DialQueue) Close() { close(d.closeCh) }

// Wait blocks until update or close; returns true if closed / ctx done.
func (d *DialQueue) Wait(ctx context.Context) bool {
    select {
    case <-ctx.Done():
        return true
    case <-d.updateCh:
        return false
    case <-d.closeCh:
        return true
    }
}

func (d *DialQueue) PopTask() *DialTask {
    d.Lock()
    defer d.Unlock()
    if len(d.heap) == 0 {
        return nil
    }
    t, ok := heap.Pop(&d.heap).(*DialTask)
    if !ok {
        return nil
    }
    delete(d.tasks, t.addrInfo.ID)
    return t
}

func (d *DialQueue) DeleteTask(id peer.ID) {
    d.Lock()
    defer d.Unlock()
    if it, ok := d.tasks[id]; ok {
        heap.Remove(&d.heap, it.index)
        delete(d.tasks, id)
    }
}

func (d *DialQueue) AddTask(info *peer.AddrInfo, prio DialPriority) {
    if d.addTaskImpl(info, prio) {
        select { case d.updateCh <- struct{}{}: default: }
    }
}

func (d *DialQueue) addTaskImpl(info *peer.AddrInfo, prio DialPriority) bool {
    d.Lock()
    defer d.Unlock()
    if it, ok := d.tasks[info.ID]; ok {
        if it.priority > uint64(prio) {
            it.addrInfo = info
            it.priority = uint64(prio)
            heap.Fix(&d.heap, it.index)
            return true
        }
        return false
    }
    it := &DialTask{addrInfo: info, priority: uint64(prio)}
    d.tasks[info.ID] = it
    heap.Push(&d.heap, it)
    return true
}

