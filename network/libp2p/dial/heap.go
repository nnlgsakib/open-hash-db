package dial

// The DialQueue is implemented as priority queue which utilizes a heap
// Inspired by Go's container/heap example

type dialQueueImpl []*DialTask

func (t dialQueueImpl) Len() int { return len(t) }
func (t dialQueueImpl) Less(i, j int) bool { return t[i].priority < t[j].priority }
func (t dialQueueImpl) Swap(i, j int) {
    t[i], t[j] = t[j], t[i]
    t[i].index = i
    t[j].index = j
}

func (t *dialQueueImpl) Push(x any) {
    n := len(*t)
    item := x.(*DialTask)
    item.index = n
    *t = append(*t, item)
}

func (t *dialQueueImpl) Pop() any {
    old := *t
    n := len(old)
    item := old[n-1]
    old[n-1] = nil
    item.index = -1
    *t = old[:n-1]
    return item
}

