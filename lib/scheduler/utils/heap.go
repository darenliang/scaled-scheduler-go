package utils

import "container/heap"

type WorkerHeapEntry struct {
	WorkerID string
	Tasks    int
	Index    int
}

type WorkerHeap struct {
	Entries         []*WorkerHeapEntry
	WorkerIDToEntry map[string]*WorkerHeapEntry
}

func NewWorkerHeap() *WorkerHeap {
	return &WorkerHeap{
		Entries:         make([]*WorkerHeapEntry, 0),
		WorkerIDToEntry: make(map[string]*WorkerHeapEntry),
	}
}

func (h WorkerHeap) Len() int {
	return len(h.Entries)
}

func (h WorkerHeap) Less(i, j int) bool {
	return h.Entries[i].Tasks < h.Entries[j].Tasks
}

func (h WorkerHeap) Swap(i, j int) {
	h.Entries[i], h.Entries[j] = h.Entries[j], h.Entries[i]
	h.Entries[i].Index = i
	h.Entries[j].Index = j
}

func (h *WorkerHeap) Push(x any) {
	n := len(h.Entries)
	entry := x.(*WorkerHeapEntry)
	if _, ok := h.WorkerIDToEntry[entry.WorkerID]; ok {
		panic("worker already exists")
	}
	entry.Index = n
	h.Entries = append(h.Entries, entry)
	h.WorkerIDToEntry[entry.WorkerID] = entry
}

func (h *WorkerHeap) Pop() any {
	old := h.Entries
	n := len(old)
	entry := old[n-1]
	old[n-1] = nil
	entry.Index = -1
	h.Entries = old[0 : n-1]
	delete(h.WorkerIDToEntry, entry.WorkerID)
	return entry
}

func (h *WorkerHeap) Peek() *WorkerHeapEntry {
	return h.Entries[0]
}

func (h *WorkerHeap) AddTasks(workerID string, count int) int {
	entry, ok := h.WorkerIDToEntry[workerID]
	if !ok {
		panic("worker does not exist")
	}
	entry.Tasks += count
	heap.Fix(h, entry.Index)
	return entry.Tasks
}
