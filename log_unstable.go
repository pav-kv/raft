// Copyright 2015 The etcd Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package raft

import (
	"fmt"

	pb "go.etcd.io/raft/v3/raftpb"
)

// unstable contains "unstable" log entries and snapshot state that has
// not yet been written to Storage. The type serves two roles. First, it
// holds on to new log entries and an optional snapshot until they are
// handed to a Ready struct for persistence. Second, it continues to
// hold on to this state after it has been handed off to provide raftLog
// with a view of the in-progress log entries and snapshot until their
// writes have been stabilized and are guaranteed to be reflected in
// queries of Storage. After this point, the corresponding log entries
// and/or snapshot can be cleared from unstable.
//
// unstable.entries[i] has raft log position i+unstable.offset.
// Note that unstable.offset may be less than the highest log
// position in storage; this means that the next write to storage
// might need to truncate the log before persisting unstable.entries.
type unstable struct {
	// the incoming unstable snapshot, if any.
	snapshot *pb.Snapshot
	// all entries that have not yet been written to storage.
	entries []pb.Entry
	// entries[i] has raft log position i+offset.
	offset uint64

	// if true, snapshot is being written to storage.
	snapshotInProgress bool
	// entries[:offsetInProgress-offset] are being written to storage.
	// Like offset, offsetInProgress is exclusive, meaning that it
	// contains the index following the largest in-progress entry.
	// Invariant: offset <= offsetInProgress
	offsetInProgress uint64

	logger Logger
}

// maybeFirstIndex returns the index of the first possible entry in entries
// if it has a snapshot.
func (u *unstable) maybeFirstIndex() (uint64, bool) {
	if u.snapshot != nil {
		return u.snapshot.Metadata.Index + 1, true
	}
	return 0, false
}

// maybeLastIndex returns the last index if it has at least one
// unstable entry or snapshot.
func (u *unstable) maybeLastIndex() (uint64, bool) {
	if l := len(u.entries); l != 0 {
		return u.offset + uint64(l) - 1, true
	}
	if u.snapshot != nil {
		return u.snapshot.Metadata.Index, true
	}
	return 0, false
}

// maybeTerm returns the term of the entry at index i, if there
// is any.
func (u *unstable) maybeTerm(i uint64) (uint64, bool) {
	if i < u.offset {
		if u.snapshot != nil && u.snapshot.Metadata.Index == i {
			return u.snapshot.Metadata.Term, true
		}
		return 0, false
	}

	last, ok := u.maybeLastIndex()
	if !ok {
		return 0, false
	}
	if i > last {
		return 0, false
	}

	return u.entries[i-u.offset].Term, true
}

// nextEntries returns the unstable entries that are not already in the process
// of being written to storage.
func (u *unstable) nextEntries() []pb.Entry {
	inProgress := int(u.offsetInProgress - u.offset)
	if len(u.entries) == inProgress {
		return nil
	}
	return u.entries[inProgress:]
}

// nextSnapshot returns the unstable snapshot, if one exists that is not already
// in the process of being written to storage.
func (u *unstable) nextSnapshot() *pb.Snapshot {
	if u.snapshot == nil || u.snapshotInProgress {
		return nil
	}
	return u.snapshot
}

// acceptInProgress marks all entries and the snapshot, if any, in the unstable
// as having begun the process of being written to storage. The entries/snapshot
// will no longer be returned from nextEntries/nextSnapshot. However, new
// entries/snapshots added after a call to acceptInProgress will be returned
// from those methods, until the next call to acceptInProgress.
func (u *unstable) acceptInProgress() {
	if len(u.entries) > 0 {
		// NOTE: +1 because offsetInProgress is exclusive, like offset.
		u.offsetInProgress = u.entries[len(u.entries)-1].Index + 1
	}
	if u.snapshot != nil {
		u.snapshotInProgress = true
	}
}

// stableTo marks entries up to the entry with the specified (index, term) as
// being successfully written to stable storage.
//
// The method should only be called when the caller can attest that the entries
// can not be overwritten by an in-progress log append. See the related comment
// in newStorageAppendRespMsg.
func (u *unstable) stableTo(i, t uint64) {
	gt, ok := u.maybeTerm(i)
	if !ok {
		// Unstable entry missing. Ignore.
		u.logger.Infof("entry at index %d missing from unstable log; ignoring", i)
		return
	}
	if i < u.offset {
		// Index matched unstable snapshot, not unstable entry. Ignore.
		u.logger.Infof("entry at index %d matched unstable snapshot; ignoring", i)
		return
	}
	if gt != t {
		// Term mismatch between unstable entry and specified entry. Ignore.
		// This is possible if part or all of the unstable log was replaced
		// between that time that a set of entries started to be written to
		// stable storage and when they finished.
		u.logger.Infof("entry at (index,term)=(%d,%d) mismatched with "+
			"entry at (%d,%d) in unstable log; ignoring", i, t, i, gt)
		return
	}
	num := int(i + 1 - u.offset)
	u.entries = u.entries[num:]
	u.offset = i + 1
	u.offsetInProgress = max(u.offsetInProgress, u.offset)
	u.shrinkEntriesArray()
}

// shrinkEntriesArray discards the underlying array used by the entries slice
// if most of it isn't being used. This avoids holding references to a bunch of
// potentially large entries that aren't needed anymore. Simply clearing the
// entries wouldn't be safe because clients might still be using them.
func (u *unstable) shrinkEntriesArray() {
	// We replace the array if we're using less than half of the space in
	// it. This number is fairly arbitrary, chosen as an attempt to balance
	// memory usage vs number of allocations. It could probably be improved
	// with some focused tuning.
	const lenMultiple = 2
	if len(u.entries) == 0 {
		u.entries = nil
	} else if len(u.entries)*lenMultiple < cap(u.entries) {
		newEntries := make([]pb.Entry, len(u.entries))
		copy(newEntries, u.entries)
		u.entries = newEntries
	}
}

func (u *unstable) stableSnapTo(i uint64) {
	if u.snapshot != nil && u.snapshot.Metadata.Index == i {
		u.snapshot = nil
		u.snapshotInProgress = false
	}
}

func (u *unstable) restore(s pb.Snapshot) {
	u.offset = s.Metadata.Index + 1
	u.offsetInProgress = u.offset
	u.entries = nil
	u.snapshot = &s
	u.snapshotInProgress = false
}

func (u *unstable) truncateAndAppend(ents []pb.Entry) {
	fromIndex := ents[0].Index
	switch {
	case fromIndex == u.offset+uint64(len(u.entries)):
		// fromIndex is the next index in the u.entries, so append directly.
		u.entries = append(u.entries, ents...)
	case fromIndex <= u.offset:
		u.logger.Infof("replace the unstable entries from index %d", fromIndex)
		// The log is being truncated to before our current offset
		// portion, so set the offset and replace the entries.
		u.entries = ents
		u.offset = fromIndex
		u.offsetInProgress = u.offset
	default:
		// Truncate to fromIndex (exclusive), and append the new entries.
		u.logger.Infof("truncate the unstable entries before index %d", fromIndex)
		keep := u.slice(u.offset, fromIndex) // NB: appending to this slice is safe,
		u.entries = append(keep, ents...)    // and will reallocate/copy it
		// Only in-progress entries before fromIndex are still considered to be
		// in-progress.
		u.offsetInProgress = min(u.offsetInProgress, fromIndex)
	}
}

// slice returns the entries from the unstable log with indexes in the range
// [lo, hi). The entire range must be stored in the unstable log or the method
// will panic. The returned slice can be appended to, but the entries in it must
// not be changed because they are still shared with unstable.
//
// TODO(pavelkalinnikov): this, and similar []pb.Entry slices, may bubble up all
// the way to the application code through Ready struct. Protect other slices
// similarly, and document how the client can use them.
func (u *unstable) slice(lo uint64, hi uint64) []pb.Entry {
	u.mustCheckOutOfBounds(lo, hi)
	// NB: use the full slice expression to limit what the caller can do with the
	// returned slice. For example, an append will reallocate and copy this slice
	// instead of corrupting the neighbouring u.entries.
	return u.entries[lo-u.offset : hi-u.offset : hi-u.offset]
}

// u.offset <= lo <= hi <= u.offset+len(u.entries)
func (u *unstable) mustCheckOutOfBounds(lo, hi uint64) {
	if lo > hi {
		u.logger.Panicf("invalid unstable.slice %d > %d", lo, hi)
	}
	upper := u.offset + uint64(len(u.entries))
	if lo < u.offset || hi > upper {
		u.logger.Panicf("unstable.slice[%d,%d) out of bound [%d,%d]", lo, hi, u.offset, upper)
	}
}

type appendMark struct {
	term  uint64
	index uint64
}

// appendTracker tracks the set of in-flight asynchronous append messages. The
// messages may have been sent under different leader terms. Later terms may
// backtrack the log index, thus truncating overriding the log starting from
// this position.
//
//	unstable: (-----------------------]
//	          .   .   .       .       .
//	t9: |     .   .   .       (-------]
//	t7: |     .   .   (---]-------]
//	t4: |     .   (---]
//	t3: |     (-----------------]
//	    +--------------------------------> index
//	         10  20  30  40  50  60  70
//
// We track the "frontier" of the appends that may take effect, i.e. all appends
// which still have at least one entry in the unstable structure. In the example
// picture above, if there is a new append at (term 10, index 30), it overrides
// the entire stream at term 7 and 9, and removes them from the tracker. Even if
// storage ends up writing them, all of these entries are already erased from
// unstable, and are to be overwritten.
//
// TODO(during review): explain better.
type appendTracker struct {
	// marks contains the "frontier" of the in-flight append messages. For every
	// leadership term that has sent at least one entry that is still in unstable,
	// there is a mark in this slice.
	//
	// INVARIANT: len(marks) > 0
	// INVARIANT: marks[i+1].term > marks[i].term
	// INVARIANT: marks[i+1].index > marks[i].index
	//
	// Invariants make it possible to perform the add() and ack() operations in
	// amortized O(1) time.
	//
	// Most of the time, len(marks) <= 2. It has a single entry during normal
	// operation, and 2 entries when leadership changes. More than 2 entries may
	// indicate that the storage appends handling is slow, the in-flight buffer is
	// large, or that leadership changes are too frequent.
	//
	// TODO(pavelkalinnikov): use a small circular buffer to avoid most of the
	// slice allocations.
	//
	// TODO(pavelkalinnikov): integrate with the commit index. We can't append at
	// index <= commit, so it would be convenient to track here too.
	//
	// TODO(pavelkalinnikov): integrate with snapshots. A snapshot is a specific
	// "append" that doesn't have to be contiguous with the previous appends, i.e.
	// it's just a "jump" in the index.
	marks []appendMark
}

func newAppendTracker() appendTracker {
	// Start from a single "sentinel"  mark.
	return appendTracker{marks: make([]appendMark, 1)}
}

// add registers the given append mark with the tracker. The mark must not be
// less, in the (term, index) tuple comparison sense, than of all previously
// added and acked marks.
func (a *appendTracker) add(mark appendMark) error {
	head := len(a.marks)
	if h := a.marks[head-1]; mark.term < h.term || mark.term == h.term && mark.index < h.index {
		return fmt.Errorf("adding mark %+v out of order: already at %+v", mark, h)
	}
	for ; head > 0 && mark.index <= a.marks[head-1].index; head-- {
	}
	a.marks = append(a.marks[:head], mark)
	return nil
}

// ack declares that all appends up to the given mark have been completed. If
// this mark is below previously acked marks, in the (term, index) tuple
// comparison sense, it is ignored.
func (a *appendTracker) ack(mark appendMark) {
	tail := 0
	for ; tail < len(a.marks) && a.marks[tail].term < mark.term; tail++ {
	}
	a.marks = a.marks[tail:]
	if len(a.marks) == 0 {
		a.marks = append(a.marks, mark)
	} else if t := a.marks[0]; mark.term == t.term && mark.index > t.index {
		a.marks[0].index = mark.index
	}
}

// mark returns the in-flight mark.
func (a appendTracker) mark() appendMark {
	return a.marks[0]
}
