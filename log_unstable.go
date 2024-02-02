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

import pb "go.etcd.io/raft/v3/raftpb"

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
	// prev is the ID of the last log entry written to storage, or the ID of the
	// snapshot if present. This entry immediately precedes entries[].
	//
	// TODO(pav-kv): (prev, entries) is now a logical unit equivalent to logAppend
	// struct. Generalize logAppend to be a logSlice, and replace these fields. We
	// need to fully support logAppend.term here first.
	prev entryID
	// all entries that have not yet been written to storage.
	entries []pb.Entry

	// if true, snapshot is being written to storage.
	snapshotInProgress bool
	// entries[:inProgress] are being written to storage.
	// Invariant: inProgress <= len(entries).
	inProgress int

	logger Logger
}

// maybeFirstIndex returns the index of the first possible entry in entries
// if it has a snapshot.
func (u *unstable) maybeFirstIndex() (uint64, bool) {
	if u.snapshot != nil {
		return u.prev.index, true
	}
	return 0, false
}

// maybeLastIndex returns the last index if it has at least one
// unstable entry or snapshot.
//
// TODO(pav-kv): this is a code compatibility adaptor, remove it.
func (u *unstable) maybeLastIndex() (uint64, bool) {
	return u.lastIndex(), true
}

// lastIndex returns the last index in the log.
func (u *unstable) lastIndex() uint64 {
	return u.prev.index + uint64(len(u.entries))
}

// lastEntryID returns the ID of the last entry in the log.
func (u *unstable) lastEntryID() entryID {
	if ln := len(u.entries); ln != 0 {
		return pbEntryID(&u.entries[ln-1])
	}
	return u.prev
}

// maybeTerm returns the term of the entry at index i, if there is any.
func (u *unstable) maybeTerm(i uint64) (uint64, bool) {
	if i < u.prev.index || i > u.lastIndex() {
		return 0, false
	} else if i == u.prev.index {
		return u.prev.term, true
	}
	return u.entries[i-u.prev.index-1].Term, true
}

// nextEntries returns the unstable entries that are not already in the process
// of being written to storage.
func (u *unstable) nextEntries() []pb.Entry {
	if u.inProgress == len(u.entries) {
		return nil
	}
	return u.entries[u.inProgress:]
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
	u.inProgress = len(u.entries)
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
func (u *unstable) stableTo(id entryID) {
	gt, ok := u.maybeTerm(id.index)
	if !ok {
		// Unstable entry missing. Ignore.
		u.logger.Infof("entry at index %d missing from unstable log; ignoring", id.index)
		return
	}
	if id.index <= u.prev.index {
		// Index matched unstable snapshot, not unstable entry. Ignore.
		u.logger.Infof("entry at index %d matched unstable snapshot; ignoring", id.index)
		return
	}
	if gt != id.term {
		// Term mismatch between unstable entry and specified entry. Ignore.
		// This is possible if part or all of the unstable log was replaced
		// between that time that a set of entries started to be written to
		// stable storage and when they finished.
		u.logger.Infof("entry at (index,term)=(%d,%d) mismatched with "+
			"entry at (%d,%d) in unstable log; ignoring", id.index, id.term, id.index, gt)
		return
	}
	num := int(id.index - u.prev.index)
	u.prev = id
	u.entries = u.entries[num:]
	u.inProgress -= min(u.inProgress, num)
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
	u.prev = entryID{term: s.Metadata.Term, index: s.Metadata.Index}
	u.entries = nil
	u.snapshot = &s
	u.inProgress = 0
	u.snapshotInProgress = false
}

func (u *unstable) truncateAndAppend(a logSlice) bool {
	// We can not accept an append from a leader at term below that of the latest
	// accepted entry in our log.
	//
	// TODO(pav-kv): we should have a stricter check that a.term >= the term of
	// the latest accepted append rather than entry.
	if a.term < u.lastEntryID().term {
		return false
	}

	// TODO(pav-kv): handle a.prev.index > u.lastIndex(). Currently, it will never
	// happen here, but this place should handle all cases for ultimate safely.
	switch {
	case a.prev.index == u.lastIndex():
		// The entries immediately follow our slice, so append directly.
		u.entries = append(u.entries, a.entries...)

	case a.prev.index <= u.prev.index:
		// TODO(pav-kv): this branch is only safe if a.term >= the last accepted
		// append term. Add a safety check here, and log error / reject if it does
		// not hold.

		u.logger.Infof("replace the unstable entries after %d", a.prev.index)
		u.prev = a.prev
		u.entries = a.entries
		// All entries are truncated, so none of the new entries are in progress.
		u.inProgress = 0

	default:
		// Truncate to fromIndex (exclusive), and append the new entries.
		u.logger.Infof("truncate the unstable entries after index %d", a.prev.index)
		// NB: appending to the `keep` slice is safe, and will reallocate/copy it.
		// See the slice() method comment.
		keep := u.slice(u.prev.index+1, a.prev.index+1)
		u.entries = append(keep, a.entries...)
		// Only the retained entries remain in-progress.
		u.inProgress = min(u.inProgress, len(keep))
	}
	return true
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
	offset := u.prev.index + 1
	// NB: use the full slice expression to limit what the caller can do with the
	// returned slice. For example, an append will reallocate and copy this slice
	// instead of corrupting the neighbouring u.entries.
	return u.entries[lo-offset : hi-offset : hi-offset]
}

// u.prev.index < lo <= hi <= u.lastIndex()+1
func (u *unstable) mustCheckOutOfBounds(lo, hi uint64) {
	if lo > hi {
		u.logger.Panicf("invalid unstable.slice %d > %d", lo, hi)
	}
	lower := u.prev.index + 1
	upper := lower + uint64(len(u.entries))
	if lo < lower || hi > upper {
		u.logger.Panicf("unstable.slice[%d,%d) out of bound [%d,%d)", lo, hi, lower, upper)
	}
}
