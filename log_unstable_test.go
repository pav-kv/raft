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
	"testing"

	"github.com/stretchr/testify/require"

	pb "go.etcd.io/raft/v3/raftpb"
)

func TestUnstableMaybeFirstIndex(t *testing.T) {
	tests := []struct {
		entries []pb.Entry
		offset  uint64
		snap    *pb.Snapshot

		wok    bool
		windex uint64
	}{
		// no snapshot
		{
			index(5).terms(1), 5, nil,
			false, 0,
		},
		{
			[]pb.Entry{}, 0, nil,
			false, 0,
		},
		// has snapshot
		{
			index(5).terms(1), 5, &pb.Snapshot{Metadata: pb.SnapshotMetadata{Index: 4, Term: 1}},
			true, 5,
		},
		{
			[]pb.Entry{}, 5, &pb.Snapshot{Metadata: pb.SnapshotMetadata{Index: 4, Term: 1}},
			true, 5,
		},
	}

	for i, tt := range tests {
		t.Run(fmt.Sprint(i), func(t *testing.T) {
			u := unstable{
				entries: tt.entries,
				// FIXME: prev
				snapshot: tt.snap,
				logger:   raftLogger,
			}
			index, ok := u.maybeFirstIndex()
			require.Equal(t, tt.wok, ok)
			require.Equal(t, tt.windex, index)
		})
	}
}

func TestMaybeLastIndex(t *testing.T) {
	tests := []struct {
		entries []pb.Entry
		offset  uint64
		snap    *pb.Snapshot

		wok    bool
		windex uint64
	}{
		// last in entries
		{
			index(5).terms(1), 5, nil,
			true, 5,
		},
		{
			index(5).terms(1), 5, &pb.Snapshot{Metadata: pb.SnapshotMetadata{Index: 4, Term: 1}},
			true, 5,
		},
		// last in snapshot
		{
			[]pb.Entry{}, 5, &pb.Snapshot{Metadata: pb.SnapshotMetadata{Index: 4, Term: 1}},
			true, 4,
		},
		// empty unstable
		{
			[]pb.Entry{}, 0, nil,
			false, 0,
		},
	}

	for i, tt := range tests {
		t.Run(fmt.Sprint(i), func(t *testing.T) {
			u := unstable{
				entries: tt.entries,
				// FIXME: prev
				snapshot: tt.snap,
				logger:   raftLogger,
			}
			index, ok := u.maybeLastIndex()
			require.Equal(t, tt.wok, ok)
			require.Equal(t, tt.windex, index)
		})
	}
}

func TestUnstableMaybeTerm(t *testing.T) {
	tests := []struct {
		entries []pb.Entry
		offset  uint64
		snap    *pb.Snapshot
		index   uint64

		wok   bool
		wterm uint64
	}{
		// term from entries
		{
			index(5).terms(1), 5, nil,
			5,
			true, 1,
		},
		{
			index(5).terms(1), 5, nil,
			6,
			false, 0,
		},
		{
			index(5).terms(1), 5, nil,
			4,
			false, 0,
		},
		{
			index(5).terms(1), 5, &pb.Snapshot{Metadata: pb.SnapshotMetadata{Index: 4, Term: 1}},
			5,
			true, 1,
		},
		{
			index(5).terms(1), 5, &pb.Snapshot{Metadata: pb.SnapshotMetadata{Index: 4, Term: 1}},
			6,
			false, 0,
		},
		// term from snapshot
		{
			index(5).terms(1), 5, &pb.Snapshot{Metadata: pb.SnapshotMetadata{Index: 4, Term: 1}},
			4,
			true, 1,
		},
		{
			index(5).terms(1), 5, &pb.Snapshot{Metadata: pb.SnapshotMetadata{Index: 4, Term: 1}},
			3,
			false, 0,
		},
		{
			[]pb.Entry{}, 5, &pb.Snapshot{Metadata: pb.SnapshotMetadata{Index: 4, Term: 1}},
			5,
			false, 0,
		},
		{
			[]pb.Entry{}, 5, &pb.Snapshot{Metadata: pb.SnapshotMetadata{Index: 4, Term: 1}},
			4,
			true, 1,
		},
		{
			[]pb.Entry{}, 0, nil,
			5,
			false, 0,
		},
	}

	for i, tt := range tests {
		t.Run(fmt.Sprint(i), func(t *testing.T) {
			u := unstable{
				entries: tt.entries,
				// FIXME: prev
				snapshot: tt.snap,
				logger:   raftLogger,
			}
			term, ok := u.maybeTerm(tt.index)
			require.Equal(t, tt.wok, ok)
			require.Equal(t, tt.wterm, term)
		})
	}
}

func TestUnstableRestore(t *testing.T) {
	u := unstable{
		prev:               entryID{term: 1, index: 4},
		entries:            index(5).terms(1),
		inProgress:         1,
		snapshot:           &pb.Snapshot{Metadata: pb.SnapshotMetadata{Index: 4, Term: 1}},
		snapshotInProgress: true,
		logger:             raftLogger,
	}
	s := pb.Snapshot{Metadata: pb.SnapshotMetadata{Index: 6, Term: 2}}
	u.restore(s)

	require.Equal(t, entryID{term: s.Metadata.Term, index: s.Metadata.Index}, u.prev)
	require.Zero(t, u.inProgress)
	require.Zero(t, len(u.entries))
	require.Equal(t, &s, u.snapshot)
	require.False(t, u.snapshotInProgress)
}

func TestUnstableNextEntries(t *testing.T) {
	prev := entryID{term: 1, index: 4}
	for _, tt := range []struct {
		entries    []pb.Entry
		inProgress int
		want       []pb.Entry
	}{
		// nothing in progress
		{
			entries: index(5).terms(1, 1),
			want:    index(5).terms(1, 1),
		},
		// partially in progress
		{
			entries:    index(5).terms(1, 1),
			inProgress: 1,
			want:       index(6).terms(1),
		},
		// everything in progress
		{
			entries:    index(5).terms(1, 1),
			inProgress: 1,
			want:       nil, // nil, not empty slice
		},
	} {
		t.Run("", func(t *testing.T) {
			u := unstable{
				prev:       prev,
				entries:    tt.entries,
				inProgress: tt.inProgress,
				logger:     discardLogger,
			}
			require.Equal(t, tt.want, u.nextEntries())
		})

	}
}

func TestUnstableNextSnapshot(t *testing.T) {
	s := &pb.Snapshot{Metadata: pb.SnapshotMetadata{Index: 4, Term: 1}}
	tests := []struct {
		snapshot           *pb.Snapshot
		snapshotInProgress bool

		wsnapshot *pb.Snapshot
	}{
		// snapshot not unstable
		{
			nil, false,
			nil,
		},
		// snapshot not in progress
		{
			s, false,
			s,
		},
		// snapshot in progress
		{
			s, true,
			nil,
		},
	}

	for i, tt := range tests {
		t.Run(fmt.Sprint(i), func(t *testing.T) {
			u := unstable{
				snapshot:           tt.snapshot,
				snapshotInProgress: tt.snapshotInProgress,
			}
			res := u.nextSnapshot()
			require.Equal(t, tt.wsnapshot, res)
		})
	}
}

func TestUnstableAcceptInProgress(t *testing.T) {
	prev := entryID{term: 1, index: 4}
	snap := pb.Snapshot{Metadata: pb.SnapshotMetadata{Index: 4, Term: 1}}
	for _, tt := range []struct {
		entries            []pb.Entry
		snapshot           *pb.Snapshot
		inProgress         int
		snapshotInProgress bool

		wInProgress         uint64
		wsnapshotInProgress bool
	}{
		{
			[]pb.Entry{}, nil,
			0,     // no entries
			false, // snapshot not already in progress
			5, false,
		},
		{
			index(5).terms(1), nil,
			0,     // entries not in progress
			false, // snapshot not already in progress
			1, false,
		},
		{
			index(5).terms(1, 1), nil,
			0,     // entries not in progress
			false, // snapshot not already in progress
			2, false,
		},
		{
			index(5).terms(1, 1), nil,
			1,     // in-progress to the first entry
			false, // snapshot not already in progress
			2, false,
		},
		{
			index(5).terms(1, 1), nil,
			2,     // in-progress to the second entry
			false, // snapshot not already in progress
			2, false,
		},
		// with snapshot
		{
			[]pb.Entry{}, &snap,
			0,     // no entries
			false, // snapshot not already in progress
			0, true,
		},
		{
			index(5).terms(1), &snap,
			0,     // entries not in progress
			false, // snapshot not already in progress
			1, true,
		},
		{
			index(5).terms(1, 1), &snap,
			0,     // entries not in progress
			false, // snapshot not already in progress
			2, true,
		},
		{
			index(5).terms(1, 1), &snap,
			1,     // in-progress to the first entry
			false, // snapshot not already in progress
			2, true,
		},
		{
			index(5).terms(1, 1), &snap,
			2,     // in-progress to the second entry
			false, // snapshot not already in progress
			2, true,
		},
		{
			[]pb.Entry{}, &snap,
			0,    // entries not in progress
			true, // snapshot already in progress
			0, true,
		},
		{
			index(5).terms(1), &snap,
			0,    // entries not in progress
			true, // snapshot already in progress
			1, true,
		},
		{
			index(5).terms(1, 1), &snap,
			0,    // entries not in progress
			true, // snapshot already in progress
			2, true,
		},
		{
			index(5).terms(1, 1), &snap,
			1,    // in-progress to the first entry
			true, // snapshot already in progress
			2, true,
		},
		{
			index(5).terms(1, 1), &snap,
			0,    // in-progress to the second entry
			true, // snapshot already in progress
			0, true,
		},
	} {
		t.Run("", func(t *testing.T) {
			u := unstable{
				prev:               prev,
				entries:            tt.entries,
				snapshot:           tt.snapshot,
				inProgress:         tt.inProgress,
				snapshotInProgress: tt.snapshotInProgress,
			}
			u.acceptInProgress()
			require.Equal(t, tt.inProgress, u.inProgress)
			require.Equal(t, tt.wsnapshotInProgress, u.snapshotInProgress)
		})
	}
}

func TestUnstableStableTo(t *testing.T) {
	for _, tt := range []struct {
		entries          []pb.Entry
		offset           uint64
		offsetInProgress uint64
		snap             *pb.Snapshot
		index, term      uint64

		woffset           uint64
		woffsetInProgress uint64
		wlen              int
	}{
		{
			[]pb.Entry{}, 0, 0, nil,
			5, 1,
			0, 0, 0,
		},
		{
			index(5).terms(1), 5, 6, nil,
			5, 1, // stable to the first entry
			6, 6, 0,
		},
		{
			index(5).terms(1, 1), 5, 6, nil,
			5, 1, // stable to the first entry
			6, 6, 1,
		},
		{
			index(5).terms(1, 1), 5, 7, nil,
			5, 1, // stable to the first entry and in-progress ahead
			6, 7, 1,
		},
		{
			index(6).terms(2), 6, 7, nil,
			6, 1, // stable to the first entry and term mismatch
			6, 7, 1,
		},
		{
			index(5).terms(1), 5, 6, nil,
			4, 1, // stable to old entry
			5, 6, 1,
		},
		{
			index(5).terms(1), 5, 6, nil,
			4, 2, // stable to old entry
			5, 6, 1,
		},
		// with snapshot
		{
			index(5).terms(1), 5, 6, &pb.Snapshot{Metadata: pb.SnapshotMetadata{Index: 4, Term: 1}},
			5, 1, // stable to the first entry
			6, 6, 0,
		},
		{
			index(5).terms(1, 1), 5, 6, &pb.Snapshot{Metadata: pb.SnapshotMetadata{Index: 4, Term: 1}},
			5, 1, // stable to the first entry
			6, 6, 1,
		},
		{
			index(5).terms(1, 1), 5, 7, &pb.Snapshot{Metadata: pb.SnapshotMetadata{Index: 4, Term: 1}},
			5, 1, // stable to the first entry and in-progress ahead
			6, 7, 1,
		},
		{
			index(6).terms(2), 6, 7, &pb.Snapshot{Metadata: pb.SnapshotMetadata{Index: 5, Term: 1}},
			6, 1, // stable to the first entry and term mismatch
			6, 7, 1,
		},
		{
			index(5).terms(1), 5, 6, &pb.Snapshot{Metadata: pb.SnapshotMetadata{Index: 4, Term: 1}},
			4, 1, // stable to snapshot
			5, 6, 1,
		},
		{
			index(5).terms(2), 5, 6, &pb.Snapshot{Metadata: pb.SnapshotMetadata{Index: 4, Term: 2}},
			4, 1, // stable to old entry
			5, 6, 1,
		},
	} {
		t.Run("", func(t *testing.T) {
			u := unstable{
				entries:          tt.entries,
				offset:           tt.offset,
				offsetInProgress: tt.offsetInProgress,
				snapshot:         tt.snap,
				logger:           raftLogger,
			}
			u.stableTo(entryID{term: tt.term, index: tt.index})
			require.Equal(t, tt.woffset, u.offset)
			require.Equal(t, tt.woffsetInProgress, u.offsetInProgress)
			require.Equal(t, tt.wlen, len(u.entries))
		})
	}
}

func TestUnstableTruncateAndAppend(t *testing.T) {
	for _, tt := range []struct {
		entries          []pb.Entry
		offset           uint64
		offsetInProgress uint64
		snap             *pb.Snapshot

		term     uint64
		prev     entryID
		toappend []pb.Entry
		notOk    bool
		want     []pb.Entry

		woffset           uint64
		woffsetInProgress uint64
	}{
		// append to the end
		{
			entries: index(5).terms(1), offset: 5, offsetInProgress: 5,
			term:     2,
			prev:     entryID{term: 1, index: 5},
			toappend: index(6).terms(1, 1),
			want:     index(5).terms(1, 1, 1),
			woffset:  5, woffsetInProgress: 5,
		},
		{
			entries: index(5).terms(1), offset: 5, offsetInProgress: 6,
			term:     2,
			prev:     entryID{term: 1, index: 5},
			toappend: index(6).terms(1, 1),
			want:     index(5).terms(1, 1, 1),
			woffset:  5, woffsetInProgress: 6,
		},
		// replace the unstable entries
		{
			entries: index(5).terms(1), offset: 5, offsetInProgress: 5,
			term:     2,
			prev:     entryID{term: 1, index: 4},
			toappend: index(5).terms(2, 2),
			want:     index(5).terms(2, 2),
			woffset:  5, woffsetInProgress: 5,
		},
		{
			entries: index(5).terms(1), offset: 5, offsetInProgress: 5,
			term:     2,
			prev:     entryID{term: 1, index: 3},
			toappend: index(4).terms(2, 2, 2),
			want:     index(4).terms(2, 2, 2),
			woffset:  4, woffsetInProgress: 4,
		},
		{
			entries: index(5).terms(1), offset: 5, offsetInProgress: 6,
			term:     2,
			prev:     entryID{term: 1, index: 4},
			toappend: index(5).terms(2, 2),
			want:     index(5).terms(2, 2),
			woffset:  5, woffsetInProgress: 5,
		},
		// truncate the existing entries and append
		{
			entries: index(5).terms(1, 1, 1), offset: 5, offsetInProgress: 5,
			term:     2,
			prev:     entryID{term: 1, index: 5},
			toappend: index(6).terms(2),
			want:     index(5).terms(1, 2),
			woffset:  5, woffsetInProgress: 5,
		},
		{
			entries: index(5).terms(1, 1, 1), offset: 5, offsetInProgress: 5,
			term:     2,
			prev:     entryID{term: 1, index: 6},
			toappend: index(7).terms(2, 2),
			want:     index(5).terms(1, 1, 2, 2),
			woffset:  5, woffsetInProgress: 5,
		},
		{
			entries: index(5).terms(1, 1, 1), offset: 5, offsetInProgress: 6,
			term:     2,
			prev:     entryID{term: 1, index: 5},
			toappend: index(6).terms(2),
			want:     index(5).terms(1, 2),
			woffset:  5, woffsetInProgress: 6,
		},
		{
			entries: index(5).terms(1, 1, 1), offset: 5, offsetInProgress: 7,
			term:     2,
			prev:     entryID{term: 1, index: 5},
			toappend: index(6).terms(2),
			want:     index(5).terms(1, 2),
			woffset:  5, woffsetInProgress: 6,
		},
	} {
		t.Run("", func(t *testing.T) {
			u := unstable{
				entries:          tt.entries,
				offset:           tt.offset,
				offsetInProgress: tt.offsetInProgress,
				snapshot:         tt.snap,
				logger:           discardLogger,
			}
			app := logSlice{term: tt.term, prev: tt.prev, entries: tt.toappend}
			require.NoError(t, app.valid())

			require.Equal(t, !tt.notOk, u.truncateAndAppend(app))
			require.Equal(t, tt.want, u.entries)
			require.Equal(t, tt.woffset, u.offset)
			require.Equal(t, tt.woffsetInProgress, u.offsetInProgress)
		})
	}
}
