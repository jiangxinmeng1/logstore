package store

import (
	"github.com/jiangxinmeng1/logstore/pkg/common"
	"github.com/jiangxinmeng1/logstore/pkg/entry"
	"io"
	"sync"
)

type StoreCfg struct {
	RotateChecker  RotateChecker
	HistoryFactory HistoryFactory
}

type RotateChecker interface {
	PrepareAppend(VFile, int) (bool, error)
}

type VFile interface {
	sync.Locker
	RLock()
	RUnlock()
	SizeLocked() int
	Destroy() error
	Id() int
	Name() string
	String() string
	InCheckpoint(map[uint32]*common.ClosedInterval) bool
	InCommits(map[uint32]*common.ClosedInterval) bool
	InTxnCommits(map[uint32]map[uint64]uint64, map[uint32]*common.ClosedInterval) bool
	MergeCheckpoint(*map[uint32]*common.ClosedInterval)
	MergeTidCidMap(map[uint32]map[uint64]uint64)
	Replay(ReplayHandle, ReplayObserver) error
}

type FileAppender interface {
	Prepare(int, interface{}) error
	Write([]byte) (int, error)
	Commit() error
	Rollback()
	Sync() error
	Revert()
}

type FileReader interface {
	// io.Reader
	// ReadAt([]byte, FileAppender) (int, error)
}

type ReplayObserver interface {
	OnNewEntry(int)
	OnNewCommit(*entry.CommitInfo)
	OnNewCheckpoint(*entry.CheckpointInfo)
	OnNewTxn(*entry.TxnInfo)
	OnNewUncommit(addrs []*VFileAddress)
}

type ReplayHandle = func(VFile, ReplayObserver) error

type History interface {
	String() string
	Append(VFile)
	Extend(...VFile)
	Entries() int
	EntryIds() []int
	GetEntry(int) VFile
	DropEntry(int) (VFile, error)
	OldestEntry() VFile
	Empty() bool
	Replay(ReplayHandle, ReplayObserver) error
	TryTruncate() error
}

type ApplyHandle = func(group uint32, commitId uint64, payload []byte, typ uint16, info interface{}) (err error)

type File interface {
	io.Closer
	sync.Locker
	RLock()
	RUnlock()
	FileReader

	Sync() error
	GetAppender() FileAppender
	Replay(ReplayHandle, ReplayObserver) error
	GetHistory() History
	TryTruncate(int64) error
}

type Store interface {
	io.Closer
	Sync() error
	Replay(ApplyHandle) error
	GetCheckpointed(uint32) uint64
	GetSynced(uint32) uint64
	AppendEntry(entry.Entry) error
	TryCompact() error
	TryTruncate(int64) error
}
