package store

import (
	"context"
	"encoding/json"
	"errors"

	// "fmt"
	"github.com/jiangxinmeng1/logstore/pkg/common"
	"github.com/jiangxinmeng1/logstore/pkg/entry"
	"sync"
)

var (
	DefaultMaxBatchSize = 500
)

type baseStore struct {
	syncBase
	common.ClosedState
	dir, name   string
	flushWg     sync.WaitGroup
	flushCtx    context.Context
	flushCancel context.CancelFunc
	flushQueue  chan entry.Entry
	wg          sync.WaitGroup
	file        File
	mu          *sync.RWMutex
}

func NewBaseStore(dir, name string, cfg *StoreCfg) (*baseStore, error) {
	var err error
	bs := &baseStore{
		syncBase:   *newSyncBase(),
		dir:        dir,
		name:       name,
		flushQueue: make(chan entry.Entry, DefaultMaxBatchSize*100),
	}
	bs.file, err = OpenRotateFile(dir, name, nil, cfg.RotateChecker, cfg.HistoryFactory)
	if err != nil {
		return nil, err
	}
	bs.flushCtx, bs.flushCancel = context.WithCancel(context.Background())
	bs.start()
	return bs, nil
}

func (bs *baseStore) start() {
	bs.wg.Add(1)
	go bs.flushLoop()
}

func (bs *baseStore) flushLoop() {
	defer bs.wg.Done()
	entries := make([]entry.Entry, 0, DefaultMaxBatchSize)
	for {
		select {
		case <-bs.flushCtx.Done():
			return
		case e := <-bs.flushQueue:
			entries = append(entries, e)
		Left:
			for i := 0; i < DefaultMaxBatchSize-1; i++ {
				select {
				case e = <-bs.flushQueue:
					entries = append(entries, e)
				default:
					break Left
				}
			}
			cnt := len(entries)
			bs.onEntries(entries)
			entries = entries[:0]
			bs.flushWg.Add(-1 * cnt)
		}
	}
}

//set infosize, mashal info
func (bs *baseStore) PrepareEntry(e entry.Entry) (entry.Entry, error) {
	v := e.GetInfo()
	switch info := v.(type) {
	case *entry.UncommitInfo:
		addrs := make([]*VFileAddress, 0)
		for group, tids := range info.Tids {
			for _, tid := range tids {
				addr := bs.GetLastAddr(group, tid)
				if addr == nil{
					addr = &VFileAddress{}
				}
				addr.Group = group
				addr.Tid = tid
				addrs = append(addrs, addr)

			}
		}
		buf, err := json.Marshal(addrs)
		if err != nil {
			return nil, err
		}
		e.SetInfoSize(len(buf))
		e.SetInfoBuf(buf)
		return e, nil
	case *entry.TxnInfo:
		addr := bs.GetLastAddr(info.Group, info.Tid)
		info.Addr = addr
		buf, err := json.Marshal(info)
		if err != nil {
			return nil, err
		}
		e.SetInfoSize(len(buf))
		e.SetInfoBuf(buf)
		return e, nil
	case *entry.CheckpointInfo, *entry.CommitInfo:
		buf, err := json.Marshal(info)
		if err != nil {
			return nil, err
		}
		size := len(buf)
		e.SetInfoSize(size)
		e.SetInfoBuf(buf)
		return e, nil
	default:
		return e, nil
	}
}

func (bs *baseStore) onEntries(entries []entry.Entry) {
	var err error
	for _, e := range entries {
		e, err := bs.PrepareEntry(e)
		if err != nil {
			panic(err)
		}
		appender := bs.file.GetAppender()
		if err = appender.Prepare(e.TotalSize(), e.GetInfo()); err != nil {
			panic(err)
		}
		if _, err = e.WriteTo(appender); err != nil {
			panic(err)
		}
		if err = appender.Commit(); err != nil {
			panic(err)
		}
		bs.OnEntryReceived(e)
	}
	if err = bs.file.Sync(); err != nil {
		panic(err)
	}
	bs.OnCommit()

	for _, e := range entries {
		e.DoneWithErr(nil)
	}
}

func (bs *baseStore) Close() error {
	if !bs.TryClose() {
		return nil
	}
	bs.flushWg.Wait()
	bs.flushCancel()
	bs.wg.Wait()
	return bs.file.Close()
}

func (bs *baseStore) Checkpoint(e entry.Entry) (err error) {
	if e.IsCheckpoint() {
		return errors.New("wrong entry type")
	}
	return bs.AppendEntry(e)
}

func (bs *baseStore) TryTruncate() error {
	return bs.file.GetHistory().TryTruncate()
}

func (bs *baseStore) AppendEntry(e entry.Entry) (err error) {
	if bs.IsClosed() {
		return common.ClosedErr
	}
	bs.flushWg.Add(1)
	if bs.IsClosed() {
		bs.flushWg.Done()
		return common.ClosedErr
	}
	bs.flushQueue <- e
	return nil
}
