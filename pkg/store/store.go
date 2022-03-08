package store

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"

	"github.com/jiangxinmeng1/logstore/pkg/common"
	"github.com/jiangxinmeng1/logstore/pkg/entry"
)

var (
	DefaultMaxBatchSize = 500
	FlushEntry          entry.Entry
)

func init() {
	FlushEntry = entry.GetBase()
	FlushEntry.SetType(entry.ETFlush)
	payload := make([]byte, 0)
	FlushEntry.Unmarshal(payload)
}

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
	if cfg == nil {
		cfg = &StoreCfg{}
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
	v1 := e.GetInfo()
	if v1 == nil {
		return e, nil
	}
	v := v1.(*entry.Info)
	v.Info = &VFileAddress{}
	switch v.Group {
	case entry.GTUncommit:
		addrs := make([]*VFileAddress, 0)
		for _, tids := range v.Uncommits {
			addr := bs.GetLastAddr(tids.Group, tids.Tid)
			if addr == nil {
				addr = &VFileAddress{}
			}
			addr.Group = tids.Group
			addr.LSN = tids.Tid
			addrs = append(addrs, addr)
		}
		buf, err := json.Marshal(addrs)
		if err != nil {
			return nil, err
		}
		e.SetInfoSize(len(buf))
		e.SetInfoBuf(buf)
		return e, nil
	default:
		buf, err := json.Marshal(v)
		if err != nil {
			return nil, err
		}
		size := len(buf)
		e.SetInfoSize(size)
		e.SetInfoBuf(buf)
		return e, nil
	}
}

func (bs *baseStore) onEntries(entries []entry.Entry) {
	var err error
	fmt.Printf("%d entries per batch\n",len(entries))
	for _, e := range entries {
		appender := bs.file.GetAppender()
		e, err := bs.PrepareEntry(e)
		if err != nil {
			panic(err)
		}
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

func (bs *baseStore) OnCommit(){
	//buffer
	//offset
	//file
	bs.syncBase.OnCommit()
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

func (bs *baseStore) TryCompact() error {
	return bs.file.GetHistory().TryTruncate()
}
func (bs *baseStore) TryTruncate(size int64) error {
	return bs.file.TryTruncate(size)
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
func (s *baseStore) Sync() error {
	if err := s.AppendEntry(FlushEntry); err != nil {
		return err
	}
	// if err := s.writer.Flush(); err != nil {
	// 	return err
	// }
	err := s.file.Sync()
	return err
}

func (s *baseStore) Replay(h ApplyHandle) error {
	r := newReplayer(h)
	o := &noopObserver{}
	err := s.file.Replay(r.replayHandler, o)
	if err != nil {
		return err
	}
	for group, checkpointed := range r.checkpointrange {
		s.checkpointed.ids[group] = checkpointed.Intervals[0].End
	}
	for _, ent := range r.entrys {
		s.synced.ids[ent.group] = ent.commitId
	}
	//TODO uncommit entry info
	r.Apply()
	return nil
}

func (s *baseStore) Load(groupId uint32, lsn uint64) (entry.Entry, error) {
	ver, err := s.GetVersionByGLSN(groupId, lsn)
	if err != nil {
		return nil, err
	}
	return s.file.Load(ver, groupId, lsn)
}
