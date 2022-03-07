package store

import (
	"bytes"
	"context"
	"fmt"
	"github.com/jiangxinmeng1/logstore/pkg/common"
	"github.com/jiangxinmeng1/logstore/pkg/entry"
	"math/rand"
	"os"
	"sync"
	"testing"

	"github.com/panjf2000/ants/v2"
	"github.com/stretchr/testify/assert"
)

func TestStore(t *testing.T) {
	dir := "/tmp/logstore/teststore"
	name := "mock"
	os.RemoveAll(dir)
	cfg := &StoreCfg{
		RotateChecker: NewMaxSizeRotateChecker(int(common.K) * 20),
	}
	s, err := NewBaseStore(dir, name, cfg)
	assert.Nil(t, err)
	defer s.Close()

	var wg sync.WaitGroup
	var fwg sync.WaitGroup
	ch := make(chan entry.Entry, 1000)
	ctx, cancel := context.WithCancel(context.Background())
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-ctx.Done():
				return
			case e := <-ch:
				err := e.WaitDone()
				assert.Nil(t, err)
				t.Logf("synced %d", s.GetSynced(1))
				// t.Logf("checkpointed %d", s.GetCheckpointed())
				fwg.Done()
			}
		}
	}()

	var bs bytes.Buffer
	for i := 0; i < 1000; i++ {
		bs.WriteString("helloyou")
	}
	buf := bs.Bytes()
	cnt := 5
	for i := 0; i < cnt; i++ {
		e := entry.GetBase()
		if i%2 == 0 && i > 0 {
			checkpointInfo := &entry.Info{
				Group: entry.GTCKp,
				Checkpoints: []entry.CkpRanges{{
					Group: 1,
					Ranges: []common.ClosedInterval{{
						End: common.GetGlobalSeqNum(),
					}},
				}},
			}
			e.SetInfo(checkpointInfo)
			e.SetType(entry.ETCheckpoint)
		} else {
			commitInterval := &entry.Info{
				Group:    entry.GTCustomizedStart,
				CommitId: common.NextGlobalSeqNum(),
			}
			e.SetInfo(commitInterval)
			e.SetType(entry.ETCustomizedStart)
		}
		n := common.GPool.Alloc(uint64(len(buf)))
		n.Buf = n.Buf[:len(buf)]
		copy(n.GetBuf(), buf)
		e.UnmarshalFromNode(n, true)
		err := s.AppendEntry(e)
		assert.Nil(t, err)
		fwg.Add(1)
		ch <- e
	}

	fwg.Wait()
	cancel()
	wg.Wait()

	h := s.file.GetHistory()
	t.Log(h.String())
	err = h.TryTruncate()
	assert.Nil(t, err)
}

func TestMultiGroup(t *testing.T) {
	dir := "/tmp/logstore/teststore"
	name := "mock"
	os.RemoveAll(dir)
	cfg := &StoreCfg{
		RotateChecker: NewMaxSizeRotateChecker(int(common.K) * 200),
	}
	s, err := NewBaseStore(dir, name, cfg)
	assert.Nil(t, err)
	defer s.Close()

	var wg sync.WaitGroup
	var fwg sync.WaitGroup
	ch := make(chan entry.Entry, 1000)
	ctx, cancel := context.WithCancel(context.Background())
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-ctx.Done():
				return
			case e := <-ch:
				err := e.WaitDone()
				assert.Nil(t, err)
				t.Logf("synced %d", s.GetSynced(1))
				t.Logf("checkpointed %d", s.GetCheckpointed(1))
				t.Logf("penddings %d", s.GetPenddings(1))
				fwg.Done()
			}
		}
	}()

	var bs bytes.Buffer
	for i := 0; i < 10; i++ {
		bs.WriteString("helloyou")
	}
	buf := bs.Bytes()

	entryPerGroup := 5000
	groupCnt := 5
	worker, _ := ants.NewPool(groupCnt)
	fwg.Add(entryPerGroup * groupCnt)
	f := func(groupNo uint32) func() {
		return func() {
			alloc := &common.IdAllocator{}
			ckp := uint64(0)
			for i := 0; i < entryPerGroup; i++ {
				e := entry.GetBase()
				if i%1000 == 0 && i > 0 {
					end := alloc.Get()
					checkpointInfo := &entry.Info{
						Group: entry.GTCKp,
						Checkpoints: []entry.CkpRanges{{
							Group: 1,
							Ranges: []common.ClosedInterval{{
								Start: ckp,
								End:   end,
							}},
						}},
					}
					ckp = end
					e.SetInfo(checkpointInfo)
					e.SetType(entry.ETCheckpoint)
				} else {
					commitInterval := &entry.Info{
						Group:    groupNo,
						CommitId: alloc.Alloc(),
					}
					e.SetInfo(commitInterval)
					e.SetType(entry.ETCustomizedStart)
				}
				n := common.GPool.Alloc(uint64(len(buf)))
				n.Buf = n.Buf[:len(buf)]
				copy(n.GetBuf(), buf)
				e.UnmarshalFromNode(n, true)
				s.AppendEntry(e)
				ch <- e
			}
		}
	}

	for j := 0; j < groupCnt; j++ {
		no := uint32(j) + entry.GTCustomizedStart
		worker.Submit(f(no))
	}

	fwg.Wait()
	cancel()
	wg.Wait()
	s.file.GetHistory().TryTruncate()

	h := s.file.GetHistory()
	t.Log(h.String())
	err = h.TryTruncate()
	assert.Nil(t, err)
}

func TestUncommitEntry(t *testing.T) {
	dir := "/tmp/logstore/teststore"
	name := "mock"
	os.RemoveAll(dir)
	cfg := &StoreCfg{
		RotateChecker: NewMaxSizeRotateChecker(int(common.K) * 2000),
	}
	s, err := NewBaseStore(dir, name, cfg)
	assert.Nil(t, err)
	defer s.Close()

	var wg sync.WaitGroup
	var fwg sync.WaitGroup
	ch := make(chan entry.Entry, 1000)
	ctx, cancel := context.WithCancel(context.Background())
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-ctx.Done():
				return
			case e := <-ch:
				err := e.WaitDone()
				assert.Nil(t, err)
				t.Logf("synced %d", s.GetSynced(1))
				t.Logf("checkpointed %d", s.GetCheckpointed(1))
				t.Logf("penddings %d", s.GetPenddings(1))
				fwg.Done()
			}
		}
	}()

	var bs bytes.Buffer
	for i := 0; i < 3000; i++ {
		bs.WriteString("helloyou")
	}
	buf := bs.Bytes()

	entryPerGroup := 550
	groupCnt := 1
	worker, _ := ants.NewPool(groupCnt)
	fwg.Add(entryPerGroup * groupCnt)
	f := func(groupNo uint32) func() {
		return func() {
			cidAlloc := &common.IdAllocator{}
			tidAlloc := &common.IdAllocator{}
			ckp := uint64(0)
			for i := 0; i < entryPerGroup; i++ {
				e := entry.GetBase()
				switch i % 100 {
				case 1, 2, 3, 4, 5:
					uncommitInfo := &entry.Info{
						Group: entry.GTUncommit,
						Uncommits: []entry.Tid{{
							Group: groupNo,
							Tid:   tidAlloc.Get() + 1 + uint64(rand.Intn(3)),
						}},
					}
					e.SetType(entry.ETUncommitted)
					e.SetInfo(uncommitInfo)
				case 99:
					end := cidAlloc.Get()
					checkpointInfo := &entry.Info{
						Group: entry.GTCKp,
						Checkpoints: []entry.CkpRanges{{
							Group: 1,
							Ranges: []common.ClosedInterval{{
								Start: ckp,
								End:   end,
							}},
						}},
					}
					ckp = end
					e.SetType(entry.ETCheckpoint)
					e.SetInfo(checkpointInfo)
				case 50, 51, 52, 53:
					txnInfo := &entry.Info{
						Group:    groupNo,
						TxnId:    tidAlloc.Alloc(),
						CommitId: cidAlloc.Alloc(),
					}
					e.SetType(entry.ETTxn)
					e.SetInfo(txnInfo)
				default:
					commitInterval := &entry.Info{
						Group:    groupNo,
						CommitId: cidAlloc.Alloc(),
					}
					e.SetType(entry.ETCustomizedStart)
					e.SetInfo(commitInterval)
				}
				n := common.GPool.Alloc(uint64(len(buf)))
				n.Buf = n.Buf[:len(buf)]
				copy(n.GetBuf(), buf)
				e.UnmarshalFromNode(n, true)
				s.AppendEntry(e)
				ch <- e
			}
		}
	}

	for j := 0; j < groupCnt; j++ {
		worker.Submit(f(uint32(j)))
	}

	fwg.Wait()
	cancel()
	wg.Wait()

	h := s.file.GetHistory()
	t.Log(h.String())
	err = h.TryTruncate()
	assert.Nil(t, err)
}

func TestReplay(t *testing.T) {
	dir := "/tmp/logstore/teststore"
	name := "mock"
	os.RemoveAll(dir)
	cfg := &StoreCfg{
		RotateChecker: NewMaxSizeRotateChecker(int(common.K) * 2000),
	}
	s, err := NewBaseStore(dir, name, cfg)
	assert.Nil(t, err)

	var wg sync.WaitGroup
	var fwg sync.WaitGroup
	ch := make(chan entry.Entry, 1000)
	ctx, cancel := context.WithCancel(context.Background())
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-ctx.Done():
				return
			case e := <-ch:
				err := e.WaitDone()
				assert.Nil(t, err)
				// t.Logf("synced %d", s.GetSynced("group1"))
				// t.Logf("checkpointed %d", s.GetCheckpointed("group1"))
				// t.Logf("penddings %d", s.GetPenddings("group1"))
				fwg.Done()
			}
		}
	}()

	entryPerGroup := 50
	groupCnt := 2
	worker, _ := ants.NewPool(groupCnt)
	fwg.Add(entryPerGroup * groupCnt)
	f := func(groupNo uint32) func() {
		return func() {
			cidAlloc := &common.IdAllocator{}
			tidAlloc := &common.IdAllocator{}
			ckp := uint64(0)
			for i := 0; i < entryPerGroup; i++ {
				e := entry.GetBase()
				switch i % 50 {
				case 1, 2, 3, 4, 5: //uncommit entry
					e.SetType(entry.ETUncommitted)
					uncommitInfo := &entry.Info{
						Group: entry.GTUncommit,
						Uncommits: []entry.Tid{{
							Group: groupNo,
							Tid:   tidAlloc.Get() + 1 + uint64(rand.Intn(3)),
						}},
					}
					fmt.Printf("%s", uncommitInfo.ToString())
					e.SetInfo(uncommitInfo)
					str := uncommitInfo.ToString()
					buf := []byte(str)
					n := common.GPool.Alloc(uint64(len(buf)))
					n.Buf = n.Buf[:len(buf)]
					copy(n.GetBuf(), buf)
					e.UnmarshalFromNode(n, true)
				case 49: //ckp entry
					e.SetType(entry.ETCheckpoint)
					end := cidAlloc.Get()
					checkpointInfo := &entry.Info{
						Group: entry.GTCKp,
						Checkpoints: []entry.CkpRanges{{
							Group: 1,
							Ranges: []common.ClosedInterval{{
								Start: ckp,
								End:   end,
							}},
						}},
					}
					ckp = end
					e.SetInfo(checkpointInfo)
					str := checkpointInfo.ToString()
					buf := []byte(str)
					n := common.GPool.Alloc(uint64(len(buf)))
					n.Buf = n.Buf[:len(buf)]
					copy(n.GetBuf(), buf)
					e.UnmarshalFromNode(n, true)
				case 20, 21, 22, 23: //txn entry
					e.SetType(entry.ETTxn)
					txnInfo := &entry.Info{
						Group:    groupNo,
						TxnId:      tidAlloc.Alloc(),
						CommitId: cidAlloc.Alloc(),
					}
					e.SetInfo(txnInfo)
					str := txnInfo.ToString()
					buf := []byte(str)
					n := common.GPool.Alloc(uint64(len(buf)))
					n.Buf = n.Buf[:len(buf)]
					copy(n.GetBuf(), buf)
					e.UnmarshalFromNode(n, true)
				case 26, 28: //flush entry
					e.SetType(entry.ETFlush)
					payload := make([]byte, 0)
					e.Unmarshal(payload)
				default: //commit entry
					e.SetType(entry.ETCustomizedStart)
					commitInterval := &entry.Info{
						Group:    groupNo,
						CommitId: cidAlloc.Alloc(),
					}
					e.SetInfo(commitInterval)
					str := commitInterval.ToString()
					buf := []byte(str)
					n := common.GPool.Alloc(uint64(len(buf)))
					n.Buf = n.Buf[:len(buf)]
					copy(n.GetBuf(), buf)
					e.UnmarshalFromNode(n, true)
				}
				s.AppendEntry(e)
				ch <- e
			}
		}
	}

	for j := 0; j < groupCnt; j++ {
		worker.Submit(f(uint32(j)))
	}

	fwg.Wait()
	cancel()
	wg.Wait()

	h := s.file.GetHistory()
	// t.Log(h.String())
	err = h.TryTruncate()
	assert.Nil(t, err)

	s.Close()

	s, _ = NewBaseStore(dir, name, cfg)
	a := func(group uint32, commitId uint64, payload []byte, typ uint16, info interface{}) (err error) {
		fmt.Printf("%s", payload)
		return nil
	}
	r := newReplayer(a)
	o := &noopObserver{}
	err = s.file.Replay(r.replayHandler, o)
	if err != nil {
		fmt.Printf("err is %v", err)
	}
	r.Apply()
	s.Close()
}
