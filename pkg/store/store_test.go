package store

import (
	"bytes"
	"context"
	"logstore/pkg/common"
	"logstore/pkg/entry"
	"math/rand"
	"os"
	"strconv"
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
				t.Logf("synced %d", s.GetSynced("group1"))
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
		e.SetType(entry.ETFlush)
		if i%2 == 0 && i > 0 {
			checkpointInterval := &entry.CheckpointInfo{
				Group: "group1",
				Checkpoint: &common.ClosedInterval{
					End: common.GetGlobalSeqNum(),
				},
			}
			e.SetInfo(checkpointInterval)
		} else {
			commitInterval := &entry.CommitInfo{
				Group:    "group1",
				CommitId: common.NextGlobalSeqNum(),
			}
			e.SetInfo(commitInterval)
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
				t.Logf("synced %d", s.GetSynced("group1"))
				t.Logf("checkpointed %d", s.GetCheckpointed("group1"))
				t.Logf("penddings %d", s.GetPenddings("group1"))
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
	f := func(groupNo int) func() {
		return func() {
			groupName := "group" + strconv.Itoa(groupNo)
			alloc := &common.IdAllocator{}
			ckp := uint64(0)
			for i := 0; i < entryPerGroup; i++ {
				e := entry.GetBase()
				e.SetType(entry.ETFlush)
				if i%1000 == 0 && i > 0 {
					checkpointInterval := &entry.CheckpointInfo{
						Group: groupName,
						Checkpoint: &common.ClosedInterval{
							Start: ckp,
							End:   alloc.Get(),
						},
					}
					ckp = checkpointInterval.Checkpoint.End + 1
					e.SetInfo(checkpointInterval)
				} else {
					commitInterval := &entry.CommitInfo{
						Group:    groupName,
						CommitId: alloc.Alloc(),
					}
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
		worker.Submit(f(j))
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
				t.Logf("synced %d", s.GetSynced("group1"))
				t.Logf("checkpointed %d", s.GetCheckpointed("group1"))
				t.Logf("penddings %d", s.GetPenddings("group1"))
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
	f := func(groupNo int) func() {
		return func() {
			groupName := "group" + strconv.Itoa(groupNo)
			cidAlloc := &common.IdAllocator{}
			tidAlloc := &common.IdAllocator{}
			ckp := uint64(0)
			for i := 0; i < entryPerGroup; i++ {
				e := entry.GetBase()
				e.SetType(entry.ETFlush)
				switch i % 100 {
				case 1, 2, 3, 4, 5:
					tids := make(map[string][]uint64)
					tids[groupName] = make([]uint64, 0)
					tids[groupName] = append(tids[groupName], tidAlloc.Get() + 1 + uint64(rand.Intn(3)))
					uncommitInfo := &entry.UncommitInfo{
						Tids:   tids,
					}
					e.SetInfo(uncommitInfo)
				case 99:
					checkpointInterval := &entry.CheckpointInfo{
						Group: groupName,
						Checkpoint: &common.ClosedInterval{
							Start: ckp,
							End:   cidAlloc.Get(),
						},
					}
					ckp = checkpointInterval.Checkpoint.End
					e.SetInfo(checkpointInterval)
				case 50, 51, 52, 53:
					txnInfo := &entry.TxnInfo{
						Group:    groupName,
						Tid:      tidAlloc.Alloc(),
						CommitId: cidAlloc.Alloc(),
					}
					e.SetInfo(txnInfo)
				default:
					commitInterval := &entry.CommitInfo{
						Group:    groupName,
						CommitId: cidAlloc.Alloc(),
					}
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
		worker.Submit(f(j))
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
				t.Logf("synced %d", s.GetSynced("group1"))
				t.Logf("checkpointed %d", s.GetCheckpointed("group1"))
				t.Logf("penddings %d", s.GetPenddings("group1"))
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
	f := func(groupNo int) func() {
		return func() {
			groupName := "group" + strconv.Itoa(groupNo)
			cidAlloc := &common.IdAllocator{}
			tidAlloc := &common.IdAllocator{}
			ckp := uint64(0)
			for i := 0; i < entryPerGroup; i++ {
				e := entry.GetBase()
				e.SetType(entry.ETFlush)
				switch i % 100 {
				case 1, 2, 3, 4, 5:
					tids := make(map[string][]uint64)
					tids[groupName] = make([]uint64, 0)
					tids[groupName] = append(tids[groupName], tidAlloc.Get() + 1 + uint64(rand.Intn(3)))
					uncommitInfo := &entry.UncommitInfo{
						Tids:   tids,
					}
					e.SetInfo(uncommitInfo)
				case 99:
					checkpointInterval := &entry.CheckpointInfo{
						Group: groupName,
						Checkpoint: &common.ClosedInterval{
							Start: ckp,
							End:   cidAlloc.Get(),
						},
					}
					ckp = checkpointInterval.Checkpoint.End
					e.SetInfo(checkpointInterval)
				case 50, 51, 52, 53:
					txnInfo := &entry.TxnInfo{
						Group:    groupName,
						Tid:      tidAlloc.Alloc(),
						CommitId: cidAlloc.Alloc(),
					}
					e.SetInfo(txnInfo)
				default:
					commitInterval := &entry.CommitInfo{
						Group:    groupName,
						CommitId: cidAlloc.Alloc(),
					}
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
		worker.Submit(f(j))
	}

	fwg.Wait()
	cancel()
	wg.Wait()

	h := s.file.GetHistory()
	t.Log(h.String())
	err = h.TryTruncate()
	assert.Nil(t, err)
}

//test replay(find last addr)
