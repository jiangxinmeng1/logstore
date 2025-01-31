package sm

import (
	"sync"
	"sync/atomic"

	"github.com/jiangxinmeng1/logstore/pkg/common"
	"github.com/jiangxinmeng1/logstore/pkg/entry"
	statemachine "github.com/jiangxinmeng1/logstore/pkg/sm"
	"github.com/jiangxinmeng1/logstore/pkg/store"

	"github.com/sirupsen/logrus"
)

type Request struct {
	Op   OpT
	Data []byte
}

type stateMachine struct {
	common.ClosedState
	statemachine.StateMachine
	wal      *Wal
	mu       *sync.RWMutex
	rows     []*Row
	visible  uint64
	pipeline *writePipeline
	lastCkp  uint64
}

func NewStateMachine(dir string, walCfg *store.StoreCfg) (*stateMachine, error) {
	wal, err := newWal(dir, "wal", walCfg)
	if err != nil {
		return nil, err
	}
	sm := &stateMachine{
		mu:   new(sync.RWMutex),
		wal:  wal,
		rows: make([]*Row, 0, 100),
	}
	sm.pipeline = newPipeline(sm)
	wg := new(sync.WaitGroup)
	waitingQueue := statemachine.NewWaitableQueue(100000, 1, sm, wg, nil, nil, sm.onEntries)
	checkpointQueue := statemachine.NewWaitableQueue(10000, 1, sm, wg, nil, nil, sm.checkpoint)
	sm.StateMachine = statemachine.NewStateMachine(wg, sm, waitingQueue, checkpointQueue)
	sm.Start()
	return sm, nil
}

func (sm *stateMachine) Close() error {
	sm.Stop()
	return sm.wal.Close()
}

func (sm *stateMachine) enqueueWait(e *pendingEntry) error {
	_, err := sm.EnqueueRecevied(e)
	return err
}

func (sm *stateMachine) enqueueCheckpoint() {
	sm.EnqueueCheckpoint(struct{}{})
}

func (sm *stateMachine) onEntries(items ...interface{}) {
	for _, item := range items {
		pending := item.(*pendingEntry)
		err := pending.entry.WaitDone()
		if err != nil {
			panic(err)
		}
		visible := pending.entry.GetInfo().(*entry.Info).CommitId
		pending.Done()
		atomic.StoreUint64(&sm.visible, visible)
		if visible >= sm.lastCkp+1000 {
			sm.enqueueCheckpoint()
			sm.lastCkp = visible
			logrus.Infof("checkpoint %d", visible)
		}
	}
}

func (sm *stateMachine) OnRequest(r *Request) error {
	switch r.Op {
	case TInsert:
		return sm.onInsert(r)
	}
	panic("not supported")
}

func (sm *stateMachine) onInsert(r *Request) error {
	row, e, err := sm.pipeline.prepare(r)
	if err != nil {
		return err
	}
	return sm.pipeline.commit(row, e)
}

func (sm *stateMachine) VisibleLSN() uint64 {
	return atomic.LoadUint64(&sm.visible)
}

func (sm *stateMachine) makeRoomForInsert(buf []byte) *Row {
	row := newRow()
	// sm.rows = append(sm.rows, row)
	return row
}

func (sm *stateMachine) checkpoint(_ ...interface{}) {
	e := entry.GetBase()
	defer e.Free()
	e.SetType(entry.ETCheckpoint)
	checkpoints := make(map[uint32]*common.ClosedInterval)
	checkpoints[1] = &common.ClosedInterval{
		End: sm.VisibleLSN(),
	}
	ckpInfo := &entry.Info{
		Checkpoints: []entry.CkpRanges{{
			Group: 1,
			Ranges: common.NewClosedIntervalsByInterval(
				&common.ClosedInterval{End: sm.VisibleLSN()},
			),
		}},
	}
	e.SetInfo(ckpInfo)
	_, err := sm.wal.AppendEntry(entry.GTCKp, e)
	if err != nil {
		return
	}
	if err = e.WaitDone(); err != nil {
		return
	}
	sm.wal.TryCompact()
}
