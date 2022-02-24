package store

import (
	"encoding/json"
	"errors"
	"fmt"
	"github.com/jiangxinmeng1/logstore/pkg/common"
	"github.com/jiangxinmeng1/logstore/pkg/entry"
	"io"
)

type noopObserver struct {
}

func (o *noopObserver) OnNewEntry(_ int) {
}

func (o *noopObserver) OnNewCommit(*entry.CommitInfo)         {}
func (o *noopObserver) OnNewCheckpoint(*entry.CheckpointInfo) {}
func (o *noopObserver) OnNewTxn(*entry.TxnInfo)               {}
func (o *noopObserver) OnNewUncommit(addrs []*VFileAddress)   {}

type replayer struct {
	version         int
	state           vFileState
	uncommit        map[string]map[uint64][]*replayEntry
	entrys          []*replayEntry
	checkpointrange map[string]*common.ClosedInterval
	checkpoints     []*replayEntry
	mergeFuncs      map[string]func(pre, curr []byte) []byte
	applyEntry      ApplyHandle
}

func newReplayer(h ApplyHandle) *replayer {
	return &replayer{
		uncommit:        make(map[string]map[uint64][]*replayEntry),
		entrys:          make([]*replayEntry, 0),
		checkpointrange: make(map[string]*common.ClosedInterval),
		checkpoints:     make([]*replayEntry, 0),
		mergeFuncs:      make(map[string]func(pre []byte, curr []byte) []byte),
		applyEntry:      h,
	}
}
func defaultMergePayload(pre, curr []byte) []byte {
	return append(pre, curr...)
}
func (r *replayer) mergeUncommittedEntries(pre, curr *replayEntry) *replayEntry {
	if pre == nil {
		return curr
	}
	mergePayload, ok := r.mergeFuncs[curr.group]
	if !ok {
		mergePayload = defaultMergePayload
	}
	curr.payload = mergePayload(pre.payload, curr.payload)
	return curr
}

func (r *replayer) Apply() {
	for _, e := range r.checkpoints {
		r.applyEntry(e.group, e.commitId, e.payload, e.entryType, e.info)
	}

	for _, e := range r.entrys {
		interval, ok := r.checkpointrange[e.group]
		if ok {
			if interval.Contains(
				common.ClosedInterval{Start: e.commitId, End: e.commitId}) {
				continue
			}
		}
		if e.entryType == entry.ETTxn {
			var pre *replayEntry
			tidMap, ok := r.uncommit[e.group]
			if ok {
				entries, ok := tidMap[e.tid]
				if ok {
					for _, entry := range entries {
						pre = r.mergeUncommittedEntries(
							pre, entry)
					}
				}
			}
			e = r.mergeUncommittedEntries(pre, e)
			r.applyEntry(e.group, e.commitId, e.payload, e.entryType, nil)
		} else {
			r.applyEntry(e.group, e.commitId, e.payload, e.entryType, nil)
		}
	}
}

type replayEntry struct {
	entryType uint16
	group     string
	commitId  uint64
	// isTxn     bool
	tid uint64
	// checkpointRange *common.ClosedInterval
	payload []byte
	info    interface{}
}

func (r *replayer) onReplayEntry(e entry.Entry, vf ReplayObserver) error {
	typ := e.GetType()
	switch typ {
	case entry.ETFlush:
		return nil
	case entry.ETCheckpoint:
		// fmt.Printf("ETCheckpoint\n")
		infobuf := e.GetInfoBuf()
		info := &entry.CheckpointInfo{}
		json.Unmarshal(infobuf, info)
		replayEty := &replayEntry{
			entryType: typ,
			payload:   make([]byte, e.GetPayloadSize()),
			info:      info,
		}
		copy(replayEty.payload, e.GetPayload())
		r.checkpoints = append(r.checkpoints, replayEty)

		for group, ckp := range info.CheckpointRanges {
			interval, ok := r.checkpointrange[group]
			if !ok {
				interval = &common.ClosedInterval{
					Start: ckp.Start,
					End:   ckp.End,
				}
			} else {
				interval.TryMerge(*ckp)
			}
			r.checkpointrange[group] = interval
		}
		vf.OnNewCheckpoint(info)
	case entry.ETUncommitted:
		// fmt.Printf("ETUncommitted\n")
		infobuf := e.GetInfoBuf()
		addrs := make([]*VFileAddress, 0)
		json.Unmarshal(infobuf, &addrs)
		for _, addr := range addrs {
			tidMap, ok := r.uncommit[addr.Group]
			if !ok {
				tidMap = make(map[uint64][]*replayEntry)
			}
			entries, ok := tidMap[addr.Tid]
			if !ok {
				entries = make([]*replayEntry, 0)
			}
			replayEty := &replayEntry{
				payload: make([]byte, e.GetPayloadSize()),
			}
			copy(replayEty.payload, e.GetPayload())
			entries = append(entries, replayEty)
			tidMap[addr.Tid] = entries
			r.uncommit[addr.Group] = tidMap
		}
	case entry.ETTxn:
		// fmt.Printf("ETTxn\n")
		infobuf := e.GetInfoBuf()
		info := &entry.TxnInfo{}
		json.Unmarshal(infobuf, info)
		replayEty := &replayEntry{
			entryType: e.GetType(),
			group:     info.Group,
			commitId:  info.CommitId,
			tid:       info.Tid,
			payload:   make([]byte, e.GetPayloadSize()),
		}
		copy(replayEty.payload, e.GetPayload())
		r.entrys = append(r.entrys, replayEty)
		vf.OnNewTxn(info)
	default:
		// fmt.Printf("default\n")
		infobuf := e.GetInfoBuf()
		info := &entry.CommitInfo{}
		json.Unmarshal(infobuf, info)
		replayEty := &replayEntry{
			entryType: e.GetType(),
			group:     info.Group,
			commitId:  info.CommitId,
			payload:   make([]byte, e.GetPayloadSize()),
		}
		copy(replayEty.payload, e.GetPayload())
		r.entrys = append(r.entrys, replayEty)
		vf.OnNewCommit(info)
	}
	return nil
}

func (r *replayer) replayHandler(v VFile, o ReplayObserver) error {
	vfile := v.(*vFile)
	if vfile.version != r.version {
		r.state.pos = 0
	}
	current := vfile.GetState()
	entry := entry.GetBase()
	defer entry.Free()

	metaBuf := entry.GetMetaBuf()
	_, err := vfile.Read(metaBuf)
	if err != nil {
		if !errors.Is(err, io.EOF) {
			return err
		}
		vfile.Truncate(int64(r.state.pos))
		return err
	}

	n, err := entry.ReadFrom(vfile)
	if err != nil {
		if !errors.Is(err, io.EOF) {
			return err
		}
		vfile.Truncate(int64(r.state.pos))
		return err
	}
	if n != entry.TotalSizeExpectMeta() {
		if current.pos == r.state.pos+n {
			vfile.Truncate(int64(current.pos))
			return io.EOF
		} else {
			return errors.New(fmt.Sprintf(
				"payload mismatch: %d != %d", n, entry.GetPayloadSize()))
		}
	}
	if err = r.onReplayEntry(entry, o); err != nil {
		return err
	}
	r.state.pos += entry.TotalSize()
	return nil
}
