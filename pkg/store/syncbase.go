package store

import (
	"github.com/jiangxinmeng1/logstore/pkg/entry"
	"sync"
)

type syncBase struct {
	*sync.RWMutex
	checkpointing, syncing map[uint32]uint64
	checkpointed, synced   *syncMap
	uncommits              map[uint32]map[uint64]*VFileAddress
}

type syncMap struct {
	*sync.RWMutex
	ids map[uint32]uint64
}

func newSyncMap() *syncMap {
	return &syncMap{
		RWMutex: new(sync.RWMutex),
		ids:     make(map[uint32]uint64),
	}
}
func newSyncBase() *syncBase {
	return &syncBase{
		checkpointing: make(map[uint32]uint64),
		syncing:       make(map[uint32]uint64),
		checkpointed:  newSyncMap(),
		synced:        newSyncMap(),
		uncommits:     make(map[uint32]map[uint64]*VFileAddress),
	}
}

func (base *syncBase) GetLastAddr(groupName uint32, tid uint64) *VFileAddress {
	tidMap, ok := base.uncommits[groupName]
	if !ok {
		return nil
	}
	return tidMap[tid]
}

func (base *syncBase) OnEntryReceived(e entry.Entry) error {
	if info := e.GetInfo(); info != nil {
		switch v := info.(type) {
		case *entry.CommitInfo:
			base.syncing[v.Group] = v.CommitId
		case *entry.CheckpointInfo:
			for group, interval:= range v.CheckpointRanges{
			base.checkpointing[group] = interval.End
			}
		case *entry.UncommitInfo:
			addr := v.Addr.(*VFileAddress)
			for group, tids := range v.Tids {
				for _, tid := range tids {
					tidMap, ok := base.uncommits[group]
					if !ok {
						tidMap = make(map[uint64]*VFileAddress)
					}
					tidMap[tid] = addr
					base.uncommits[group] = tidMap
				}
			}
		case *entry.TxnInfo:
			base.syncing[v.Group] = v.CommitId
			tidMap, ok := base.uncommits[v.Group]
			if !ok {
				return nil
			}
			_, ok = tidMap[v.Tid]
			if !ok {
				return nil
			}
			delete(tidMap, v.Tid)
		default:
			panic("not supported")
		}
	}
	return nil
}

func (base *syncBase) GetPenddings(groupId uint32) uint64 {
	ckp := base.GetCheckpointed(groupId)
	commit := base.GetSynced(groupId)
	return commit - ckp
}

func (base *syncBase) GetCheckpointed(groupId uint32) uint64 {
	base.checkpointed.RLock()
	defer base.checkpointed.RUnlock()
	return base.checkpointed.ids[groupId]
}

func (base *syncBase) SetCheckpointed(groupId uint32, id uint64) {
	base.checkpointed.Lock()
	base.checkpointed.ids[groupId] = id
	base.checkpointed.Unlock()
}

func (base *syncBase) GetSynced(groupId uint32) uint64 {
	base.synced.RLock()
	defer base.synced.RUnlock()
	return base.synced.ids[groupId]
}

func (base *syncBase) SetSynced(groupId uint32, id uint64) {
	base.synced.Lock()
	base.synced.ids[groupId] = id
	base.synced.Unlock()
}

func (base *syncBase) OnCommit() {
	for group, checkpointingId := range base.checkpointing {
		checkpointedId := base.GetCheckpointed(group)
		if checkpointingId > checkpointedId {
			base.SetCheckpointed(group, checkpointingId)
		}
	}

	for group, syncingId := range base.syncing {
		syncedId := base.GetSynced(group)
		if syncingId > syncedId {
			base.SetSynced(group, syncingId)
		}
	}
}
