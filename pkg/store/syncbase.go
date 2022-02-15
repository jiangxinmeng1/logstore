package store

import (
	"logstore/pkg/entry"
	"sync"
)

type syncBase struct {
	*sync.RWMutex
	checkpointing, syncing map[string]uint64
	checkpointed, synced   *syncMap
}

type syncMap struct {
	*sync.RWMutex
	ids map[string]uint64
}

func newSyncMap() *syncMap {
	return &syncMap{
		RWMutex: new(sync.RWMutex),
		ids:     make(map[string]uint64),
	}
}
func newSyncBase() *syncBase {
	return &syncBase{
		checkpointing: make(map[string]uint64),
		syncing:       make(map[string]uint64),
		checkpointed:  newSyncMap(),
		synced:        newSyncMap(),
	}
}

func (base *syncBase) OnEntryReceived(e entry.Entry) error {
	if info := e.GetInfo(); info != nil {
		switch v := info.(type) {
		case *entry.CommitInfo:
			base.syncing[v.Group] = v.CommitId
		case *entry.CheckpointInfo:
			base.checkpointing[v.Group] = v.Checkpoint.End
		default:
			panic("not supported")
		}
	}
	return nil
}

func (base *syncBase) GetPenddings(groupName string) uint64 {
	ckp := base.GetCheckpointed(groupName)
	commit := base.GetSynced(groupName)
	return commit - ckp
}

func (base *syncBase) GetCheckpointed(groupName string) uint64 {
	base.checkpointed.RLock()
	defer base.checkpointed.RUnlock()
	return base.checkpointed.ids[groupName]
}

func (base *syncBase) SetCheckpointed(groupName string, id uint64) {
	base.checkpointed.Lock()
	base.checkpointed.ids[groupName] = id
	base.checkpointed.Unlock()
}

func (base *syncBase) GetSynced(groupName string) uint64 {
	base.synced.RLock()
	defer base.synced.RUnlock()
	return base.synced.ids[groupName]
}

func (base *syncBase) SetSynced(groupName string, id uint64) {
	base.synced.Lock()
	base.synced.ids[groupName] = id
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
