package store

import (
	"errors"
	"fmt"
	"sync"

	"github.com/jiangxinmeng1/logstore/pkg/common"
	"github.com/jiangxinmeng1/logstore/pkg/entry"
)

type syncBase struct {
	*sync.RWMutex
	checkpointing, syncing map[uint32]uint64
	checkpointed, synced   *syncMap
	uncommits              map[uint32][]uint64
	addrs                  map[uint32]map[int]common.ClosedInterval //group-version-glsn range
	addrmu                 sync.RWMutex
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
		uncommits:     make(map[uint32][]uint64),
		addrs:         make(map[uint32]map[int]common.ClosedInterval),
		addrmu:        sync.RWMutex{},
	}
}

func (base *syncBase) GetVersionByGLSN(groupId uint32, lsn uint64) (int, error) {
	base.addrmu.RLock()
	defer base.addrmu.RUnlock()
	versionsMap, ok := base.addrs[groupId]
	if !ok {
		return 0, errors.New("group not existed")
	}
	for ver, interval := range versionsMap {
		if interval.Contains(common.ClosedInterval{Start: lsn, End: lsn}) {
			return ver, nil
		}
	}
	fmt.Printf("versionsMap is %v\n", versionsMap)
	return 0, errors.New("lsn not existed")
}

//TODO
func (base *syncBase) GetLastAddr(groupName uint32, tid uint64) *VFileAddress {
	// tidMap, ok := base.uncommits[groupName]
	// if !ok {
	// 	return nil
	// }
	return nil
}

func (base *syncBase) OnEntryReceived(e entry.Entry) error {
	if info := e.GetInfo(); info != nil {
		v := info.(*entry.Info)
		switch v.Group {
		case entry.GTCKp:
			for _, intervals := range v.Checkpoints {
				base.checkpointing[intervals.Group] = intervals.Ranges.Intervals[0].End //TODO calculate the first range
			}
		case entry.GTUncommit:
			// addr := v.Addr.(*VFileAddress)
			for _, tid := range v.Uncommits {
				tids, ok := base.uncommits[tid.Group]
				if !ok {
					tids = make([]uint64, 0)
				}
				existed := false
				for _, id := range tids {
					if id == tid.Tid {
						existed = true
						break
					}
				}
				if !existed {
					tids = append(tids, tid.Tid)
				}
				base.uncommits[tid.Group] = tids
			}
			// fmt.Printf("receive uncommit %d-%d\n", v.Group, v.GroupLSN)
		default:
			base.syncing[v.Group] = v.CommitId
		}
		base.addrmu.Lock()
		defer base.addrmu.Unlock()
		addr := v.Info.(*VFileAddress)
		versionRanges, ok := base.addrs[addr.Group]
		if !ok {
			versionRanges = make(map[int]common.ClosedInterval)
		}
		interval, ok := versionRanges[addr.Version]
		if !ok {
			interval = common.ClosedInterval{}
		}
		interval.TryMerge(common.ClosedInterval{Start: 0, End: addr.LSN})
		versionRanges[addr.Version] = interval
		base.addrs[addr.Group] = versionRanges
		// fmt.Printf("versionsMap is %v\n", base.addrs)
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

func (base *syncBase) OnCommit(commitInfo *prepareCommit) {
	for group, checkpointingId := range commitInfo.checkpointing {
		checkpointedId := base.GetCheckpointed(group)
		if checkpointingId > checkpointedId {
			base.SetCheckpointed(group, checkpointingId)
		}
	}

	for group, syncingId := range commitInfo.syncing {
		syncedId := base.GetSynced(group)
		if syncingId > syncedId {
			base.SetSynced(group, syncingId)
		}
	}
}

func (base *syncBase) PrepareCommit() *prepareCommit {
	checkpointing := make(map[uint32]uint64)
	for group, checkpointingId := range base.checkpointing {
		checkpointing[group] = checkpointingId
	}
	syncing := make(map[uint32]uint64)
	for group, syncingId := range base.syncing {
		syncing[group] = syncingId
	}
	return &prepareCommit{
		checkpointing: checkpointing,
		syncing:       syncing,
	}
}
