package store

import (
	"encoding/binary"
	"encoding/json"
	// "errors"
	"fmt"
	"logstore/pkg/common"
	"logstore/pkg/entry"

	"github.com/RoaringBitmap/roaring/roaring64"
)

type vInfo struct {
	Commits     map[string]*common.ClosedInterval
	Checkpoints map[string][]*common.ClosedInterval
	UncommitTxn map[string][]uint64
	// TxnCommit   map[string]*roaring64.Bitmap
	TidCidMap map[string]map[uint64]uint64
}

type VFileUncommitInfo struct {
	Index *roaring64.Bitmap
	Addr  *VFileAddress
}

type VFileAddress struct {
	FileName string
	Offset   int
}

//result contains addr, addr size
func (addr *VFileAddress) Marshal() ([]byte, error) {
	addrBuf, err := json.Marshal(addr)
	if err != nil {
		return nil, err
	}
	size := uint32(len(addrBuf))
	sizebuf := make([]byte, 4)
	binary.BigEndian.PutUint32(sizebuf, size)
	addrBuf = append(addrBuf, sizebuf...)
	return addrBuf, nil
}

//marshal address, return remained bytes
func (addr *VFileAddress) Unmarshal(buf []byte) ([]byte, error) {
	size := int(binary.BigEndian.Uint32(buf[len(buf)-4:]))
	err := json.Unmarshal(buf[len(buf)-4-size:len(buf)-4], addr)
	if err != nil {
		return nil, err
	}
	return buf[:len(buf)-4-size], nil
}

func newVInfo() *vInfo {
	return &vInfo{
		Commits:     make(map[string]*common.ClosedInterval),
		Checkpoints: make(map[string][]*common.ClosedInterval),
		UncommitTxn: make(map[string][]uint64),
		// TxnCommit:   make(map[string]*roaring64.Bitmap),
		TidCidMap: make(map[string]map[uint64]uint64),
	}
}

func (info *vInfo) ReadMeta(vf *vFile) {
	buf := make([]byte, Metasize)
	vf.ReadAt(buf, int64(vf.size)-int64(Metasize))
	size := binary.BigEndian.Uint16(buf)
	buf = make([]byte, int(size))
	vf.ReadAt(buf, int64(vf.size)-int64(Metasize)-int64(size))
	json.Unmarshal(buf, info)
	fmt.Printf("replay-%s\n", vf.String())
}

//get metadata
//return logged txn
// func (info *vInfo) GetSyncedTxnOffset(groupName string, tid uint64) (*roaring64.Bitmap, error) {
// 	return info.UncommitTxn[groupName][tid].Index.Clone(), nil
// }

// func (info *vInfo) OnTxnCommit(groupName string, tid, commitId uint64) error {
// 	tids, ok := info.UncommitTxn[groupName]
// 	if !ok {
// 		return errors.New("Uncommit Entry Not Exist")
// 	}
// 	for i, uncommitTid := range tids {
// 		if uncommitTid == tid {
// 			tids = append(tids[:i], tids[i+1:]...)
// 			if len(tids) == 0 {
// 				delete(info.UncommitTxn, groupName)
// 			} else {
// 				info.UncommitTxn[groupName] = tids
// 			}
// 			bitMap, ok := info.TxnCommit[groupName]
// 			if !ok {
// 				bitMap = &roaring64.Bitmap{}
// 			}
// 			bitMap.Add(commitId)
// 			info.TxnCommit[groupName] = bitMap
// 			return nil
// 		}
// 	}
// 	return errors.New("Uncommit Entry Not Exist")
// }
func (info *vInfo) MergeTidCidMap(tidCidMap map[string]map[uint64]uint64) {
	for group, infoMap := range info.TidCidMap {
		gMap, ok := tidCidMap[group]
		if !ok {
			gMap = make(map[uint64]uint64)
		}
		for tid, cid := range infoMap {
			gMap[tid] = cid
		}
		tidCidMap[group] = gMap
	}
}
func (info *vInfo) InTxnCommits(tidCidMap map[string]map[uint64]uint64, intervals map[string]*common.ClosedInterval) bool {
	for group, tids := range info.UncommitTxn {
		tidMap, ok := tidCidMap[group]
		if !ok {
			return false
		}
		interval, ok := intervals[group]
		if !ok {
			return false
		}
		for _, tid := range tids {
			cid, ok := tidMap[tid]
			if !ok {
				return false
			}
			if !interval.Contains(common.ClosedInterval{Start: cid, End: cid}) {
				return false
			}
		}
	}
	return true
}
func (info *vInfo) MetatoBuf() []byte {
	buf, _ := json.Marshal(info)
	return buf
}

func (info *vInfo) GetCommits(groupName string) (commits common.ClosedInterval) {
	commits = *info.Commits[groupName]
	return commits
}

func (info *vInfo) GetCheckpoints(groupName string) (checkpoint []common.ClosedInterval) {
	checkpoint = make([]common.ClosedInterval, 0)
	for _, interval := range info.Checkpoints[groupName] {
		checkpoint = append(checkpoint, *interval)
	}
	return checkpoint
}

func (info *vInfo) String() string {
	s := "("
	groups := make(map[string]struct{})
	for group := range info.Commits {
		groups[group] = struct{}{}
	}
	for group := range info.Checkpoints {
		groups[group] = struct{}{}
	}
	for group := range info.UncommitTxn {
		groups[group] = struct{}{}
	}
	for group := range info.TidCidMap {
		groups[group] = struct{}{}
	}
	for group := range groups {
		s = fmt.Sprintf("%s<%s>-[", s, group)

		commit, ok := info.Commits[group]
		if ok {
			s = fmt.Sprintf("%s%s|", s, commit.String())
		} else {
			s = fmt.Sprintf("%sNone|", s)
		}

		ckps, ok := info.Checkpoints[group]
		if ok {
			for _, ckp := range ckps {
				s = fmt.Sprintf("%s%s", s, ckp.String())
			}
			s = fmt.Sprintf("%s\n", s)
		} else {
			s = fmt.Sprintf("%sNone\n", s)
		}

		uncommits, ok := info.UncommitTxn[group]
		if ok {
			s = fmt.Sprintf("%s %v\n", s, uncommits)
		} else {
			s = fmt.Sprintf("%sNone\n", s)
		}

		tidcid, ok := info.TidCidMap[group]
		if ok {
			for tid, cid := range tidcid {
				s = fmt.Sprintf("%s %v-%v,", s, tid, cid)
			}
			s = fmt.Sprintf("%s]\n", s)
		} else {
			s = fmt.Sprintf("%sNone]\n", s)
		}
	}
	s = fmt.Sprintf("%s)", s)
	return s
}

func (info *vInfo) Log(v interface{}) error {
	if v == nil {
		return nil
	}
	switch vi := v.(type) {
	case *entry.CommitInfo:
		return info.LogCommit(vi)
	case *entry.CheckpointInfo:
		return info.LogCheckpoint(vi)
	case *entry.UncommitInfo:
		return info.LogUncommitInfo(vi)
	case *entry.TxnInfo:
		return info.LogTxnInfo(vi)
	}
	panic("not supported")
}

func (info *vInfo) LogTxnInfo(txnInfo *entry.TxnInfo) error {
	tidMap, ok := info.TidCidMap[txnInfo.Group]
	if !ok {
		tidMap = make(map[uint64]uint64)
	}
	tidMap[txnInfo.Tid] = txnInfo.CommitId
	info.TidCidMap[txnInfo.Group] = tidMap

	_, ok = info.Commits[txnInfo.Group]
	if !ok {
		info.Commits[txnInfo.Group] = &common.ClosedInterval{}
	}
	return info.Commits[txnInfo.Group].Append(txnInfo.CommitId)
}

func (info *vInfo) LogUncommitInfo(uncommitInfo *entry.UncommitInfo) error {
	for group, tids := range uncommitInfo.Tids {
		for _, tid := range tids {
			tids, ok := info.UncommitTxn[group]
			if !ok {
				tids = make([]uint64, 0)
				info.UncommitTxn[group] = tids
			}
			for _, infoTid := range tids {
				if infoTid == tid {
					return nil
				}
			}
			tids = append(tids, tid)
			info.UncommitTxn[group] = tids
		}
	}
	return nil
}

func (info *vInfo) LogCommit(commitInfo *entry.CommitInfo) error {
	_, ok := info.Commits[commitInfo.Group]
	if !ok {
		info.Commits[commitInfo.Group] = &common.ClosedInterval{}
	}
	return info.Commits[commitInfo.Group].Append(commitInfo.CommitId)
}

func (info *vInfo) LogCheckpoint(checkpointInfo *entry.CheckpointInfo) error {
	ckps, ok := info.Checkpoints[checkpointInfo.Group]
	if !ok {
		ckps = make([]*common.ClosedInterval, 0)
		ckps = append(ckps, checkpointInfo.Checkpoint)
		info.Checkpoints[checkpointInfo.Group] = ckps
		return nil
	}
	if len(ckps) == 0 {
		ckps = append(ckps, checkpointInfo.Checkpoint)
		info.Checkpoints[checkpointInfo.Group] = ckps
		return nil
	}
	ok = ckps[len(ckps)-1].TryMerge(*checkpointInfo.Checkpoint)
	if !ok {
		ckps = append(ckps, checkpointInfo.Checkpoint)
	}
	info.Checkpoints[checkpointInfo.Group] = ckps
	return nil
}
