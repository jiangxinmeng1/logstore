package store

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"logstore/pkg/common"
	"logstore/pkg/entry"
)

type vInfo struct {
	Commits     map[string]*common.ClosedInterval
	Checkpoints map[string][]*common.ClosedInterval
}

func newVInfo() *vInfo {
	return &vInfo{
		Commits:     make(map[string]*common.ClosedInterval),
		Checkpoints: make(map[string][]*common.ClosedInterval),
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
	for group := range groups {
		s = fmt.Sprintf("%s<%s>-[", s, group)
		commit, ok := info.Commits[group]
		if ok {
			s = fmt.Sprintf("%s%s|", s,commit.String())
		}else{
			s = fmt.Sprintf("%sNone|", s)
		}
		ckps := info.Checkpoints[group]
		for _, ckp := range ckps {
			s = fmt.Sprintf("%s%s", s, ckp.String())
		}
		s = fmt.Sprintf("%s]", s)
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
	}
	panic("not supported")
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
