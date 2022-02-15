package store

import (
	"fmt"
	"logstore/pkg/common"
	"logstore/pkg/entry"
)

type vInfo struct {
	commits     map[string]*common.ClosedInterval
	checkpoints map[string][]*common.ClosedInterval
}

func newVInfo() *vInfo {
	return &vInfo{
		commits:     make(map[string]*common.ClosedInterval),
		checkpoints: make(map[string][]*common.ClosedInterval),
	}
}

func (info *vInfo) String() string {
	s := "("
	for group, commit := range info.commits {
		s = fmt.Sprintf("%s<%s>-[%s|", s, group, commit.String())
		ckps := info.checkpoints[group]
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
	_, ok := info.commits[commitInfo.Group]
	if !ok {
		info.commits[commitInfo.Group] = &common.ClosedInterval{}
	}
	return info.commits[commitInfo.Group].Append(commitInfo.CommitId)
}

func (info *vInfo) LogCheckpoint(checkpointInfo *entry.CheckpointInfo) error {
	ckps, ok := info.checkpoints[checkpointInfo.Group]
	if !ok {
		ckps = make([]*common.ClosedInterval, 0)
		info.checkpoints[checkpointInfo.Group] = ckps
		return nil
	}
	if len(ckps) == 0 {
		ckps = append(ckps, checkpointInfo.Checkpoint)
		info.checkpoints[checkpointInfo.Group] = ckps
		return nil
	}
	ok = ckps[len(ckps)-1].TryMerge(*checkpointInfo.Checkpoint)
	if !ok {
		ckps = append(ckps, checkpointInfo.Checkpoint)
	}
	info.checkpoints[checkpointInfo.Group] = ckps
	return nil
}
