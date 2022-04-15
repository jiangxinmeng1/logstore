package store

import (
	"fmt"

	"github.com/RoaringBitmap/roaring"
	"github.com/jiangxinmeng1/logstore/pkg/common"
	"github.com/jiangxinmeng1/logstore/pkg/entry"
)


type VGroup interface { //append(vfile) ckp(by what) compact/iscovered
	Log(interface{}) error
	OnCheckpoint(interface{}) //ckp info
	IsCovered(c *compactor) bool
	MergeCheckpointInfo(c *compactor) //only commit group

	IsCheckpointGroup() bool
	IsUncommitGroup() bool
	IsCommitGroup() bool

	String() string
}

type compactor struct {
	//tid-cid map
	//partial ckp
	//ckp ranges
	gIntervals map[uint32]*common.ClosedIntervals
	tidCidMap  map[uint32]map[uint64]uint64
}

func newCompactor()*compactor{
	return &compactor{
		gIntervals: make(map[uint32]*common.ClosedIntervals),
		tidCidMap: make(map[uint32]map[uint64]uint64),
	}
}

type baseGroup struct {
	groupType uint16
	groupId   uint32
	vInfo     *vInfo //get groupbyid
	// addrs     map[uint64]int //gid-lsn
}

func newbaseGroup(v *vInfo, gid uint32) *baseGroup {
	return &baseGroup{
		vInfo: v,
		groupId: gid,
	}
}
func (g *baseGroup) IsCovered(c *compactor)           {}
func (g *baseGroup) MergeCheckpointInfo(c *compactor) {}
func (g *baseGroup) String() string                   { return "" }

type commitGroup struct {
	*baseGroup
	ckps       *common.ClosedIntervals //ckpped
	Commits    *common.ClosedInterval
	tidCidMap  map[uint64]uint64
	partialCkp map[uint64]*partialCkpInfo
	//tid-{mask, commands-mask}
	//partial ckp
}
type partialCkpInfo struct {
	size uint32
	ckps *roaring.Bitmap
}

func newPartialCkpInfo() *partialCkpInfo {
	return &partialCkpInfo{
		ckps: &roaring.Bitmap{},
	}
}

// partitial ckp
// uncommit -> commit -> partitial ckp -> ckp
// log uncommit group -> commit group tid cid map -> partitial ckp entry -> calculate when compact
// log tid only | offset/size
func newcommitGroup(v *vInfo,gid uint32) *commitGroup {
	return &commitGroup{
		baseGroup: newbaseGroup(v,gid),
		ckps:      common.NewClosedIntervals(),
		// Commits:,
		tidCidMap:  make(map[uint64]uint64),
		partialCkp: make(map[uint64]*partialCkpInfo),
	}
}

func (g *commitGroup) String() string {
	s := fmt.Sprintf("Commit[ckp-%s,commits-%s,tc-{", g.ckps, g.Commits)
	for tid, cid := range g.tidCidMap {
		s = fmt.Sprintf("%s%d-%d,", s, tid, cid)
	}
	s = fmt.Sprintf("%s},partial-{", s)
	for tid, cmd := range g.partialCkp {
		s = fmt.Sprintf("%s%d-%v/%d,", s, tid, cmd.ckps, cmd.size)
	}
	s = fmt.Sprintf("%s}]", s)
	return s
}

func (g *commitGroup) Log(info interface{}) error {
	commitInfo := info.(*entry.Info)
	if g.Commits == nil {
		g.Commits = &common.ClosedInterval{}
	}
	err := g.Commits.Append(commitInfo.CommitId)
	g.tidCidMap[commitInfo.TxnId] = commitInfo.CommitId
	return err
}

func (g *commitGroup) IsCovered(c *compactor) bool {
	interval, ok := c.gIntervals[g.groupId]
	if !ok {
		return false
	}
	if g.Commits!=nil&&!interval.ContainsInterval(*g.Commits) {
		return false
	}
	return interval.Contains(*g.ckps)
}

func (g *commitGroup) IsCommitGroup() bool {
	return true
}

func (g *commitGroup) MergeCheckpointInfo(c *compactor) {
	//tid cid map
	gMap, ok := c.tidCidMap[g.groupId]
	if !ok {
		gMap = make(map[uint64]uint64)
	}
	for tid, cid := range g.tidCidMap {
		gMap[tid] = cid
	}
	c.tidCidMap[g.groupId] = gMap
	//merge partialckp
	for tid, commandsInfo := range g.partialCkp {
		if commandsInfo.ckps.GetCardinality() == uint64(commandsInfo.size) {
			g.ckps.TryMerge(*common.NewClosedIntervalsByInt(tid))
			delete(g.partialCkp, tid)
		}
	}
	//merge ckps
	if len(g.ckps.Intervals) == 0 {
		return
	}
	if c.gIntervals == nil {
		ret := make(map[uint32]*common.ClosedIntervals)
		ret[g.groupId] = common.NewClosedIntervalsByIntervals(g.ckps)
		c.gIntervals = ret
		return
	}
	_, ok = c.gIntervals[g.groupId]
	if !ok {
		c.gIntervals[g.groupId] = &common.ClosedIntervals{}
	}
	c.gIntervals[g.groupId].TryMerge(*g.ckps)
}

func (g *commitGroup) IsCheckpointGroup() bool {
	return false
}
func (g *commitGroup) IsUncommitGroup() bool {
	return false
}
func (g *commitGroup) OnCheckpoint(info interface{}) {
	ranges := info.(*entry.CkpRanges)
	g.ckps.TryMerge(*ranges.Ranges)
	for _, command := range ranges.Command {
		commandinfo, ok := g.partialCkp[command.Tid]
		if !ok {
			commandinfo = newPartialCkpInfo()
		}
		for _, cmd := range command.CommandIds {
			commandinfo.ckps.Add(cmd)
		}
		commandinfo.size = command.Size
		g.partialCkp[command.Tid] = commandinfo
	}
}

type uncommitGroup struct {
	*baseGroup
	//uncheckpointed gid-tids//-commands
	UncommitTxn map[uint32][]uint64
}

func newuncommitGroup(v *vInfo,gid uint32) *uncommitGroup {
	return &uncommitGroup{
		baseGroup:   newbaseGroup(v,gid),
		UncommitTxn: make(map[uint32][]uint64),
	}
}
func (g *uncommitGroup) String() string {
	s := "Uncommit["
	for gid, tids := range g.UncommitTxn {
		s = fmt.Sprintf("%s%d-%v", s, gid, tids)
	}
	s = fmt.Sprintf("%s]", s)
	return s
}
func (g *uncommitGroup) OnCheckpoint(interface{}) {} //calculate ckp when compact
func (g *uncommitGroup) IsCovered(c *compactor) bool {
	for group, tids := range g.UncommitTxn {
		tidMap, ok := c.tidCidMap[group]
		if !ok {
			return false
		}
		interval, ok := c.gIntervals[group]
		if !ok {
			return false
		}
		for _, tid := range tids {
			cid, ok := tidMap[tid]
			if !ok {
				return false
			}
			if !interval.ContainsInterval(common.ClosedInterval{Start: cid, End: cid}) {
				return false
			}
		}
	}
	return true
}

func (g *uncommitGroup) MergeCheckpointInfo(c *compactor) {
}

func (g *uncommitGroup) IsCheckpointGroup() bool {
	return false
}
func (g *uncommitGroup) IsUncommitGroup() bool {
	return true
}
func (g *uncommitGroup) IsCommitGroup() bool {
	return false
}
func (g *uncommitGroup) Log(info interface{}) error { //misuxi uncommit info stored in commitgroup
	uncommitInfo := info.(*entry.Info)
	for _, uncommit := range uncommitInfo.Uncommits {
		tids, ok := g.UncommitTxn[uncommit.Group]
		if !ok {
			tids = make([]uint64, 0)
			g.UncommitTxn[uncommit.Group] = tids
		}
		existed := false
		for _, infoTid := range tids {
			if infoTid == uncommit.Tid {
				existed = true
				return nil
			}
		}
		if !existed {
			tids = append(tids, uncommit.Tid)
			g.UncommitTxn[uncommit.Group] = tids
		}
	}
	return nil
}

type checkpointGroup struct {
	//gid,lsn ->
	*baseGroup
}

func newcheckpointGroup(v *vInfo,gid uint32) *checkpointGroup {
	return &checkpointGroup{
		baseGroup: newbaseGroup(v,gid),
	}
}
func (g *checkpointGroup) OnCheckpoint(interface{})         {  } //ckp info
func (g *checkpointGroup) IsCovered(c *compactor) bool      { return false }
func (g *checkpointGroup) MergeCheckpointInfo(c *compactor) { }
func (g *checkpointGroup) IsCheckpointGroup() bool {
	return true
}
func (g *checkpointGroup) IsCommitGroup() bool {
	return false
}
func (g *checkpointGroup) IsUncommitGroup() bool { return false }

func (g *checkpointGroup) Log(info interface{}) error {
	checkpointInfo := info.(*entry.Info)
	for _, interval := range checkpointInfo.Checkpoints {
		group := g.vInfo.getGroupById(interval.Group) //todo new group if not exist
		if group == nil {
			group = newcommitGroup(g.vInfo,interval.Group)
			g.vInfo.groups[interval.Group] = group
		}
		group.OnCheckpoint(&interval) //todo range -> ckp info
	}
	return nil
}
func (g *checkpointGroup) String() string {
	s := "Ckp"
	return s
}
