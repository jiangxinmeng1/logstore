package store

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"sync"
	"sync/atomic"

	"github.com/jiangxinmeng1/logstore/pkg/common"
	"github.com/jiangxinmeng1/logstore/pkg/entry"

	log "github.com/sirupsen/logrus"
)

var Metasize = 2
var DefaultBufSize = common.M

type vFileState struct {
	bufPos  int
	bufSize int
	pos     int
	file    *vFile
}

type vFile struct {
	*sync.RWMutex
	*os.File
	vInfo
	version    int
	committed  int32
	size       int
	wg         sync.WaitGroup
	commitCond sync.Cond
	history    History
	buf        []byte
	syncpos    int
	bufpos     int
	bufSize    int
}

func newVFile(mu *sync.RWMutex, name string, version int, history History) (*vFile, error) {
	if mu == nil {
		mu = new(sync.RWMutex)
	}
	file, err := os.Create(name)
	if err != nil {
		return nil, err
	}

	return &vFile{
		vInfo:      *newVInfo(),
		RWMutex:    mu,
		File:       file,
		version:    version,
		commitCond: *sync.NewCond(new(sync.Mutex)),
		history:    history,
		buf:        make([]byte, DefaultBufSize),
		bufSize:    int(DefaultBufSize),
	}, nil
}

func (vf *vFile) InCommits(intervals map[uint32]*common.ClosedIntervals) bool {
	for group, commits := range vf.Commits {
		interval, ok := intervals[group]
		if !ok {
			return false
		}
		if !interval.ContainsInterval(*commits) {
			return false
		}
	}
	return true
}

func (vf *vFile) InCheckpoint(intervals map[uint32]*common.ClosedIntervals) bool {
	for group, ckps := range vf.Checkpoints {
		interval, ok := intervals[group]
		if !ok {
			return false
		}
		for _, ckp := range ckps.Intervals {
			if !interval.ContainsInterval(*ckp) {
				return false
			}
		}
	}
	return true
}

// TODO: process multi checkpoints.
func (vf *vFile) MergeCheckpoint(interval map[uint32]*common.ClosedIntervals) {
	if len(vf.Checkpoints) == 0 {
		return
	}
	if interval == nil {
		ret := make(map[uint32]*common.ClosedIntervals)
		for group, ckps := range vf.Checkpoints {
			ret[group] = common.NewClosedIntervalsByIntervals(ckps)
		}
		interval = ret
		return
	}
	for group, ckps := range vf.Checkpoints {
		if len(ckps.Intervals) == 0 {
			continue
		}
		_, ok := interval[group]
		if !ok {
			interval[group] = &common.ClosedIntervals{}
		}
		interval[group].TryMerge(*ckps)
	}
}

func (vf *vFile) String() string {
	var w bytes.Buffer
	w.WriteString(fmt.Sprintf("[%s]\n%s", vf.Name(), vf.vInfo.String()))
	return w.String()
}

func (vf *vFile) Archive() error {
	if vf.history == nil {
		if err := vf.Destroy(); err != nil {
			return err
		}
	}
	vf.history.Append(vf)
	return nil
}

func (vf *vFile) Id() int {
	return vf.version
}

func (vf *vFile) GetState() *vFileState {
	vf.RLock()
	defer vf.RUnlock()
	return &vFileState{
		bufPos:  vf.bufpos,
		bufSize: vf.bufSize,
		pos:     vf.size,
		file:    vf,
	}
}

func (vf *vFile) HasCommitted() bool {
	return atomic.LoadInt32(&vf.committed) == int32(1)
}

func (vf *vFile) PrepareWrite(size int) {
	// fmt.Printf("PrepareWrite %s\n", vf.Name())
	vf.wg.Add(1)
	vf.size += size
	// fmt.Printf("\n%p|prepare write %d->%d\n", vf, vf.size-size, vf.size)
}

func (vf *vFile) FinishWrite() {
	// fmt.Printf("FinishWrite %s\n", vf.Name())
	vf.wg.Done()
}

func (vf *vFile) Commit() {
	// fmt.Printf("Committing %s\n", vf.Name())
	vf.wg.Wait()
	vf.WriteMeta()
	vf.Sync()
	fmt.Printf("sync-%s\n", vf.String())
	vf.commitCond.L.Lock()
	atomic.StoreInt32(&vf.committed, int32(1))
	vf.commitCond.Broadcast()
	vf.commitCond.L.Unlock()
}

func (vf *vFile) Sync() error {
	_, err := vf.File.WriteAt(vf.buf[:vf.bufpos], int64(vf.syncpos))
	if err != nil {
		return err
	}
	vf.File.Sync()
	// fmt.Printf("%p|sync [%v,%v](total%v|n=%d)\n", vf, vf.syncpos, vf.syncpos+vf.bufpos, vf.bufpos,n)
	buf := make([]byte, 10)
	_, err = vf.ReadAt(buf, int64(vf.syncpos))
	// fmt.Printf("%p|read at %v, buf is %v, n=%d, err is %v\n", vf, vf.syncpos, buf, n, err)
	vf.syncpos += vf.bufpos
	if vf.syncpos != vf.size {
		panic(fmt.Sprintf("%p|logic error, sync %v, size %v", vf, vf.syncpos, vf.size))
	}
	vf.bufpos = 0
	return nil
}

func (vf *vFile) WriteMeta() {
	buf := vf.MetatoBuf()
	n, _ := vf.WriteAt(buf, int64(vf.size))
	vf.size += n
	buf = make([]byte, Metasize)
	binary.BigEndian.PutUint16(buf, uint16(n))
	n, _ = vf.WriteAt(buf, int64(vf.size))
	vf.size += n
}

func (vf *vFile) WaitCommitted() {
	if atomic.LoadInt32(&vf.committed) == int32(1) {
		return
	}
	vf.commitCond.L.Lock()
	if atomic.LoadInt32(&vf.committed) != int32(1) {
		vf.commitCond.Wait()
	}
	vf.commitCond.L.Unlock()
}

func (vf *vFile) WriteAt(b []byte, off int64) (n int, err error) {
	// n, err = vf.File.WriteAt(b, off)
	n = copy(vf.buf[int(off)-vf.syncpos:], b)
	// fmt.Printf("%p|write in buf[%v,%v]\n", vf, int(off)-vf.syncpos, int(off)-vf.syncpos+n)
	// fmt.Printf("%p|write vf in buf [%v,%v]\n", vf, int(off), int(off)+n)
	vf.bufpos = int(off) + n - vf.syncpos
	if err != nil {
		return
	}
	return
}

func (vf *vFile) Write(b []byte) (n int, err error) {
	n, err = vf.File.Write(b)
	if err != nil {
		return
	}
	return
}

func (vf *vFile) SizeLocked() int {
	return vf.size
}

func (vf *vFile) Destroy() error {
	if err := vf.Close(); err != nil {
		return err
	}
	name := vf.Name()
	log.Infof("Removing version file: %s", name)
	err := os.Remove(name)
	return err
}

func (vf *vFile) Replay(handle ReplayHandle, observer ReplayObserver) error {
	observer.OnNewEntry(vf.Id())
	for {
		if err := handle(vf, vf); err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			return err
		}
	}
	return nil
}

func (vf *vFile) OnNewEntry(int) {}
func (vf *vFile) OnNewCommit(info *entry.Info) {
	vf.Log(info)
}
func (vf *vFile) OnNewCheckpoint(info *entry.Info) {
	vf.Log(info)
}
func (vf *vFile) OnNewTxn(info *entry.Info) {
	vf.Log(info)
}
func (vf *vFile) OnNewUncommit(addrs []*VFileAddress) {
	for _, addr := range addrs {
		exist := false
		tids, ok := vf.UncommitTxn[addr.Group]
		if !ok {
			tids = make([]uint64, 0)
		}
		for _, tid := range tids {
			if tid == addr.LSN {
				exist = true
			}
		}
		if !exist {
			tids = append(tids, addr.LSN)
			vf.UncommitTxn[addr.Group] = tids
		}
	}
}

func (vf *vFile) Load(groupId uint32, lsn uint64) (entry.Entry, error) {
	offset, err := vf.GetOffsetByLSN(groupId, lsn)
	if err != nil {
		return nil, err
	}
	entry := entry.GetBase()
	metaBuf := entry.GetMetaBuf()
	_, err = vf.ReadAt(metaBuf, int64(offset))
	// fmt.Printf("%p|read meta [%v,%v]\n", vf, offset, offset+n)
	if err != nil {
		return nil, err
	}
	_, err = entry.ReadAt(vf.File, offset)
	return entry, err
}
