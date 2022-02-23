package store

import (
	"context"
	"errors"
	"fmt"
	"github.com/jiangxinmeng1/logstore/pkg/common"
	"io/ioutil"
	"os"
	"path"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
)

var suffix = ".rot"

func MakeVersionFile(dir, name string, version uint64) string {
	return fmt.Sprintf("%s-%d%s", filepath.Join(dir, name), version, suffix)
}

func ParseVersion(name, prefix, suffix string) (int, error) {
	woPrefix := strings.TrimPrefix(name, prefix+"-")
	if len(woPrefix) == len(name) {
		return 0, errors.New("parse version error")
	}
	strVersion := strings.TrimSuffix(woPrefix, suffix)
	if len(strVersion) == len(woPrefix) {
		return 0, errors.New("parse version error")
	}
	v, err := strconv.Atoi(strVersion)
	if err != nil {
		return 0, errors.New("parse version error")
	}
	return v, nil
}

type files struct {
	files []*vFile
}

type rotateFile struct {
	*sync.RWMutex
	dir, name   string
	checker     RotateChecker
	uncommitted []*vFile
	history     History
	commitMu    sync.RWMutex

	commitWg     sync.WaitGroup
	commitCtx    context.Context
	commitCancel context.CancelFunc
	commitQueue  chan *vFile

	nextVer uint64
	idAlloc common.IdAllocator

	wg sync.WaitGroup
}

func OpenRotateFile(dir, name string, mu *sync.RWMutex, rotateChecker RotateChecker,
	historyFactory HistoryFactory) (*rotateFile, error) {
	var err error
	if mu == nil {
		mu = new(sync.RWMutex)
	}
	newDir := false
	if _, err = os.Stat(dir); os.IsNotExist(err) {
		err = os.MkdirAll(dir, 0755)
		if err != nil {
			return nil, err
		}
		newDir = true
	}

	if rotateChecker == nil {
		rotateChecker = NewMaxSizeRotateChecker(DefaultRotateCheckerMaxSize)
	}
	if historyFactory == nil {
		historyFactory = DefaultHistoryFactory
	}

	rf := &rotateFile{
		RWMutex:     mu,
		dir:         dir,
		name:        name,
		uncommitted: make([]*vFile, 0),
		checker:     rotateChecker,
		commitQueue: make(chan *vFile, 10000),
		history:     historyFactory(),
	}
	if !newDir {
		files, err := ioutil.ReadDir(dir)
		if err != nil {
			return nil, err
		}
		vfiles := make([]VFile, 0)
		for _, f := range files {
			version, err := ParseVersion(f.Name(), rf.name, suffix)
			if err != nil {
				fmt.Printf("err is %v\n", err)
				continue
			}
			file, err := os.OpenFile(path.Join(dir, f.Name()), os.O_RDWR|os.O_APPEND, os.ModePerm)
			if err != nil {
				return nil, err
			}
			vf := &vFile{
				vInfo:      *newVInfo(),
				RWMutex:    &sync.RWMutex{}, //?
				File:       file,
				version:    version,
				commitCond: *sync.NewCond(new(sync.Mutex)),
				history:    rf.history,
				size:       int(f.Size()),
			}
			vf.ReadMeta(vf)
			vfiles = append(vfiles, vf)
		}
		sort.Slice(vfiles, func(i, j int) bool {
			return vfiles[i].(*vFile).version < vfiles[j].(*vFile).version
		})
		if len(vfiles) == 0 {
			err = rf.scheduleNew()
			if err != nil {
				return nil, err
			}
		} else {
			rf.history.Extend(vfiles[:len(vfiles)-1]...)
			rf.uncommitted = append(rf.uncommitted, vfiles[len(vfiles)-1].(*vFile))
		}
	} else {
		err = rf.scheduleNew()
	}
	rf.commitCtx, rf.commitCancel = context.WithCancel(context.Background())
	rf.wg.Add(1)
	go rf.commitLoop()
	return rf, err
}
func (rf *rotateFile) Replay(r ReplayHandle, o ReplayObserver) error {
	rf.history.Replay(r, o)
	for _, vf := range rf.uncommitted { //sequence?
		vf.Replay(r, o)
	}
	return nil
}
func (rf *rotateFile) commitLoop() {
	defer rf.wg.Done()
	for {
		select {
		case <-rf.commitCtx.Done():
			return
		case file := <-rf.commitQueue:
			// fmt.Printf("Receive request: %s\n", file.Name())
			file.Commit()
			rf.commitFile()
			rf.commitWg.Done()
		}
	}
}

func (rf *rotateFile) scheduleCommit(file *vFile) {
	rf.commitWg.Add(1)
	// fmt.Printf("Schedule request: %s\n", file.Name())
	rf.commitQueue <- file
}

func (rf *rotateFile) GetHistory() History {
	return rf.history
}

func (rf *rotateFile) Close() error {
	rf.commitWg.Wait()
	rf.commitCancel()
	rf.wg.Wait()
	// logrus.Info(rf.GetHistory().String())
	for _, vf := range rf.uncommitted {
		vf.Close()
	}
	return nil
}

func (rf *rotateFile) scheduleNew() error {
	fname := MakeVersionFile(rf.dir, rf.name, rf.nextVer)
	rf.nextVer++
	vf, err := newVFile(nil, fname, int(rf.nextVer), rf.history)
	if err != nil {
		return err
	}
	rf.uncommitted = append(rf.uncommitted, vf)
	return nil
}

func (rf *rotateFile) getFileState() *vFileState {
	l := len(rf.uncommitted)
	if l == 0 {
		return nil
	}
	return rf.uncommitted[l-1].GetState()
}

func (rf *rotateFile) makeSpace(size int) (rotated *vFile, curr *vFileState, err error) {
	var (
		rotNeeded bool
	)
	l := len(rf.uncommitted)
	if l == 0 {
		rotNeeded, err = rf.checker.PrepareAppend(nil, size)
	} else {
		rotNeeded, err = rf.checker.PrepareAppend(rf.uncommitted[l-1], size)
	}
	if err != nil {
		return nil, nil, err
	}
	if l == 0 || rotNeeded {
		if rotNeeded {
			rotated = rf.uncommitted[l-1]
			rf.scheduleCommit(rotated)
		}
		if err = rf.scheduleNew(); err != nil {
			return nil, nil, err
		}
	}
	curr = rf.getFileState()
	curr.file.PrepareWrite(size)
	return rotated, curr, nil
}

func (rf *rotateFile) GetAppender() FileAppender {
	return newFileAppender(rf)
}

func (rf *rotateFile) commitFile() {
	rf.Lock()
	f := rf.uncommitted[0]
	if !f.HasCommitted() {
		panic("logic error")
	}
	rf.uncommitted = rf.uncommitted[1:]
	f.Archive()
	rf.Unlock()
	fmt.Printf("Committed %s\n", f.Name())
}

func (rf *rotateFile) Sync() error {
	rf.RLock()
	if len(rf.uncommitted) == 0 {
		rf.RUnlock()
		return nil
	}
	if len(rf.uncommitted) == 1 {
		f := rf.uncommitted[0]
		rf.RUnlock()
		return f.Sync()
	}
	lastFile := rf.uncommitted[len(rf.uncommitted)-1]
	waitFile := rf.uncommitted[len(rf.uncommitted)-2]
	rf.RUnlock()
	waitFile.WaitCommitted()
	return lastFile.Sync()
}
