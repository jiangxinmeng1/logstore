package store

import (
	"bytes"
	"io/ioutil"
	"logstore/pkg/common"
	"logstore/pkg/entry"
	"os"
	"path"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/panjf2000/ants/v2"

	"github.com/stretchr/testify/assert"
)

func TestVFile(t *testing.T) {
	dir := "/tmp/testvfile"
	os.RemoveAll(dir)
	name := "mock"
	os.MkdirAll(dir, 0755)
	v0, err := newVFile(nil, MakeVersionFile(dir, name, 0), 0, nil)
	assert.Nil(t, err)
	var wg sync.WaitGroup
	wg.Add(1)
	var bs bytes.Buffer
	toWrite := "helloworld"
	for i := 0; i < 10; i++ {
		bs.WriteString(toWrite)
	}
	buf := bs.Bytes()
	val := int32(1)
	go func() {
		defer wg.Done()
		v0.PrepareWrite(len(buf))
		_, err := v0.Write(buf)
		assert.Nil(t, err)
		v0.FinishWrite()
		t.Logf("committing v0")
		v0.Commit()
		assert.True(t, atomic.CompareAndSwapInt32(&val, int32(1), int32(2)))
		t.Logf("committed v0")
	}()

	t.Logf("waiting v0 commit")
	assert.Equal(t, int32(1), atomic.LoadInt32(&val))
	v0.WaitCommitted()
	assert.Equal(t, int32(2), atomic.LoadInt32(&val))
	t.Logf("WaitDone")

	wg.Wait()
}

func TestAppender(t *testing.T) {
	dir := "/tmp/testappender"
	os.RemoveAll(dir)
	name := "mock"
	checker := &MaxSizeRotateChecker{
		MaxSize: int(common.M) * 1,
	}
	rf, err := OpenRotateFile(dir, name, nil, checker, nil)
	assert.Nil(t, err)
	defer rf.Close()

	var data bytes.Buffer
	data.WriteString("helloworldhello1")
	for i := 0; i < 32*2048-1; i++ {
		data.WriteString("helloworldhello1")
	}
	toWrite := data.Bytes()

	worker, _ := ants.NewPool(1)
	pool, _ := ants.NewPool(5)
	var wg sync.WaitGroup

	now := time.Now()
	total := 10
	for i := 0; i < total; i++ {
		// ff, _ := os.Create(fmt.Sprintf("/tmp/testappender/xxx%d", i))
		// defer ff.Close()
		// fff := func(file *os.File) func() {
		// 	return func() {
		// 		defer wg.Done()
		// 		now := time.Now()
		// 		t.Logf("%s started %s", file.Name(), now)
		// 		ff.Write(toWrite)
		// 		file.Sync()
		// 		t.Logf("%s takes %s", file.Name(), time.Since(now))
		// 	}
		// }
		// wg.Add(1)
		// pool.Submit(fff(ff))
		// continue
		appender := rf.GetAppender()
		assert.NotNil(t, appender)
		if i%4 == 0 && i > 0 {
			checkpointInfo := &entry.CheckpointInfo{
				Group: "group1",
				Checkpoint: &common.ClosedInterval{
					End: common.GetGlobalSeqNum(),
				},
			}
			err = appender.Prepare(len(toWrite), checkpointInfo)
			assert.Nil(t, err)
		} else {
			commitInfo := &entry.CommitInfo{
				Group:    "group1",
				CommitId: common.NextGlobalSeqNum(),
			}
			err = appender.Prepare(len(toWrite), commitInfo)
			assert.Nil(t, err)
		}

		f := func(app FileAppender, idx int) func() {
			return func() {
				defer wg.Done()
				appender := app.(*fileAppender)
				now := time.Now()
				t.Logf("%s started %s", appender.rollbackState.file.Name(), now)
				_, err := app.Write(toWrite)
				assert.Nil(t, err)
				err = app.Commit()
				assert.Nil(t, err)
				app.Sync()
				t.Logf("[%s] takes %s", appender.rollbackState.file.Name(), time.Since(now))
				assert.Nil(t, err)
				truncate := func() {
					defer wg.Done()
					rf.history.TryTruncate()
				}
				wg.Add(1)
				worker.Submit(truncate)
			}
		}
		wg.Add(1)
		pool.Submit(f(appender, i))
	}
	wg.Wait()
	t.Logf("1. %s", time.Since(now))
	t.Log(rf.history.String())
}

func TestVInfo(t *testing.T) {
	vinfo := *newVInfo()
	end := 10
	for i := 0; i <= end; i++ {
		commitInfo := &entry.CommitInfo{Group: "group1", CommitId: uint64(i)}
		err := vinfo.LogCommit(commitInfo)
		assert.Nil(t, err)
	}
	assert.Equal(t, uint64(end), vinfo.Commits["group1"].End)
	commitInfo := &entry.CommitInfo{Group: "group1", CommitId: uint64(end + 2)}
	err := vinfo.LogCommit(commitInfo)
	assert.NotNil(t, err)

	checkpointInfo := &entry.CheckpointInfo{
		Group: "group1",
		Checkpoint: &common.ClosedInterval{
			Start: 0,
			End:   uint64(end / 2),
		},
	}
	err = vinfo.LogCheckpoint(checkpointInfo)
	assert.Nil(t, err)
}

func TestReadVInfo(t *testing.T){
	dir := "/tmp/testappender"
	os.RemoveAll(dir)
	name := "mock"
	checker := &MaxSizeRotateChecker{
		MaxSize: int(common.M) * 1,
	}
	rf, _ := OpenRotateFile(dir, name, nil, checker, nil)

	var data bytes.Buffer
	data.WriteString("helloworldhello1")
	for i := 0; i < 32*2048-1; i++ {
		data.WriteString("helloworldhello1")
	}
	toWrite := data.Bytes()

	worker, _ := ants.NewPool(1)
	pool, _ := ants.NewPool(5)
	var wg sync.WaitGroup

	total := 10
	for i := 0; i < total; i++ {
		appender := rf.GetAppender()
		assert.NotNil(t, appender)
		if i%4 == 0 && i > 0 {
			checkpointInfo := &entry.CheckpointInfo{
				Group: "group1",
				Checkpoint: &common.ClosedInterval{
					End: common.GetGlobalSeqNum(),
				},
			}
			appender.Prepare(len(toWrite), checkpointInfo)
		} else {
			commitInfo := &entry.CommitInfo{
				Group:    "group1",
				CommitId: common.NextGlobalSeqNum(),
			}
			appender.Prepare(len(toWrite), commitInfo)
		}

		f := func(app FileAppender, idx int) func() {
			return func() {
				defer wg.Done()
				app.Write(toWrite)
				app.Commit()
				app.Sync()
				truncate := func() {
					defer wg.Done()
					rf.history.TryTruncate()
				}
				wg.Add(1)
				worker.Submit(truncate)
			}
		}
		wg.Add(1)
		pool.Submit(f(appender, i))
	}
	wg.Wait()

	rf.Close()

	
	
	files, _ := ioutil.ReadDir(dir)
	for _,file:=range files{
		f, _ := os.OpenFile(path.Join(dir, file.Name()), os.O_RDWR|os.O_APPEND, os.ModePerm)
		vf:=&vFile{
			vInfo: *newVInfo(),
			File: f,
			size: int(file.Size()),
		}
		vf.ReadMeta(vf)
	}

}