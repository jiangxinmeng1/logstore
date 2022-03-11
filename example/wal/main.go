package main

import (
	"bytes"
	"fmt"
	"os"
	"sync"
	"time"

	"github.com/jiangxinmeng1/logstore/example/wal/sm"
	"github.com/jiangxinmeng1/logstore/pkg/common"
	"github.com/jiangxinmeng1/logstore/pkg/store"

	"github.com/panjf2000/ants/v2"
	log "github.com/sirupsen/logrus"

	"net/http"
	_ "net/http/pprof"
	"runtime/pprof"
	// "runtime/trace"
)

func main() {
	// trace.Sta

	go func() {
        if err := http.ListenAndServe(":6060", nil); err != nil {
            log.Fatal(err)
        }
        os.Exit(0)
    }()

	dir := "/tmp/walexample"
	os.RemoveAll(dir)
	checker := store.NewMaxSizeRotateChecker(int(common.M) * 64)
	cfg := &store.StoreCfg{
		RotateChecker: checker,
	}
	machine, err := sm.NewStateMachine(dir, cfg)
	if err != nil {
		panic(err)
	}
	var wg sync.WaitGroup
	pool, _ := ants.NewPool(10000)

	f,_:=os.Create("./cpuProf")
	pprof.StartCPUProfile(f)
	defer pprof.StopCPUProfile()

	lsn := uint64(0)
	now := time.Now()
	for i := 0; i < 1000000; i++ {
		insert := func() {
			defer wg.Done()
			var bs bytes.Buffer
			i := common.NextGlobalSeqNum()
			bs.WriteString(fmt.Sprintf("request-%d", i))
			// fmt.Printf("bs is %s\n",bs.Bytes())
			r := &sm.Request{
				Op:   sm.TInsert,
				Data: bs.Bytes(),
			}
			if err := machine.OnRequest(r); err != nil {
				panic(err)
			}
			bs.Reset()
		}
		wg.Add(1)
		pool.Submit(insert)
		currLsn := machine.VisibleLSN()
		if currLsn != lsn {
			log.Infof("VisibleLSN %d, duration %s", currLsn, time.Since(now))
			lsn = currLsn
		}
	}

	wg.Wait()
	machine.Close()
	log.Infof("It takes %s", time.Since(now))
	currLsn := machine.VisibleLSN()
	log.Infof("VisibleLSN %d", currLsn)
}
