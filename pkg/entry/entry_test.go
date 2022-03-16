package entry

import (
	"bytes"
	"testing"
	"time"

	"github.com/jiangxinmeng1/logstore/pkg/common"
	"github.com/stretchr/testify/assert"
)

func TestBase(t *testing.T) {
	now := time.Now()
	var buffer bytes.Buffer
	buffer.WriteString("helloworld")
	buffer.WriteString("helloworld")
	buffer.WriteString("helloworld")
	buf := buffer.Bytes()
	for i := 0; i < 100000; i++ {
		e := GetBase()
		e.SetType(ETFlush)
		n := common.GPool.Alloc(30)
		copy(n.GetBuf(), buf)
		e.UnmarshalFromNode(n, true)
		e.Free()
	}
	t.Logf("takes %s", time.Since(now))
	t.Log(common.GPool.String())
}

func TestInfo(t *testing.T) {
	info := &Info{
		Group:    GTCKp,
		CommitId: 20,
		TxnId:    35,
		Checkpoints: []CkpRanges{
			{
				Group: GTNoop,
				Ranges: &common.ClosedIntervals{
					Intervals: []*common.ClosedInterval{{Start: 1, End: 5}, {Start: 7, End: 7}}}},
			{
				Group: GTUncommit,
				Ranges: &common.ClosedIntervals{
					Intervals: []*common.ClosedInterval{{Start: 3, End: 5}, {Start: 6, End: 7}}}}},
		Uncommits: []Tid{{
			Group: GTCKp,
			Tid: 12,
		}, {
			Group: GTInvalid,
			Tid: 25,
		}},
		GroupLSN: 1,
	}
	buf:=info.Marshal()
	info2:=Unmarshal(buf)
	assert.Equal(t,info.Group,info2.Group)
}
