package sm

import (
	"github.com/jiangxinmeng1/logstore/pkg/common"
	"github.com/jiangxinmeng1/logstore/pkg/entry"
	"github.com/jiangxinmeng1/logstore/pkg/store"
)

type OpT = entry.Type
type driver = store.Store

const (
	TCreateTable OpT = iota + entry.ETCustomizedStart
	TDropTable
	TInsert
	TDelete
)

type Wal struct {
	driver
	idAlloc common.IdAllocator
}

func newWal(dir, name string, cfg *store.StoreCfg) (*Wal, error) {
	dirver, err := store.NewBaseStore(dir, name, cfg)
	if err != nil {
		return nil, err
	}
	return &Wal{driver: dirver}, nil
}

func (wal *Wal) PrepareLog(op OpT, item []byte) (entry.Entry, error) {
	var err error
	e := entry.GetBase()
	e.SetType(op)
	e.SetPayloadSize(len(item))
	e.Unmarshal(item)
	id := wal.idAlloc.Alloc()
	info:=&entry.CommitInfo{
		Group: 1,
		CommitId: id,
	}
	e.SetInfo(info)
	err = wal.AppendEntry(e)
	return e, err
}

func (wal *Wal) SyncLog(op OpT, item []byte) error {
	e, err := wal.PrepareLog(op, item)
	if err != nil {
		return err
	}
	err = e.WaitDone()
	item = e.GetPayload()
	e.Free()
	return err
}
