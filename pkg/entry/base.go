package entry

import (
	"fmt"
	"github.com/jiangxinmeng1/logstore/pkg/common"
	"io"
	"sync"
)

var (
	_basePool = sync.Pool{New: func() interface{} {
		return &Base{
			descriptor: newDescriptor(),
		}
	}}
)

type Base struct {
	*descriptor
	node    *common.MemNode
	payload []byte
	info    interface{}
	infobuf []byte
	wg      sync.WaitGroup
	err     error
}

type CheckpointInfo struct {
	CheckpointRanges map[uint32]*common.ClosedInterval
	// Group      string
	// Checkpoint *common.ClosedInterval
}

func (info *CheckpointInfo) ToString() string {
	s := "checkpoint entry "
	for group, checkpoint := range info.CheckpointRanges {
		s = fmt.Sprintf("%s <%d>-%s", s, group, checkpoint)
	}
	return fmt.Sprintf("%s\n", s)
}

type CommitInfo struct {
	Group    uint32
	CommitId uint64
}

func (info *CommitInfo) ToString() string {
	return fmt.Sprintf("commit entry <%d>-%d\n", info.Group, info.CommitId)
}

type UncommitInfo struct {
	Tids map[uint32][]uint64
	Addr interface{}
}

func (info *UncommitInfo) ToString() string {
	return fmt.Sprintf("uncommit entry %v\n", info.Tids)
}

type TxnInfo struct {
	Group    uint32
	Tid      uint64
	CommitId uint64
	Addr     interface{}
}

func (info *TxnInfo) ToString() string {
	return fmt.Sprintf("txn entry <%d> %d-%d\n", info.Group, info.Tid, info.CommitId)
}

func GetBase() *Base {
	b := _basePool.Get().(*Base)
	b.wg.Add(1)
	return b
}

func (b *Base) reset() {
	b.descriptor.reset()
	if b.node != nil {
		common.GPool.Free(b.node)
		b.node = nil
	}
	b.payload = nil
	b.info = nil
	b.wg = sync.WaitGroup{}
	b.err = nil
}

func (b *Base) GetInfoBuf() []byte {
	return b.infobuf
}
func (b *Base) SetInfoBuf(buf []byte) {
	b.infobuf = buf
}
func (b *Base) GetError() error {
	return b.err
}

func (b *Base) WaitDone() error {
	b.wg.Wait()
	return b.err
}

func (b *Base) DoneWithErr(err error) {
	b.err = err
	b.wg.Done()
}

func (b *Base) Free() {
	b.reset()
	_basePool.Put(b)
}

func (b *Base) GetPayload() []byte {
	if b.node != nil {
		return b.node.Buf[:b.GetPayloadSize()]
	}
	return b.payload
}

func (b *Base) SetInfo(info interface{}) {
	b.info = info
}

func (b *Base) GetInfo() interface{} {
	return b.info
}

func (b *Base) UnmarshalFromNode(n *common.MemNode, own bool) error {
	if b.node != nil {
		common.GPool.Free(b.node)
		b.node = nil
	}
	if own {
		b.node = n
		b.payload = b.node.GetBuf()
	} else {
		copy(b.payload, n.GetBuf())
	}
	b.SetPayloadSize(len(b.payload))
	return nil
}

func (b *Base) Unmarshal(buf []byte) error {
	if b.node != nil {
		common.GPool.Free(b.node)
		b.node = nil
	}
	b.payload = buf
	b.SetPayloadSize(len(buf))
	return nil
}

func (b *Base) ReadFrom(r io.Reader) (int, error) {
	if b.node == nil {
		b.node = common.GPool.Alloc(uint64(b.GetPayloadSize()))
		b.payload = b.node.Buf[:b.GetPayloadSize()]
	}
	infoBuf := make([]byte, b.GetInfoSize())
	n1, err := r.Read(infoBuf)
	if err != nil {
		return n1, err
	}
	b.SetInfoBuf(infoBuf)
	n2, err := r.Read(b.payload)
	if err != nil {
		return n2, err
	}
	return n1 + n2, nil
}

func (b *Base) WriteTo(w io.Writer) (int, error) {
	n1, err := b.descriptor.WriteTo(w)
	if err != nil {
		return n1, err
	}
	n2, err := w.Write(b.GetInfoBuf())
	if err != nil {
		return n2, err
	}
	n3, err := w.Write(b.payload)
	if err != nil {
		return n3, err
	}
	return n1 + n2 + n3, err
}
