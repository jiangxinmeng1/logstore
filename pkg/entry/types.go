package entry

import (
	"io"
	"logstore/pkg/common"
)

type Type = uint16

const (
	ETInvalid Type = iota
	ETNoop
	ETFlush
	ETCheckpoint
	ETUncommitted
	ETTxn
	ETCustomizedStart
)

type Desc interface {
	GetType() Type
	SetType(Type)
	GetPayloadSize() int
	SetPayloadSize(int)
	GetInfoSize() int
	SetInfoSize(int)
	TotalSize() int
	GetMetaBuf() []byte
	IsFlush() bool
	IsCheckpoint() bool
}

type Entry interface {
	Desc
	GetPayload() []byte
	SetInfo(interface{})
	GetInfo() interface{}
	GetInfoBuf() []byte
	SetInfoBuf(buf []byte)

	Unmarshal([]byte) error
	UnmarshalFromNode(*common.MemNode, bool) error
	ReadFrom(io.Reader) (int, error)
	WriteTo(io.Writer) (int, error)

	WaitDone() error
	DoneWithErr(error)
	GetError() error

	Free()
}
