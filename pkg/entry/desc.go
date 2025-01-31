package entry

import (
	"encoding/binary"
	"io"
	"unsafe"
)

const (
	PayloadSizeOffset = int(unsafe.Sizeof(ETInvalid))
	InfoSizeOffset    = int(unsafe.Sizeof(ETInvalid) + unsafe.Sizeof(uint32(0)))
	DescriptorSize    = int(unsafe.Sizeof(ETInvalid) + 2*unsafe.Sizeof(uint32(0)))
)

//type u16, payloadsize u32, infosize u32
type descriptor struct {
	descBuf []byte
}

func newDescriptor() *descriptor {
	return &descriptor{
		descBuf: make([]byte, DescriptorSize),
	}
}

func (desc *descriptor) IsFlush() bool {
	return desc.GetType() == ETFlush
}

func (desc *descriptor) IsCheckpoint() bool {
	return desc.GetType() == ETCheckpoint
}

func (desc *descriptor) SetType(t Type) {
	binary.BigEndian.PutUint16(desc.descBuf, t)
}

func (desc *descriptor) SetPayloadSize(size int) {
	binary.BigEndian.PutUint32(desc.descBuf[PayloadSizeOffset:], uint32(size))
}

func (desc *descriptor) SetInfoSize(size int) {
	binary.BigEndian.PutUint32(desc.descBuf[InfoSizeOffset:], uint32(size))
}

func (desc *descriptor) reset() {
	desc.SetType(ETInvalid)
	desc.SetPayloadSize(0)
	desc.SetInfoSize(0)
}

func (desc *descriptor) GetMetaBuf() []byte {
	return desc.descBuf
}

func (desc *descriptor) GetType() Type {
	return binary.BigEndian.Uint16(desc.descBuf)
}

func (desc *descriptor) GetPayloadSize() int {
	return int(binary.BigEndian.Uint32(desc.descBuf[PayloadSizeOffset:]))
}

func (desc *descriptor) GetInfoSize() int {
	return int(binary.BigEndian.Uint32(desc.descBuf[InfoSizeOffset:]))
}

func (desc *descriptor) TotalSize() int {
	return DescriptorSize + desc.GetPayloadSize() + desc.GetInfoSize()
}

func (desc *descriptor) TotalSizeExpectMeta() int {
	return desc.GetPayloadSize() + desc.GetInfoSize()
}

func (desc *descriptor) WriteTo(w io.Writer) (int, error) {
	return w.Write(desc.descBuf)
}

func (desc *descriptor) ReadMeta(r io.Reader) (int, error) {
	return r.Read(desc.descBuf)
}
