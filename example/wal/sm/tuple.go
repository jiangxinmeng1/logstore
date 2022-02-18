package sm

import "logstore/pkg/entry"

type Row struct {
	lsn  uint64
	data []byte
}

func newRow() *Row {
	return &Row{}
}

func (t *Row) Fill(e entry.Entry) {
	t.lsn = e.GetInfo().(*entry.CommitInfo).CommitId
	t.data = make([]byte, e.GetPayloadSize())
	copy(t.data, e.GetPayload())
}
