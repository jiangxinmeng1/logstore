package common

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestMerge1(t *testing.T) {
	i1 := &ClosedIntervals{
		Intervals: []*ClosedInterval{
			{Start: 1, End: 3},
			{Start: 5, End: 5},
			{Start: 11, End: 12},
		},
	}
	i2 := &ClosedIntervals{
		Intervals: []*ClosedInterval{
			{Start: 2, End: 2},
			{Start: 4, End: 4},
			{Start: 6, End: 6},
			{Start: 8, End: 13},
		},
	}
	i2bk := &ClosedIntervals{
		Intervals: []*ClosedInterval{
			{Start: 2, End: 2},
			{Start: 4, End: 4},
			{Start: 6, End: 6},
			{Start: 8, End: 13},
		},
	}
	i1.TryMerge(*i2)
	res := &ClosedIntervals{
		Intervals: []*ClosedInterval{
			{Start: 1, End: 6},
			{Start: 8, End: 13},
		},
	}
	assert.True(t, i1.Equal(res))
	assert.True(t, i2.Equal(i2bk)) //i2 should remain the same
}

func TestMerge2(t *testing.T) {
	i1 := &ClosedIntervals{
		Intervals: []*ClosedInterval{},
	}
	i2 := &ClosedIntervals{
		Intervals: []*ClosedInterval{
			{Start: 2, End: 2},
			{Start: 4, End: 4},
			{Start: 6, End: 6},
			{Start: 8, End: 13},
		},
	}
	i2bk := &ClosedIntervals{
		Intervals: []*ClosedInterval{
			{Start: 2, End: 2},
			{Start: 4, End: 4},
			{Start: 6, End: 6},
			{Start: 8, End: 13},
		},
	}
	i1.TryMerge(*i2)
	res := &ClosedIntervals{
		Intervals: []*ClosedInterval{
			{Start: 2, End: 2},
			{Start: 4, End: 4},
			{Start: 6, End: 6},
			{Start: 8, End: 13},
		},
	}
	assert.True(t, i1.Equal(res))
	assert.True(t, i2.Equal(i2bk)) //i2 should remain the same
}

func TestMerge3(t *testing.T) {
	i1 := &ClosedIntervals{
		Intervals: []*ClosedInterval{
			{Start: 2, End: 2},
			{Start: 4, End: 4},
			{Start: 6, End: 6},
			{Start: 8, End: 13},
		},
	}
	i2 := &ClosedIntervals{
		Intervals: []*ClosedInterval{},
	}
	i2bk := &ClosedIntervals{
		Intervals: []*ClosedInterval{},
	}
	i1.TryMerge(*i2)
	res := &ClosedIntervals{
		Intervals: []*ClosedInterval{
			{Start: 2, End: 2},
			{Start: 4, End: 4},
			{Start: 6, End: 6},
			{Start: 8, End: 13},
		},
	}
	assert.True(t, i1.Equal(res))
	assert.True(t, i2.Equal(i2bk)) //i2 should remain the same
}
func TestContains1(t *testing.T) {
	i1 := &ClosedIntervals{
		Intervals: []*ClosedInterval{
			{Start: 2, End: 2},
			{Start: 4, End: 4},
			{Start: 6, End: 6},
			{Start: 8, End: 13},
		},
	}
	i2 := &ClosedIntervals{
		Intervals: []*ClosedInterval{},
	}
	i2bk := &ClosedIntervals{
		Intervals: []*ClosedInterval{},
	}
	assert.True(t, i1.Contains(*i2))
	assert.True(t, i2.Equal(i2bk)) //i2 should remain the same
}
func TestContains2(t *testing.T) {
	i1 := &ClosedIntervals{
		Intervals: []*ClosedInterval{
			{Start: 2, End: 2},
			{Start: 4, End: 4},
			{Start: 6, End: 6},
			{Start: 8, End: 13},
		},
	}
	i2 := &ClosedIntervals{
		Intervals: []*ClosedInterval{
			{Start: 2, End: 2},
			{Start: 10, End: 13},
		},
	}
	i2bk := &ClosedIntervals{
		Intervals: []*ClosedInterval{
			{Start: 2, End: 2},
			{Start: 10, End: 13},
		},
	}
	assert.True(t, i1.Contains(*i2))
	assert.True(t, i2.Equal(i2bk)) //i2 should remain the same
}
func TestContains3(t *testing.T) {
	i1 := &ClosedIntervals{
		Intervals: []*ClosedInterval{
		},
	}
	i2 := &ClosedIntervals{
		Intervals: []*ClosedInterval{
			{Start: 6, End: 6},
			{Start: 8, End: 13},
		},
	}
	i2bk := &ClosedIntervals{
		Intervals: []*ClosedInterval{
			{Start: 6, End: 6},
			{Start: 8, End: 13},
		},
	}
	assert.False(t, i1.Contains(*i2))
	assert.True(t, i2.Equal(i2bk)) //i2 should remain the same
}