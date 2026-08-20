package catalog

import (
	"math"
	"reflect"
	"testing"
)

func TestPaginateCurrentFilesBoundaries(t *testing.T) {
	refs := []CurrentFileRef{
		{LogicalFileID: 1},
		{LogicalFileID: 2},
		{LogicalFileID: 3},
	}
	value := func(n int64) *int64 { return &n }
	tests := []struct {
		name string
		page CurrentFilePage
		want []int64
	}{
		{name: "nil offset and limit", want: []int64{1, 2, 3}},
		{name: "zero offset", page: CurrentFilePage{Offset: value(0)}, want: []int64{1, 2, 3}},
		{name: "offset below length", page: CurrentFilePage{Offset: value(1)}, want: []int64{2, 3}},
		{name: "offset equal length", page: CurrentFilePage{Offset: value(3)}, want: []int64{}},
		{name: "offset greater than length", page: CurrentFilePage{Offset: value(4)}, want: []int64{}},
		{name: "maximum int64 offset", page: CurrentFilePage{Offset: value(math.MaxInt64)}, want: []int64{}},
		{name: "zero limit", page: CurrentFilePage{Limit: value(0)}, want: []int64{}},
		{name: "limit below remaining", page: CurrentFilePage{Limit: value(2)}, want: []int64{1, 2}},
		{name: "limit equal remaining", page: CurrentFilePage{Offset: value(1), Limit: value(2)}, want: []int64{2, 3}},
		{name: "limit greater than remaining", page: CurrentFilePage{Offset: value(1), Limit: value(3)}, want: []int64{2, 3}},
		{name: "maximum allowed page size", page: CurrentFilePage{Limit: value(MaxCurrentFilePageSize)}, want: []int64{1, 2, 3}},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			gotRefs := paginateCurrentFiles(refs, tc.page)
			got := make([]int64, len(gotRefs))
			for i, ref := range gotRefs {
				got[i] = ref.LogicalFileID
			}
			if !reflect.DeepEqual(got, tc.want) {
				t.Fatalf("paginateCurrentFiles() = %v, want %v", got, tc.want)
			}
		})
	}

	if err := validateCurrentFilePage(CurrentFilePage{Limit: value(MaxCurrentFilePageSize)}); err != nil {
		t.Fatalf("MaxCurrentFilePageSize must remain valid: %v", err)
	}
}
