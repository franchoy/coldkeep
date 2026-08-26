package faultfs

import (
	"errors"
	"testing"
)

func TestFaultFSReleaseFaultClassesAreScriptable(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name  string
		fault Fault
		op    Operation
		want  error
	}{
		{
			name:  "write",
			fault: Fault{Op: OpWrite, Err: ErrFaultWrite},
			op:    OpWrite,
			want:  ErrFaultWrite,
		},
		{
			name:  "sync",
			fault: Fault{Op: OpSync, Err: ErrFaultSync},
			op:    OpSync,
			want:  ErrFaultSync,
		},
		{
			name:  "close",
			fault: Fault{Op: OpClose, Err: ErrFaultClose},
			op:    OpClose,
			want:  ErrFaultClose,
		},
		{
			name:  "truncate",
			fault: Fault{Op: OpTruncate, Err: ErrFaultTruncate},
			op:    OpTruncate,
			want:  ErrFaultTruncate,
		},
		{
			name:  "rename",
			fault: Fault{Op: OpRename, Err: ErrFaultRename},
			op:    OpRename,
			want:  ErrFaultRename,
		},
		{
			name:  "mkdir",
			fault: Fault{Op: OpMkdirAll, Err: ErrFaultMkdir},
			op:    OpMkdirAll,
			want:  ErrFaultMkdir,
		},
		{
			name:  "stat",
			fault: Fault{Op: OpStat, Err: ErrFaultStat},
			op:    OpStat,
			want:  ErrFaultStat,
		},
		{
			name:  "remove",
			fault: Fault{Op: OpRemove, Err: ErrFaultRemove},
			op:    OpRemove,
			want:  ErrFaultRemove,
		},
	}

	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			script := NewScript(tc.fault)
			got, ok := script.record(tc.op)
			if !ok {
				t.Fatalf("fault was not triggered for %s", tc.op)
			}
			if !errors.Is(got.errOrDefault(nil), tc.want) {
				t.Fatalf("fault error = %v, want %v", got.errOrDefault(nil), tc.want)
			}
		})
	}
}
