package maps

import "testing"

func TestCopy(t *testing.T) {
	type args struct {
		m map[int]string
	}
	tests := []struct {
		name string
		args args
	}{
		{
			name: "Copy",
			args: args{
				m: map[int]string{
					1: "one",
					2: "two",
					3: "three",
				},
			},
		},
		{
			name: "Copy empty",
			args: args{
				m: map[int]string{},
			},
		}, {
			name: "Copy nil",
			args: args{
				m: nil,
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			copied := Copy(tt.args.m)
			for k, v := range copied {
				if v != tt.args.m[k] {
					t.Errorf("Copy() = %v, want %v", copied, tt.args.m)
				}
			}
		})
	}
}
