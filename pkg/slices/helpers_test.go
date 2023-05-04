package slices

import (
	"fmt"
	"testing"
)

func TestContainsAll(t *testing.T) {
	type testCase[T comparable] struct {
		s1  []T
		s2  []T
		res bool
	}

	testCases := []testCase[string]{
		{
			s1:  []string{"a", "b", "c", "d"},
			s2:  []string{"b", "c"},
			res: true,
		},
		{
			s1:  []string{"a", "b", "b", "c", "d"},
			s2:  []string{"b", "c", "c"},
			res: true,
		},
		{
			s1:  []string{"a", "b", "b", "c", "d"},
			s2:  []string{"b", "c", "c", "f"},
			res: false,
		},
		{
			s1:  []string{"a", "b", "c", "d"},
			s2:  []string{},
			res: true,
		},
		{
			s1:  []string{},
			s2:  []string{"a", "b", "c", "d"},
			res: false,
		},
	}

	for _, tc := range testCases {
		testname := fmt.Sprintf("%v,%v,%v", tc.s1, tc.s2, tc.res)
		t.Run(testname, func(t *testing.T) {
			res := ContainsAll(tc.s1, tc.s2...)
			if res != tc.res {
				t.Errorf("got %v, want %v", res, tc.res)
			}
		})
	}
}

func TestRetainByCondition(t *testing.T) {
	type testCase[T any] struct {
		name      string
		s         []*T
		res       []*T
		condition func(*T) bool
	}

	var testCases []testCase[int64]
	a, b, c, d := int64(1), int64(2), int64(3), int64(4)
	testCases = append(testCases, testCase[int64]{
		name: "remove if smaller than 4",
		s:    []*int64{&a, &b, &c, &d},
		res:  []*int64{&a, &b, &c},
		condition: func(i *int64) bool {
			return *i < 4
		},
	})
	testCases = append(testCases, testCase[int64]{
		name: "remove if odd number",
		s:    []*int64{&a, &b, &c, &d},
		res:  []*int64{&b, &d},
		condition: func(i *int64) bool {
			return *i%2 == 0
		},
	})

	for _, tc := range testCases {
		testname := fmt.Sprintf("%s", tc.name)
		t.Run(testname, func(t *testing.T) {
			tc.s = RetainByCondition(tc.s, tc.condition)
			if len(tc.s) != len(tc.res) {
				t.Errorf("got %v, want %v", tc.s, tc.res)
			}
			for i := 0; i < len(tc.s); i++ {
				if *tc.s[i] != *tc.res[i] {
					t.Errorf("got %v, want %v", tc.s, tc.res)
				}
			}
		})
	}
}

func TestRemoveAt(t *testing.T) {
	type testCase[T any] struct {
		name string
		s    []T
		i    int
		res  []T
	}

	var testCases []testCase[string]
	testCases = append(testCases, testCase[string]{
		name: "remove at index 0",
		s:    []string{"a", "b", "c", "d"},
		i:    0,
		res:  []string{"b", "c", "d"},
	}, testCase[string]{
		name: "remove at index 0 when len(s) == 1",
		s:    []string{"a"},
		i:    0,
		res:  []string{},
	}, testCase[string]{
		name: "remove at index 1",
		s:    []string{"a", "b", "c", "d"},
		i:    1,
		res:  []string{"a", "c", "d"},
	}, testCase[string]{
		name: "remove at last index",
		s:    []string{"a", "b", "c", "d"},
		i:    3,
		res:  []string{"a", "b", "c"},
	})

	for _, tc := range testCases {
		testname := fmt.Sprintf("Remove At : %s", tc.name)
		t.Run(testname, func(t *testing.T) {
			tc.s = RemoveAt(tc.s, tc.i)
			if len(tc.s) != len(tc.res) {
				t.Errorf("got %v, want %v", tc.s, tc.res)
			}
			for i := 0; i < len(tc.s); i++ {
				if tc.s[i] != tc.res[i] {
					t.Errorf("got %v, want %v", tc.s, tc.res)
				}
			}
		})
	}
}
