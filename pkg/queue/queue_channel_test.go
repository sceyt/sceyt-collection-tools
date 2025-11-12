package queue

import "testing"

func TestNewChannelQueue(t *testing.T) {
	type testCase[T any] struct {
		cap  int
		name string
	}

	testCases := []testCase[string]{
		{
			cap:  1,
			name: "cap 1",
		},
		{
			cap:  100,
			name: "cap 100",
		},
	}

	for _, tc := range testCases {
		testname := tc.name
		t.Run(testname, func(t *testing.T) {
			cq := NewChannelQueue[int64](tc.cap)
			if cq.Len() != 0 {
				t.Errorf("got %v, want %v", cq.Len(), 0)
			}
		})
	}
}

func TestChannelQueueEnqueue(t *testing.T) {
	type testCase[T any] struct {
		cap  int
		name string
	}

	testCases := []testCase[string]{
		{
			cap:  1,
			name: "cap 1",
		},
		{
			cap:  100,
			name: "cap 100",
		},
	}

	for _, tc := range testCases {
		testname := tc.name
		t.Run(testname, func(t *testing.T) {
			cq := NewChannelQueue[int64](tc.cap)
			if cq.Len() != 0 {
				t.Errorf("got %v, want %v", cq.Len(), 0)
			}
			err := cq.Enqueue(1)
			if err != nil {
				t.Errorf("got %v, want %v", err, nil)
			}
			if cq.Len() != 1 {
				t.Errorf("got %v, want %v", cq.Len(), 1)
			}
		})
	}
}

func TestChannelQueueDequeue(t *testing.T) {
	type testCase[T any] struct {
		cap  int
		name string
	}

	testCases := []testCase[string]{
		{
			cap:  1,
			name: "cap 1",
		},
		{
			cap:  100,
			name: "cap 100",
		},
	}

	for _, tc := range testCases {
		testname := tc.name
		t.Run(testname, func(t *testing.T) {
			cq := NewChannelQueue[int64](tc.cap)
			if cq.Len() != 0 {
				t.Errorf("got %v, want %v", cq.Len(), 0)
			}
			err := cq.Enqueue(1)
			if err != nil {
				t.Errorf("got %v, want %v", err, nil)
			}
			if cq.Len() != 1 {
				t.Errorf("got %v, want %v", cq.Len(), 1)
			}
			v, err := cq.Dequeue()
			if err != nil {
				t.Errorf("got %v, want %v", err, nil)
			}
			if v != 1 {
				t.Errorf("got %v, want %v", v, 1)
			}
			if cq.Len() != 0 {
				t.Errorf("got %v, want %v", cq.Len(), 0)
			}
		})
	}
}
