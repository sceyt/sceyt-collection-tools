package eventcache

import (
	"errors"
	"sync/atomic"
	"testing"
)

func TestGetWithLoaderCachesValue(t *testing.T) {
	var calls int32
	loader := func(key string) (interface{}, error) {
		atomic.AddInt32(&calls, 1)
		return "val:" + key, nil
	}

	c := New(1000, loader, nil, "", nil, nil)

	v1, err := c.Get("k1")
	if err != nil {
		t.Fatalf("unexpected error on first get: %v", err)
	}
	if v1.(string) != "val:k1" {
		t.Fatalf("unexpected value: %v", v1)
	}

	v2, err := c.Get("k1")
	if err != nil {
		t.Fatalf("unexpected error on second get: %v", err)
	}
	if v2.(string) != "val:k1" {
		t.Fatalf("unexpected value on second get: %v", v2)
	}

	if atomic.LoadInt32(&calls) != 1 {
		t.Fatalf("loader should be called once, got %d", calls)
	}
}

func TestSetAndRemove(t *testing.T) {
	c := New(1000, nil, nil, "", nil, nil)

	if err := c.Set("a", "b"); err != nil {
		t.Fatalf("set returned error: %v", err)
	}
	v, err := c.Get("a")
	if err != nil {
		t.Fatalf("unexpected error on get after set: %v", err)
	}
	if v.(string) != "b" {
		t.Fatalf("unexpected value: %v", v)
	}

	c.Remove("a")
	_, err = c.Get("a")
	if err == nil {
		t.Fatalf("expected error after remove, got nil")
	}
}

func TestPurge(t *testing.T) {
	c := New(1000, nil, nil, "", nil, nil)
	_ = c.Set("k1", 1)
	_ = c.Set("k2", 2)
	c.Purge()
	if _, err := c.Get("k1"); err == nil {
		t.Fatalf("expected error for k1 after purge")
	}
	if _, err := c.Get("k2"); err == nil {
		t.Fatalf("expected error for k2 after purge")
	}
}

func TestLoaderError(t *testing.T) {
	wantErr := errors.New("boom")
	loader := func(key string) (interface{}, error) { return nil, wantErr }
	c := New(1000, loader, nil, "", nil, nil)
	_, err := c.Get("x")
	if err == nil {
		t.Fatalf("expected error from loader, got nil")
	}
}

func TestDeriveKeysFromJSONField(t *testing.T) {
	kd := DeriveKeysFromJSONField("customer_id")
	body := []byte(`{"customer_id":"123"}`)
	keys := kd("update", body, nil)
	if len(keys) != 1 || keys[0] != "123" {
		t.Fatalf("unexpected keys: %v", keys)
	}

	// invalid/missing field -> no keys
	keys = kd("update", []byte(`{"x":"y"}`), nil)
	if len(keys) != 0 {
		t.Fatalf("expected no keys for missing field, got %v", keys)
	}
}
