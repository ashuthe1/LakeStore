package engine

import (
	"bytes"
	"fmt"
	"testing"
)

func TestBasicCRUD(t *testing.T) {
	db, err := Open(t.TempDir()+"/db", nil)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	if err := db.Put([]byte("hello"), []byte("world"), false); err != nil {
		t.Fatal(err)
	}
	val, found, err := db.Get([]byte("hello"))
	if err != nil || !found || string(val) != "world" {
		t.Fatalf("get: %v found=%v val=%q", err, found, val)
	}

	if err := db.Delete([]byte("hello"), false); err != nil {
		t.Fatal(err)
	}
	_, found, err = db.Get([]byte("hello"))
	if err != nil || found {
		t.Fatalf("expected deleted, found=%v err=%v", found, err)
	}
}

func TestBatchAndReopen(t *testing.T) {
	dir := t.TempDir() + "/db"
	db, err := Open(dir, &Options{WriteBufferSize: 32 * 1024})
	if err != nil {
		t.Fatal(err)
	}

	b := NewWriteBatch()
	for i := 0; i < 1000; i++ {
		b.Put([]byte(fmt.Sprintf("key_%04d", i)), []byte(fmt.Sprintf("val_%d", i)))
	}
	b.Delete([]byte("key_0500"))
	if err := db.Write(b, true); err != nil {
		t.Fatal(err)
	}
	b.Destroy()
	db.Close()

	// Reopen and verify durability.
	db, err = Open(dir, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	for i := 0; i < 1000; i++ {
		key := []byte(fmt.Sprintf("key_%04d", i))
		val, found, err := db.Get(key)
		if err != nil {
			t.Fatal(err)
		}
		if i == 500 {
			if found {
				t.Fatalf("key_0500 should be deleted")
			}
			continue
		}
		if !found || string(val) != fmt.Sprintf("val_%d", i) {
			t.Fatalf("key %s: found=%v val=%q", key, found, val)
		}
	}
}

func TestIterator(t *testing.T) {
	db, err := Open(t.TempDir()+"/db", nil)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	for i := 0; i < 100; i++ {
		if err := db.Put([]byte(fmt.Sprintf("it_%03d", i)), []byte("v"), false); err != nil {
			t.Fatal(err)
		}
	}
	if err := db.Delete([]byte("it_050"), false); err != nil {
		t.Fatal(err)
	}

	it, err := db.NewIterator()
	if err != nil {
		t.Fatal(err)
	}
	defer it.Close()

	count := 0
	var prev []byte
	for it.SeekToFirst(); it.Valid(); it.Next() {
		k := it.Key()
		if prev != nil && bytes.Compare(prev, k) >= 0 {
			t.Fatalf("keys out of order: %q then %q", prev, k)
		}
		if string(k) == "it_050" {
			t.Fatalf("deleted key surfaced in scan")
		}
		prev = k
		count++
	}
	if count != 99 {
		t.Fatalf("scanned %d keys, want 99", count)
	}

	it.Seek([]byte("it_075"))
	if !it.Valid() || string(it.Key()) != "it_075" {
		t.Fatalf("seek failed")
	}
}

func TestEmptyValue(t *testing.T) {
	db, err := Open(t.TempDir()+"/db", nil)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	if err := db.Put([]byte("empty"), nil, false); err != nil {
		t.Fatal(err)
	}
	val, found, err := db.Get([]byte("empty"))
	if err != nil || !found || len(val) != 0 {
		t.Fatalf("empty value roundtrip: %v found=%v val=%q", err, found, val)
	}
}
