package datastore

import (
	"fmt"
	"testing"
)

func TestDb(t *testing.T) {
	tmp := t.TempDir()
	db, err := Open(tmp)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = db.Close()
	})

	pairs := [][]string{
		{"k1", "v1"},
		{"k2", "v2"},
		{"k3", "v3"},
		{"k2", "v2.1"},
	}

	t.Run("put/get", func(t *testing.T) {
		for _, pair := range pairs {
			err := db.Put(pair[0], pair[1])
			if err != nil {
				t.Errorf("Cannot put %s: %s", pairs[0], err)
			}
			value, err := db.Get(pair[0])
			if err != nil {
				t.Errorf("Cannot get %s: %s", pairs[0], err)
			}
			if value != pair[1] {
				t.Errorf("Bad value returned expected %s, got %s", pair[1], value)
			}
		}
	})

	t.Run("file growth", func(t *testing.T) {
		sizeBefore, _ := db.Size()
		_ = db.Put("key", "value")
		sizeAfter, _ := db.Size()
		if sizeBefore == sizeAfter {
			t.Errorf("Size does not grow after put (before %d, after %d)", sizeBefore, sizeAfter)
		}
		for _, pair := range pairs {
			err := db.Put(pair[0], pair[1])
			if err != nil {
				t.Errorf("Cannot put %s: %s", pairs[0], err)
			}
		}
		sizeAfter, err := db.Size()
		if err != nil {
			t.Fatal(err)
		}
		if sizeAfter <= sizeBefore {
			t.Errorf("Size does not grow after put (before %d, after %d)", sizeBefore, sizeAfter)
		}
	})

	t.Run("new db process", func(t *testing.T) {
		if err := db.Close(); err != nil {
			t.Fatal(err)
		}
		db, err = Open(tmp)
		if err != nil {
			t.Fatal(err)
		}

		uniquePairs := make(map[string]string)
		for _, pair := range pairs {
			uniquePairs[pair[0]] = pair[1]
		}

		for key, expectedValue := range uniquePairs {
			value, err := db.Get(key)
			if err != nil {
				t.Errorf("Cannot get %s: %s", key, err)
			}
			if value != expectedValue {
				t.Errorf("Get(%q) = %q, wanted %q", key, value, expectedValue)
			}
		}
	})
}

func TestSegmentRollingAndMerge(t *testing.T) {
	tmp := t.TempDir()

	db, err := OpenWithSegmentLimit(tmp, 200)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = db.Close()
	})

	for i := 0; i < 20; i++ {
		key := fmt.Sprintf("key%d", i)
		value := fmt.Sprintf("value%d", i)
		if err := db.Put(key, value); err != nil {
			t.Fatalf("Put failed at %d: %v", i, err)
		}
	}

	for i := 0; i < 20; i++ {
		key := fmt.Sprintf("key%d", i)
		expected := fmt.Sprintf("value%d", i)
		val, err := db.Get(key)
		if err != nil {
			t.Errorf("Get failed for key=%s: %v", key, err)
		}
		if val != expected {
			t.Errorf("Expected %s, got %s", expected, val)
		}
	}
}

func TestPutGetInt64(t *testing.T) {
	tmp := t.TempDir()

	db, err := Open(tmp)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = db.Close()
	})

	t.Run("put and get int64 value", func(t *testing.T) {
		var key = "int64-key"
		var originalValue int64 = 9876543210123

		err := db.PutInt64(key, originalValue)
		if err != nil {
			t.Fatalf("PutInt64 failed: %v", err)
		}

		got, err := db.GetInt64(key)
		if err != nil {
			t.Fatalf("GetInt64 failed: %v", err)
		}
		if got != originalValue {
			t.Errorf("GetInt64 = %d; want %d", got, originalValue)
		}
	})
}

func TestGetInt64WrongType(t *testing.T) {
	tmp := t.TempDir()

	db, err := Open(tmp)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = db.Close()
	})

	t.Run("get int64 from wrong type", func(t *testing.T) {
		key := "wrong-type"
		err := db.Put(key, "just a string")
		if err != nil {
			t.Fatalf("Put failed: %v", err)
		}

		_, err = db.GetInt64(key)
		if err == nil {
			t.Errorf("Expected error when getting int64 from string, got none")
		}
	})
}
