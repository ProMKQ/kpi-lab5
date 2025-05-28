package datastore

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
)

const outFileName = "current-data"

var ErrNotFound = fmt.Errorf("record does not exist")

type hashIndex map[string]int64

type writeRequest struct {
	key   string
	value string
	resp  chan error
}

type Db struct {
	out              *os.File
	outOffset        int64
	index            hashIndex
	segmentSize      int64
	mu               sync.Mutex
	segmentSizeLimit int64
	dir              string
	muIndex          sync.RWMutex
	writeChan        chan writeRequest
	closeChan        chan struct{}
}

func (db *Db) writeLoop() {
	for {
		select {
		case req := <-db.writeChan:
			err := db.writeEntry(req.key, req.value)
			req.resp <- err
		case <-db.closeChan:
			return
		}
	}
}

func (db *Db) writeEntry(key, value string) error {
	e := entry{key: key, value: value}
	data := e.Encode()

	if db.segmentSizeLimit > 0 && db.outOffset+int64(len(data)) > db.segmentSizeLimit {
		if err := db.rollSegment(); err != nil {
			return err
		}
	}

	n, err := db.out.Write(data)
	if err != nil {
		return err
	}

	db.muIndex.Lock()
	db.index[key] = db.outOffset
	db.muIndex.Unlock()

	db.outOffset += int64(n)
	return nil
}

func OpenWithSegmentLimit(dir string, limit int64) (*Db, error) {
	outputPath := filepath.Join(dir, outFileName)
	f, err := os.OpenFile(outputPath, os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0o600)
	if err != nil {
		return nil, err
	}
	db := &Db{
		out:              f,
		index:            make(hashIndex),
		dir:              dir,
		segmentSizeLimit: limit,
		writeChan:        make(chan writeRequest),
		closeChan:        make(chan struct{}),
	}
	go db.writeLoop()
	return db, nil
}

func Open(dir string) (*Db, error) {
	outputPath := filepath.Join(dir, outFileName)
	f, err := os.OpenFile(outputPath, os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0o600)
	if err != nil {
		return nil, err
	}
	db := &Db{
		out:              f,
		index:            make(hashIndex),
		dir:              dir,
		segmentSizeLimit: 0,
		writeChan:        make(chan writeRequest),
		closeChan:        make(chan struct{}),
	}
	go db.writeLoop()
	err = db.recover()
	if err != nil && err != io.EOF {
		return nil, err
	}
	return db, nil
}

func (db *Db) recover() error {
	f, err := os.Open(db.out.Name())
	if err != nil {
		return err
	}
	defer f.Close()

	in := bufio.NewReader(f)
	for err == nil {
		var (
			record entry
			n      int
		)
		n, err = record.DecodeFromReader(in)
		if errors.Is(err, io.EOF) {
			if n != 0 {
				return fmt.Errorf("corrupted file")
			}
			break
		}

		db.index[record.key] = db.outOffset
		db.outOffset += int64(n)
	}
	return err
}

func (db *Db) Close() error {
	if db.closeChan != nil {
		close(db.closeChan)
	}
	return db.out.Close()
}

func (db *Db) Get(key string) (string, error) {
	db.muIndex.RLock()
	position, ok := db.index[key]
	db.muIndex.RUnlock()
	if !ok {
		return "", ErrNotFound
	}

	file, err := os.Open(db.out.Name())
	if err != nil {
		return "", err
	}
	defer file.Close()

	_, err = file.Seek(position, 0)
	if err != nil {
		return "", err
	}

	var record entry
	if _, err = record.DecodeFromReader(bufio.NewReader(file)); err != nil {
		return "", err
	}
	return record.value, nil
}

func (db *Db) Put(key, value string) error {
	resp := make(chan error)
	db.writeChan <- writeRequest{
		key:   key,
		value: value,
		resp:  resp,
	}
	return <-resp
}

func (db *Db) rollSegment() error {
	oldName := db.out.Name()
	backupName := oldName + ".bak"

	if err := db.out.Close(); err != nil {
		return err
	}
	if err := os.Rename(oldName, backupName); err != nil {
		return err
	}

	newFile, err := os.OpenFile(oldName, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o600)
	if err != nil {
		_ = os.Rename(backupName, oldName)
		return err
	}

	backupFile, err := os.Open(backupName)
	if err != nil {
		_ = newFile.Close()
		_ = os.Rename(backupName, oldName)
		return err
	}
	defer backupFile.Close()

	reader := bufio.NewReader(backupFile)
	latest := make(map[string]string)

	for {
		var rec entry
		_, err := rec.DecodeFromReader(reader)
		if err == io.EOF {
			break
		}
		if err != nil {
			_ = newFile.Close()
			_ = os.Rename(backupName, oldName)
			return fmt.Errorf("decode error: %w", err)
		}
		latest[rec.key] = rec.value
	}

	db.out = newFile
	db.outOffset = 0
	db.index = make(hashIndex)

	for k, v := range latest {
		e := entry{key: k, value: v}
		n, err := db.out.Write(e.Encode())
		if err != nil {
			_ = db.out.Close()
			_ = os.Rename(backupName, oldName)
			return fmt.Errorf("write error: %w", err)
		}
		db.index[k] = db.outOffset
		db.outOffset += int64(n)
	}

	_ = os.Remove(backupName)
	return nil
}

func (db *Db) Size() (int64, error) {
	info, err := db.out.Stat()
	if err != nil {
		return 0, err
	}
	return info.Size(), nil
}
