package lsm

import (
	"errors"
	"fmt"
	"kvschool/internal/skiplist"
	"kvschool/internal/sstable"
	"kvschool/internal/wal"
	"os"
	"path/filepath"
)

// ErrNotImplemented используется в заготовке практики второго дня.
var ErrNotImplemented = errors.New("lsm: функция не реализована")

// Options задаёт параметры LSM движка.
type Options struct {
	Dir string // Директория для хранения WAL и SSTables

	// Максимальный размер Memtable перед сбросом на диск (Flush).
	// В телекоме это баланс между памятью и частотой I/O.
	MemtableFlushThreshold int
}

// Engine — основной движок CDR Storage.
// Координирует работу Memtable, WAL и SSTables.
// Отвечает за Compaction (сборку мусора).
type Engine struct {
	options  Options
	memtable *skiplist.SkipList
	wal      *wal.Writer
	walFile  *os.File
	sstCount int
	memSize  int
}

func Open(opts Options) (*Engine, error) {
	_ = os.MkdirAll(opts.Dir, 0755)

	e := &Engine{
		options:  opts,
		memtable: skiplist.New(1),
	}

	walPath := filepath.Join(opts.Dir, "wal.log")

	if f, err := os.Open(walPath); err == nil {
		reader := wal.NewReader(f)
		for {
			rec, ok, _ := reader.Next()
			if !ok {
				break
			}
			if rec.Type == wal.OpPut {
				e.memtable.Put(rec.Key, rec.Value)
			} else {
				e.memtable.Delete(rec.Key)
			}
		}
		f.Close()
	}
	f, err := os.OpenFile(walPath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return nil, err
	}
	e.walFile = f
	e.wal = wal.NewWriter(f)

	return e, nil
}

func (e *Engine) Put(key, value []byte) error {
	_ = e.wal.Append(wal.Record{Type: wal.OpPut, Key: key, Value: value})
	e.memSize += len(key) + len(value)
	err := e.memtable.Put(key, value)

	if e.options.MemtableFlushThreshold > 0 && e.memSize >= e.options.MemtableFlushThreshold {
		_ = e.Flush()
		e.memSize = 0
	}

	return err
}

func (e *Engine) Get(key []byte) ([]byte, error) {
	return e.memtable.Get(key)
}

func (e *Engine) Flush() error {
	e.sstCount++
	path := filepath.Join(e.options.Dir, fmt.Sprintf("data_%d.sst", e.sstCount))

	f, _ := os.Create(path)
	writer := sstable.NewWriter(f)
	if err := writer.WriteFromSkipList(e.memtable); err != nil {
		return err
	}
	e.memtable = skiplist.New(1)
	return e.walFile.Truncate(0)
}

func (e *Engine) Close() error {
	_ = e.Flush()
	return e.walFile.Close()
}

func (e *Engine) Delete(key []byte) error {
	_ = e.wal.Append(wal.Record{Type: wal.OpDelete, Key: key})
	return e.memtable.Delete(key)
}
