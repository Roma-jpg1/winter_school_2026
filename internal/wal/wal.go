package wal

import (
	"bufio"
	"encoding/binary"
	"errors"
	"io"
)

// ErrNotImplemented используется в заготовке практики Дня 2.
var ErrNotImplemented = errors.New("wal: функция не реализована")

// OpType — тип операции в WAL (Put или Delete).
type OpType byte

const (
	OpPut    OpType = 1
	OpDelete OpType = 2
)

// Record — запись в логе.
// Используется для восстановления Memtable после сбоя (Crash Recovery).
type Record struct {
	Type  OpType
	Key   []byte
	Value []byte // только для Put
}

// Writer — append-only запись в лог.
// Гарантирует, что данные записаны до того, как мы подтвердим успешность операции пользователю.
type Writer struct {
	bw *bufio.Writer
}

func NewWriter(w io.Writer) *Writer {
	return &Writer{bw: bufio.NewWriter(w)}
}

func (w *Writer) Append(rec Record) error {

	if err := w.bw.WriteByte(byte(rec.Type)); err != nil {
		return err
	}

	if err := writeBytes(w.bw, rec.Key); err != nil {
		return err
	}

	if rec.Type == OpPut {
		if err := writeBytes(w.bw, rec.Value); err != nil {
			return err
		}
	}

	// важно: сбросить в underlying writer, чтобы WAL реально записался
	return w.bw.Flush()
}

func (w *Writer) Close() error { return nil }

// Reader — последовательное чтение лога при старте системы.
type Reader struct {
	br *bufio.Reader
}

func NewReader(r io.Reader) *Reader {
	return &Reader{br: bufio.NewReader(r)}
}

func (r *Reader) Next() (Record, bool, error) {

	t, err := r.br.ReadByte()
	if err == io.EOF {
		return Record{}, false, nil
	}
	if err != nil {
		return Record{}, false, err
	}

	rec := Record{Type: OpType(t)}

	key, err := readBytes(r.br)
	if err != nil {
		return Record{}, false, err
	}
	rec.Key = key

	if rec.Type == OpPut {
		val, err := readBytes(r.br)
		if err != nil {
			return Record{}, false, err
		}
		rec.Value = val
	}

	return rec, true, nil
}

func writeBytes(w *bufio.Writer, b []byte) error {
	var lenBuf [4]byte
	binary.LittleEndian.PutUint32(lenBuf[:], uint32(len(b)))
	if _, err := w.Write(lenBuf[:]); err != nil {
		return err
	}
	_, err := w.Write(b)
	return err
}

func readBytes(r *bufio.Reader) ([]byte, error) {
	var lenBuf [4]byte
	if _, err := io.ReadFull(r, lenBuf[:]); err != nil {
		return nil, err
	}
	n := binary.LittleEndian.Uint32(lenBuf[:])
	b := make([]byte, int(n))
	_, err := io.ReadFull(r, b)
	return b, err
}
