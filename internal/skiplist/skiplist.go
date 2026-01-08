package skiplist

import (
	"bytes"
	"errors"
)

// ErrNotFound означает отсутствие ключа (IMSI).
var ErrNotFound = errors.New("skiplist: ключ не найден")

// ErrNotImplemented используется в заготовке практики первого дня.
var ErrNotImplemented = errors.New("skiplist: функция не реализована")

// Iterator — упорядоченная итерация по диапазону ключей (Range Scan).
// В HLR используется для выгрузки абонентов по префиксу IMSI.
type Iterator interface {
	Next() (key, value []byte, ok bool, err error)
	Close() error
}

// SkipList — In-Memory движок для HLR.
// Обеспечивает O(log N) на чтение/запись и упорядоченный доступ.
//
// В практической реализации вам нужно хранить:
// - ключи/значения как []byte
// - уровни (forward pointers)
// - генератор уровней с фиксируемым seed (для детерминизма тестов)
type SkipList struct {
	_ int // TODO(day1): заменить на реальные поля (Head, MaxLevel, etc)
}

// New создаёт SkipList. seed требуется для детерминируемых тестов (воспроизводимость поведения при ошибках).
func New(seed int64) *SkipList {
	_ = seed
	return &SkipList{}
}

func (s *SkipList) Put(key, value []byte) error {
	_ = s
	_ = bytes.Compare // Важно: используйте bytes.Compare для лексикографического сравнения IMSI
	_ = key
	_ = value
	return ErrNotImplemented
}

func (s *SkipList) Get(key []byte) ([]byte, error) {
	_ = s
	_ = key
	return nil, ErrNotImplemented
}

func (s *SkipList) Delete(key []byte) error {
	_ = s
	_ = key
	return ErrNotImplemented
}

// Scan возвращает итератор по диапазону [start, end).
// Если start == nil, считается -∞ (начало списка).
// Если end == nil, считается +∞ (конец списка).
func (s *SkipList) Scan(start, end []byte) (Iterator, error) {
	_ = s
	_ = start
	_ = end
	return nil, ErrNotImplemented
}
