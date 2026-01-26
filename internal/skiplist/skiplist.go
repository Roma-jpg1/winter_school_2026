package skiplist

import (
	"bytes"
	"errors"
	"math/rand"
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

type scanIter struct {
	cur *Node
	end []byte
}

func (it *scanIter) Next() (key, value []byte, ok bool, err error) {
	if it.cur == nil {
		return nil, nil, false, nil
	}

	if it.end != nil && bytes.Compare(it.cur.key, it.end) >= 0 {
		it.cur = nil
		return nil, nil, false, nil
	}

	key = append([]byte(nil), it.cur.key...)
	value = append([]byte(nil), it.cur.value...)
	it.cur = it.cur.next[0]

	return key, value, true, nil
}

func (it *scanIter) Close() error {
	return nil
}

// SkipList — In-Memory движок для HLR.
// Обеспечивает O(log N) на чтение/запись и упорядоченный доступ.
//
// В практической реализации вам нужно хранить:
// - ключи/значения как []byte
// - уровни (forward pointers)
// - генератор уровней с фиксируемым seed (для детерминизма тестов)

type Node struct {
	value []byte
	key   []byte
	next  []*Node
}

type SkipList struct {
	_        int // TODO(day1): заменить на реальные поля (Head, MaxLevel, etc)
	Head     *Node
	MaxLevel int
	p        float64
	RNG      *rand.Rand
}

// New создаёт SkipList. seed требуется для детерминируемых тестов (воспроизводимость поведения при ошибках).
func New(seed int64) *SkipList {
	const maxLevel = 100
	sl := &SkipList{
		MaxLevel: maxLevel,
		p:        0.5,
		RNG:      rand.New(rand.NewSource(seed)),
	}

	sl.Head = &Node{
		key:  nil,
		next: make([]*Node, maxLevel),
	}

	return sl
}

func (s *SkipList) Put(key, value []byte) error {
	_ = s
	_ = bytes.Compare // Важно: используйте bytes.Compare для лексикографического сравнения IMSI
	_ = key
	_ = value

	update := make([]*Node, s.MaxLevel)
	x := s.Head

	for i := s.MaxLevel - 1; i >= 0; i-- {
		for x.next[i] != nil && bytes.Compare(x.next[i].key, key) < 0 {
			x = x.next[i]
		}
		update[i] = x
	}

	if x.next[0] != nil && bytes.Compare(x.next[0].key, key) == 0 {
		v := append([]byte(nil), value...)
		x.next[0].value = v
		return nil
	}

	lvl := 1
	for lvl < s.MaxLevel && s.RNG.Float64() < s.p {
		lvl++
	}

	n := &Node{
		key:   append([]byte(nil), key...),
		value: append([]byte(nil), value...),
		next:  make([]*Node, lvl),
	}

	for i := 0; i < lvl; i++ {
		n.next[i] = update[i].next[i]
		update[i].next[i] = n
	}

	return nil
}

func (s *SkipList) Get(key []byte) ([]byte, error) {
	_ = s
	_ = key

	x := s.Head

	for i := s.MaxLevel - 1; i >= 0; i-- {
		for x.next[i] != nil && bytes.Compare(x.next[i].key, key) < 0 {
			x = x.next[i]
		}
	}

	x = x.next[0]

	if x != nil && bytes.Compare(x.key, key) == 0 {

		res := make([]byte, len(x.value))
		copy(res, x.value)
		return res, nil

	}

	return nil, ErrNotFound
}

func (s *SkipList) Delete(key []byte) error {
	_ = s
	_ = key
	update := make([]*Node, s.MaxLevel)
	x := s.Head

	for i := s.MaxLevel - 1; i >= 0; i-- {
		for x.next[i] != nil && bytes.Compare(x.next[i].key, key) < 0 {
			x = x.next[i]
		}
		update[i] = x
	}

	x = x.next[0]
	if x == nil || bytes.Compare(x.key, key) != 0 {
		return ErrNotFound
	}

	for i := 0; i < s.MaxLevel; i++ {
		if update[i].next[i] == x {
			update[i].next[i] = x.next[i]
		}
	}
	return nil
}

// Scan возвращает итератор по диапазону [start, end).
// Если start == nil, считается -∞ (начало списка).
// Если end == nil, считается +∞ (конец списка).
func (s *SkipList) Scan(start, end []byte) (Iterator, error) {
	_ = s
	_ = start
	_ = end

	x := s.Head
	if start != nil {
		for i := s.MaxLevel - 1; i >= 0; i-- {
			for x.next[i] != nil && bytes.Compare(x.next[i].key, start) < 0 {
				x = x.next[i]
			}
		}
	}

	x = x.next[0]

	if end != nil {
		end = append([]byte(nil), end...)
	}

	return &scanIter{
		cur: x,
		end: end,
	}, nil

}
