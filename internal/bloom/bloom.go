package bloom

import (
	"errors"
	"hash"
	"hash/fnv"
)

// ErrNotImplemented используется в заготовке практики третьего дня.
var ErrNotImplemented = errors.New("bloom: функция не реализована")

// Filter — вероятностный фильтр Блума ("Охранник диска").
// Позволяет мгновенно сказать "НЕТ, ключа здесь нет" с вероятностью 100%.
// Если говорит "ВОЗМОЖНО ЕСТЬ", придется проверять диск.
type Filter struct {
	bitSet []bool
	size   int
	hashes []hash.Hash64
}

// New создает новый фильтр.
// size (m) — размер битового массива.
// hashes (k) — количество хеш-функций.
func New(size uint64, hashesc uint8) *Filter {

	bitSet := make([]bool, size)
	hashes := make([]hash.Hash64, hashesc)
	for i := 0; i < int(hashesc); i++ {
		hashes[i] = fnv.New64a()
	}

	return &Filter{
		bitSet: bitSet,
		size:   int(size),
		hashes: hashes,
	}
}

// Add добавляет ключ в фильтр.
func (f *Filter) Add(Str []byte) error {

	for _, b := range f.hashes {
		b.Reset()
		b.Write([]byte(Str))
		index := b.Sum64() % uint64(f.size)
		f.bitSet[index] = true
	}
	return nil
}

// MayContain проверяет наличие ключа.
// Возвращает false, если ключа точно нет.
// Возвращает true, если ключ возможно есть (или произошел false positive).
func (f *Filter) MayContain(Str []byte) (bool, error) {
	for _, fun := range f.hashes {
		fun.Reset()
		fun.Write(Str)
		index := fun.Sum64() % uint64(f.size)
		if !f.bitSet[index] {
			return false, nil
		}
	}
	return true, nil
}
