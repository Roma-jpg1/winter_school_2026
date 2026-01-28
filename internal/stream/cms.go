package stream

import (
	"errors"
	"hash/fnv"
)

// ErrNotImplemented используется в заготовке практики третьего дня.
var ErrNotImplemented = errors.New("stream: функция не реализована")

// CountMinSketch — структура для поиска Top-Talkers (частых элементов).
// Использует фиксированный объем памяти (w * d счетчиков), чтобы считать трафик миллионов абонентов.
type CountMinSketch struct {
	table []uint64
	width uint32
	depth uint32
}

// NewCountMinSketch создает скетч.
// width (w) — ширина таблицы (больше ширина -> меньше коллизий).
// depth (d) — количество хеш-функций (больше глубина -> выше точность).
func NewCountMinSketch(width, depth uint32, _ uint64) *CountMinSketch {
	table := make([]uint64, uint64(width)*uint64(depth))

	return &CountMinSketch{
		table: table,
		width: width,
		depth: depth,
	}
}

// Add увеличивает счетчик для ключа (например, +1 байт трафика).
func (c *CountMinSketch) Add(key []byte) error {
	for row := uint32(0); row < c.depth; row++ {

		h := fnv.New64a()
		h.Reset()
		h.Write(key)

		col := uint32((h.Sum64() + uint64(row)) % uint64(c.width))
		idx := uint64(row)*uint64(c.width) + uint64(col)

		c.table[idx]++
	}

	return nil
}

// Estimate возвращает примерную частоту ключа.
// Гарантия: Estimate >= TrueCount (никогда не занижает).
func (c *CountMinSketch) Estimate(key []byte) (uint64, error) {
	var min uint64

	for row := uint32(0); row < c.depth; row++ {

		h := fnv.New64a()
		h.Reset()
		h.Write(key)

		col := uint32((h.Sum64() + uint64(row)) % uint64(c.width))
		idx := uint64(row)*uint64(c.width) + uint64(col)

		val := c.table[idx]
		if row == 0 || val < min {
			min = val
		}
	}
	return min, nil
}
