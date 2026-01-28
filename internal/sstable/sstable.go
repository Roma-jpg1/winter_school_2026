package sstable

import (
	"bytes"
	"encoding/binary"
	"errors"
	"io"
	"os"
)

type SparseIndex struct {
	startKey []byte
	endKey   []byte
	size     int
	offset   int64
}

func (si SparseIndex) Offset() int64 {
	return si.offset
}

type KeyValue struct {
	Key   []byte
	Value []byte
}

type SSTable struct {
	file         *os.File
	sparseIndexs []SparseIndex
	blockSize    int
}

func (s *SSTable) File() *os.File {
	return s.file
}

func (s *SSTable) SparseIndexs() []SparseIndex {
	return s.sparseIndexs
}

func (s *SSTable) ReadBlockFromOffset(offset int64) ([]KeyValue, error) {
	return s.readBlockFromOffset(offset)
}

func NewSSTable(file *os.File, blockSize int) *SSTable {
	return &SSTable{
		file:      file,
		blockSize: blockSize,
	}
}

func (s *SSTable) Close() error {
	if s.file != nil {
		return s.file.Close()
	}
	return nil
}

func (s *SSTable) BuildSparseIndex() error {
	_, err := s.file.Seek(0, 0)
	if err != nil {
		return err
	}

	var sparseIndex []SparseIndex
	blockOffset := int64(0)

	for {
		blockData, err := s.readBlockFromOffset(blockOffset)
		if err != nil {
			return err
		}

		if len(blockData) == 0 {
			break
		}

		startKey := blockData[0].Key
		endKey := blockData[len(blockData)-1].Key
		blockSize := 0

		for _, kv := range blockData {
			blockSize += int(binary.Size(int32(0))) + len(kv.Key) + int(binary.Size(int32(0))) + len(kv.Value)
		}

		sparseIndex = append(sparseIndex, SparseIndex{
			startKey: startKey,
			endKey:   endKey,
			size:     blockSize,
			offset:   blockOffset,
		})

		blockOffset += int64(blockSize)
	}

	s.sparseIndexs = sparseIndex
	return nil
}

func (s *SSTable) WriteBlock(blockData []KeyValue) error {
	for _, kv := range blockData {
		err := binary.Write(s.file, binary.BigEndian, int32(len(kv.Key)))
		if err != nil {
			return err
		}

		_, err = s.file.Write(kv.Key)
		if err != nil {
			return err
		}

		err = binary.Write(s.file, binary.BigEndian, int32(len(kv.Value)))
		if err != nil {
			return err
		}

		_, err = s.file.Write(kv.Value)
		if err != nil {
			return err
		}
	}
	return nil
}

func (s *SSTable) readBlockFromOffset(startOffset int64) ([]KeyValue, error) {
	var result []KeyValue

	_, err := s.file.Seek(startOffset, 0)
	if err != nil {
		return nil, err
	}

	for {
		var keyLen int32
		err = binary.Read(s.file, binary.BigEndian, &keyLen)
		if err != nil {
			if errors.Is(err, io.EOF) {
				return result, nil
			}
			return nil, err
		}

		if keyLen <= 0 {
			return result, nil
		}

		key := make([]byte, keyLen)
		_, err = s.file.Read(key)
		if err != nil {
			return nil, err
		}

		var valueLen int32
		err = binary.Read(s.file, binary.BigEndian, &valueLen)
		if err != nil {
			return nil, err
		}

		if valueLen < 0 {
			return result, nil
		}

		value := make([]byte, valueLen)
		_, err = s.file.Read(value)
		if err != nil {
			return nil, err
		}

		result = append(result, KeyValue{
			Key:   key,
			Value: value,
		})
	}
}

func (s *SSTable) binarySearchInBlock(sp SparseIndex, key []byte) []byte {
	blockData, err := s.readBlockFromOffset(sp.offset)
	if err != nil {
		return nil
	}

	left := 0
	right := len(blockData) - 1

	for left <= right {
		mid := left + (right-left)/2
		cmp := bytes.Compare(blockData[mid].Key, key)

		if cmp == 0 {
			return blockData[mid].Value
		} else if cmp < 0 {
			left = mid + 1
		} else {
			right = mid - 1
		}
	}

	return nil
}

func (s *SSTable) GetValue(key []byte) []byte {
	for _, sp := range s.sparseIndexs {
		if bytes.Compare(sp.startKey, key) <= 0 && bytes.Compare(key, sp.endKey) <= 0 {
			return s.binarySearchInBlock(sp, key)
		}
	}
	return nil
}
