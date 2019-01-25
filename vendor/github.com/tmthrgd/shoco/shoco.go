// Copyright 2016 Tom Thorogood. All rights reserved.
// Use of this source code is governed by a
// Modified BSD License license that can be found in
// the LICENSE file.
//
// Copyright 2014 Christian Schramm. All rights reserved.
// Use of this source code is governed by a MIT-style
// license that can be found in the LICENSE file.

// Package shoco is a compressor for small text strings based on the shoco C
// library.
package shoco

import (
	"bytes"
	"encoding/binary"
	"errors"
	"sync"
)

// ErrInvalid is returned by decompress functions when the compressed input
// data is malformed.
var ErrInvalid = errors.New("shoco: invalid input")

// DefaultModel is the default model used by the package level functions.
var DefaultModel = WordsEnModel

// Compress uses DefaultModel to compress the input data.
func Compress(in []byte) (out []byte) {
	return DefaultModel.Compress(in)
}

// ProposedCompress uses DefaultModel to compress the input data, it uses a
// shorter encoding for non-ASCII characters.
func ProposedCompress(in []byte) (out []byte) {
	return DefaultModel.ProposedCompress(in)
}

// Decompress uses DefaultModel to decompress the input data, it will return
// an error if the data is invalid.
func Decompress(in []byte) (out []byte, err error) {
	return DefaultModel.Decompress(in)
}

// ProposedDecompress uses DefaultModel to decompress the input data, it will
// return an error if the data is invalid. It requires the data to have been
// previously compressed with the shorter encoding produced by
// ProposedCompress.
func ProposedDecompress(in []byte) (out []byte, err error) {
	return DefaultModel.ProposedDecompress(in)
}

// Pack represents encoding data for a shoco compression model.
type Pack struct {
	Word          uint32
	BytesPacked   int
	BytesUnpacked int
	Offsets       [8]uint
	Masks         [8]int16
}

func (p *Pack) checkIndices(indices *[8]int16) bool {
	for i := 0; i < p.BytesUnpacked; i++ {
		if indices[i] > p.Masks[i] {
			return false
		}
	}

	return true
}

// Model represents a shoco compression model.
//
// It can be generated using the generate_compressor_model.py script in
// Ed-von-Schleck/shoco. The output of that script will require conversion to
// Go code.
// The script is available at: https://github.com/Ed-von-Schleck/shoco/blob/4dee0fc850cdec2bdb911093fe0a6a56e3623b71/generate_compressor_model.py.
type Model struct {
	check sync.Once

	ChrsByChrID                 []byte
	ChrIdsByChr                 [256]int8
	SuccessorIDsByChrIDAndChrID [][]int8
	ChrsByChrAndSuccessorID     [][]byte
	Packs                       []Pack
	MinChr                      byte
	MaxSuccessorN               int
}

func (m *Model) checkValid() {
	const invalidModel = "shoco: invalid model"

	if len(m.ChrsByChrID) == 0 ||
		len(m.SuccessorIDsByChrIDAndChrID) != len(m.ChrsByChrID) ||
		len(m.ChrsByChrAndSuccessorID) == 0 || len(m.Packs) == 0 ||
		m.MaxSuccessorN > 7 {
		panic(invalidModel)
	}

	for _, s := range m.SuccessorIDsByChrIDAndChrID {
		if len(s) != len(m.ChrsByChrID) {
			panic(invalidModel)
		}
	}

	for _, p := range m.Packs {
		if p.BytesPacked == 0 || p.BytesPacked > 4 || p.BytesPacked&(p.BytesPacked-1) != 0 ||
			p.BytesUnpacked == 0 || p.BytesUnpacked > 8 || p.BytesUnpacked&(p.BytesUnpacked-1) != 0 {
			panic(invalidModel)
		}
	}
}

func (m *Model) findBestEncoding(indices *[8]int16, nConsecutive int) int {
	for p := len(m.Packs) - 1; p >= 0; p-- {
		if nConsecutive >= m.Packs[p].BytesUnpacked && m.Packs[p].checkIndices(indices) {
			return p
		}
	}

	return -1
}

// Compress uses the given model to compress the input data.
func (m *Model) Compress(in []byte) (out []byte) {
	return m.compress(in, false)
}

// ProposedCompress uses the given model to compress the input data, it uses a
// shorter encoding for non-ASCII characters.
func (m *Model) ProposedCompress(in []byte) (out []byte) {
	return m.compress(in, true)
}

func (m *Model) compress(in []byte, proposed bool) (out []byte) {
	m.check.Do(m.checkValid)

	var buf bytes.Buffer
	buf.Grow(len(in))

	var indices [8]int16

	for len(in) != 0 {
		// find the longest string of known successors
		indices[0] = int16(m.ChrIdsByChr[in[0]])

		if lastChrIndex := indices[0]; lastChrIndex >= 0 {
			nConsecutive := 1
			for ; nConsecutive <= m.MaxSuccessorN && nConsecutive < len(in); nConsecutive++ {
				currentIndex := m.ChrIdsByChr[in[nConsecutive]]
				if currentIndex < 0 { // '\0' is always -1
					break
				}

				sucessorIndex := m.SuccessorIDsByChrIDAndChrID[lastChrIndex][currentIndex]
				if sucessorIndex < 0 {
					break
				}

				indices[nConsecutive] = int16(sucessorIndex)
				lastChrIndex = int16(currentIndex)
			}

			if nConsecutive >= 2 {
				if packN := m.findBestEncoding(&indices, nConsecutive); packN >= 0 {
					code := m.Packs[packN].Word
					for i := 0; i < m.Packs[packN].BytesUnpacked; i++ {
						code |= uint32(indices[i]) << m.Packs[packN].Offsets[i]
					}

					var codeBuf [4]byte
					binary.BigEndian.PutUint32(codeBuf[:], code)
					buf.Write(codeBuf[:m.Packs[packN].BytesPacked])

					in = in[m.Packs[packN].BytesUnpacked:]
					continue
				}
			}
		}

		if proposed {
			// See https://github.com/Ed-von-Schleck/shoco/issues/11
			if in[0]&0x80 != 0 || in[0] < 0x09 {
				j := byte(1)
				for ; int(j) < len(in) && j < 0x09 && (in[j]&0x80 != 0 || in[j] < 0x09); j++ {
				}

				buf.Grow(1 + int(j))
				buf.WriteByte(j - 1)
				buf.Write(in[:j])
				in = in[j:]
			} else {
				buf.WriteByte(in[0])
				in = in[1:]
			}

			continue
		}

		if in[0]&0x80 != 0 || in[0] == 0x00 { // non-ascii case or NUL char
			// Encoding NUL chars in this way is not compatible with
			// shoco_compress. shoco_compress terminates the compression
			// upon encountering a NUL char. shoco_decompress will
			// nonetheless correctly decode compressed strings that
			// contained NUL chars.

			buf.Grow(2)
			buf.WriteByte(0x00) // put in a sentinel byte
		}

		buf.WriteByte(in[0])
		in = in[1:]
	}

	return buf.Bytes()
}

// Decompress uses the given model to decompress the input data, it will return
// an error if the data is invalid.
func (m *Model) Decompress(in []byte) (out []byte, err error) {
	return m.decompress(in, false)
}

// ProposedDecompress uses the given model to decompress the input data, it
// will return an error if the data is invalid. It requires the data to have
// been previously compressed with the shorter encoding produced by
// ProposedCompress.
func (m *Model) ProposedDecompress(in []byte) (out []byte, err error) {
	return m.decompress(in, true)
}

func (m *Model) decompress(in []byte, proposed bool) (out []byte, err error) {
	m.check.Do(m.checkValid)

	var buf bytes.Buffer
	buf.Grow(len(in) * 2)

	for len(in) != 0 {
		mark := -1
		for val := in[0]; val&0x80 != 0; val <<= 1 {
			mark++
		}

		if mark < 0 {
			if proposed {
				// See https://github.com/Ed-von-Schleck/shoco/issues/11
				if in[0] < 0x09 {
					j := in[0] + 1
					if len(in) < 1+int(j) {
						return nil, ErrInvalid
					}

					buf.Write(in[1 : 1+j])
					in = in[1+j:]
				} else {
					buf.WriteByte(in[0])
					in = in[1:]
				}

				continue
			}

			if in[0] == 0x00 { // ignore the sentinel value for non-ascii chars
				if len(in) < 2 {
					return nil, ErrInvalid
				}

				buf.WriteByte(in[1])
				in = in[2:]
			} else {
				buf.WriteByte(in[0])
				in = in[1:]
			}

			continue
		}

		if mark >= len(m.Packs) || m.Packs[mark].BytesPacked > len(in) {
			return nil, ErrInvalid
		}

		buf.Grow(m.Packs[mark].BytesUnpacked)

		var codeBuf [4]byte
		copy(codeBuf[:], in[:m.Packs[mark].BytesPacked])
		code := binary.BigEndian.Uint32(codeBuf[:])

		offset, mask := m.Packs[mark].Offsets[0], m.Packs[mark].Masks[0]

		idx := (code >> offset) & uint32(mask)
		if int(idx) >= len(m.ChrsByChrID) {
			return nil, ErrInvalid
		}

		lastChr := m.ChrsByChrID[idx]
		buf.WriteByte(lastChr)

		for i := 1; i < m.Packs[mark].BytesUnpacked; i++ {
			offset, mask := m.Packs[mark].Offsets[i], m.Packs[mark].Masks[i]

			idx0, idx1 := lastChr-m.MinChr, (code>>offset)&uint32(mask)
			if int(idx0) >= len(m.ChrsByChrAndSuccessorID) ||
				int(idx1) >= len(m.ChrsByChrAndSuccessorID[idx0]) {
				return nil, ErrInvalid
			}

			lastChr = m.ChrsByChrAndSuccessorID[idx0][idx1]
			buf.WriteByte(lastChr)
		}

		in = in[m.Packs[mark].BytesPacked:]
	}

	return buf.Bytes(), nil
}
