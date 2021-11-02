// Copyright 2019 The go-ethereum Authors
// This file is part of the go-ethereum library.
//
// The go-ethereum library is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// The go-ethereum library is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with the go-ethereum library. If not, see <http://www.gnu.org/licenses/>.

package trie

import (
	"bytes"
	"math/rand"
	"testing"

	"github.com/VictoriaMetrics/fastcache"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/ethdb/memorydb"
)

// randomHash generates a random blob of data and returns it as a hash.
func randomHash() common.Hash {
	var hash common.Hash
	if n, err := rand.Read(hash[:]); n != common.HashLength || err != nil {
		panic(err)
	}
	return hash
}

func randomNode() *cachedNode {
	val := randBytes(100)
	return &cachedNode{
		hash: crypto.Keccak256Hash(val),
		node: rawNode(val),
		size: 100,
	}
}

func emptyLayer() *diskLayer {
	return &diskLayer{
		diskdb: memorydb.New(),
		cache:  fastcache.New(500 * 1024),
	}
}

func benchmarkSearch(b *testing.B, depth int) {
	var (
		target     []byte
		targetHash common.Hash
		want       []byte
	)
	// First, we set up 128 diff layers, with 3K items each
	fill := func(parent snapshot, index int) *diffLayer {
		var nodes = make(map[string]*cachedNode)
		for i := 0; i < 3000; i++ {
			var (
				path    = randomHash().Bytes()
				storage = EncodeStorageKey(common.Hash{}, path)
				val     = randomNode()
			)
			nodes[string(storage)] = val
			if target == nil && depth == index {
				want = val.rlp()
				target = append([]byte{}, storage...)
				targetHash = val.hash
			}
		}
		return newDiffLayer(parent, common.Hash{}, nodes)
	}
	var layer snapshot
	layer = emptyLayer()
	for i := 0; i < 128; i++ {
		layer = fill(layer, i)
	}
	b.ResetTimer()
	var (
		have []byte
		err  error
	)
	for i := 0; i < b.N; i++ {
		have, err = layer.NodeBlob(target, targetHash)
		if err != nil {
			b.Fatal(err)
		}
	}
	if !bytes.Equal(have, want) {
		b.Fatalf("have %x want %x", have, want)
	}
}

// BenchmarkSearchBottom benchmarks the search hits in the bottom diff layer.

// cpu: Intel(R) Core(TM) i5-7360U CPU @ 2.30GHz
// BenchmarkSearchBottom
// BenchmarkSearchBottom-4   	  222717	      6167 ns/op
func BenchmarkSearchBottom(b *testing.B) { benchmarkSearch(b, 0) }

// BenchmarkSearchBottom benchmarks the search hits in the top diff layer.
//
// cpu: Intel(R) Core(TM) i5-7360U CPU @ 2.30GHz
// BenchmarkSearchTop
// BenchmarkSearchTop-4   	10910677	       111.8 ns/op
func BenchmarkSearchTop(b *testing.B) { benchmarkSearch(b, 127) }
