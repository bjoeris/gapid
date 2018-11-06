// Copyright (C) 2018 Google Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package dependencygraph2

import (
	"context"
	"math/bits"
	"reflect"

	"github.com/google/gapid/core/math/interval"
	"github.com/google/gapid/gapis/api"

	"github.com/google/gapid/gapis/memory"
)

type MemoryAccess struct {
	Node NodeID
	Pool memory.PoolID
	Span interval.U64Span
	Mode AccessMode
	Deps []NodeID
}

type MemWatcher interface {
	CmdWatcher
	OnWriteSlice(ctx context.Context, cmdCtx CmdContext, s memory.Slice)
	OnReadSlice(ctx context.Context, cmdCtx CmdContext, s memory.Slice)
	OnWriteObs(ctx context.Context, cmdCtx CmdContext, obs []api.CmdObservation)
	OnReadObs(ctx context.Context, cmdCtx CmdContext, obs []api.CmdObservation)
	Flush(ctx context.Context, cmdCtx CmdContext)
	NodeAccesses() map[NodeID][]MemoryAccess
}

func NewMemWatcher(addNode func(Node) NodeID) MemWatcher {
	return &memWatcher{
		pendingAccesses: make(map[memory.PoolID]*memoryAccessList),
		memoryWrites:    make(map[memory.PoolID]*memoryWriteList),
		nodeAccesses:    make(map[NodeID][]MemoryAccess),
		addNode:         addNode,
	}
}

type memWatcher struct {
	pendingAccesses map[memory.PoolID]*memoryAccessList
	memoryWrites    map[memory.PoolID]*memoryWriteList
	nodeAccesses    map[NodeID][]MemoryAccess
	addNode         func(Node) NodeID
	isPostFence     bool
	stats           struct {
		// The distribution of the number of relevant writes for each read
		RelevantWriteDist Distribution
	}
}

func (b *memWatcher) OnWriteSlice(ctx context.Context, cmdCtx CmdContext, slice memory.Slice) {
	span := interval.U64Span{
		Start: slice.Base(),
		End:   slice.Base() + slice.Size(),
	}
	if list, ok := b.pendingAccesses[slice.Pool()]; ok {
		list.AddWrite(span)
	} else {
		b.pendingAccesses[slice.Pool()] = &memoryAccessList{memoryAccess{ACCESS_WRITE, span}}
	}
}

func (b *memWatcher) OnReadSlice(ctx context.Context, cmdCtx CmdContext, slice memory.Slice) {
	span := interval.U64Span{
		Start: slice.Base(),
		End:   slice.Base() + slice.Size(),
	}
	if list, ok := b.pendingAccesses[slice.Pool()]; ok {
		list.AddRead(span)
	} else {
		b.pendingAccesses[slice.Pool()] = &memoryAccessList{memoryAccess{ACCESS_READ, span}}
	}
}

func (b *memWatcher) OnWriteObs(ctx context.Context, cmdCtx CmdContext, obs []api.CmdObservation) {
	for i, o := range obs {
		b.addObs(ctx, cmdCtx, i, o, true)
	}
}

func (b *memWatcher) OnReadObs(ctx context.Context, cmdCtx CmdContext, obs []api.CmdObservation) {
	for i, o := range obs {
		b.addObs(ctx, cmdCtx, i, o, false)
	}
}

func (b *memWatcher) Flush(ctx context.Context, cmdCtx CmdContext) {
	nodeID := cmdCtx.nodeID
	memAccesses := b.nodeAccesses[nodeID]

	// Compute the maximum possible of size of memAccesses at the end of `Flush`.
	memAccessesCap := len(memAccesses)
	for _, acc := range b.pendingAccesses {
		memAccessesCap += len(*acc)
	}

	// Ensure that fragAccesses has sufficient capacity
	if memAccessesCap > cap(memAccesses) {
		// round up to next power of 2
		memAccessesCap = 1 << uint(bits.Len(uint(memAccessesCap))+1)

		newMemAccesses := make([]MemoryAccess, len(memAccesses), memAccessesCap)
		copy(newMemAccesses, memAccesses)
		memAccesses = newMemAccesses
	}

	for poolID, accessList := range b.pendingAccesses {
		for _, access := range *accessList {
			writeNodes := []NodeID{}
			used := false
			if access.mode&ACCESS_READ != 0 {
				writeNodes = applyMemRead(b.memoryWrites, poolID, access.span)
				b.stats.RelevantWriteDist.Add(uint64(len(writeNodes)))
				used = used || len(writeNodes) > 0
			}

			if access.mode&ACCESS_WRITE != 0 && poolID != 0 {
				used = used || applyMemWrite(b.memoryWrites, poolID, nodeID, access.span)
			}

			if used {
				memAccesses = append(memAccesses, MemoryAccess{
					Node: nodeID,
					Pool: poolID,
					Span: access.span,
					Mode: access.mode & ACCESS_READ_WRITE,
					Deps: writeNodes,
				})
			}
		}
	}
	b.nodeAccesses[nodeID] = memAccesses
	b.pendingAccesses = make(map[memory.PoolID]*memoryAccessList)
}

func (b *memWatcher) NodeAccesses() map[NodeID][]MemoryAccess {
	return b.nodeAccesses
}

func (b *memWatcher) OnBeginCmd(ctx context.Context, cmdCtx CmdContext) {
	b.isPostFence = false
}
func (b *memWatcher) OnEndCmd(ctx context.Context, cmdCtx CmdContext) {
	b.pendingAccesses = make(map[memory.PoolID]*memoryAccessList)
	b.nodeAccesses = make(map[NodeID][]MemoryAccess)
}
func (b *memWatcher) OnBeginSubCmd(ctx context.Context, cmdCtx CmdContext) {}
func (b *memWatcher) OnEndSubCmd(ctx context.Context, cmdCtx CmdContext)   {}

func (b *memWatcher) addObs(ctx context.Context, cmdCtx CmdContext, index int, obs api.CmdObservation, isWrite bool) {
	nodeID := b.getObsNodeID(obs, cmdCtx.cmdID, isWrite, index)
	if isWrite && !b.isPostFence {
		b.Flush(ctx, cmdCtx)
		b.isPostFence = true
	}
	span := obs.Range.Span()
	applyMemWrite(b.memoryWrites, obs.Pool, nodeID, span)
	b.nodeAccesses[nodeID] = []MemoryAccess{
		MemoryAccess{
			Node: nodeID,
			Pool: obs.Pool,
			Span: span,
			Mode: ACCESS_WRITE,
		}}
}

func (b *memWatcher) getObsNodeID(cmdObservation api.CmdObservation,
	cmdID api.CmdID, isWrite bool, index int) NodeID {
	nodeID := b.addNode(ObsNode{
		CmdObservation: cmdObservation,
		CmdID:          cmdID,
		IsWrite:        isWrite,
		Index:          index,
	})
	return nodeID
}

// observationSlice constructs a Slice from a CmdObservation
func observationSlice(obs api.CmdObservation) memory.Slice {
	return memory.NewSlice(obs.Range.Base, obs.Range.Base, obs.Range.Size, obs.Range.Size, obs.Pool, reflect.TypeOf(memory.Char(0)))
}

func applyMemWrite(wmap map[memory.PoolID]*memoryWriteList,
	p memory.PoolID, n NodeID, s interval.U64Span) bool {
	if writes, ok := wmap[p]; ok {
		i := interval.Replace(writes, s)
		w := &(*writes)[i]
		if w.node != n {
			w.node = n
			return true
		}
	} else {
		wmap[p] = &memoryWriteList{
			memoryWrite{
				node: n,
				span: s,
			},
		}
		return true
	}
	return false
}

func applyMemRead(wmap map[memory.PoolID]*memoryWriteList,
	p memory.PoolID, s interval.U64Span) []NodeID {
	writeNodes := []NodeID{}
	if writes, ok := wmap[p]; ok {
		i, c := interval.Intersect(writes, s)
		depSet := map[NodeID]struct{}{}
		for _, w := range (*writes)[i : i+c] {
			depSet[w.node] = struct{}{}
		}
		writeNodes = make([]NodeID, 0, len(depSet))
		for d, _ := range depSet {
			writeNodes = append(writeNodes, d)
		}
	}
	return writeNodes
}
