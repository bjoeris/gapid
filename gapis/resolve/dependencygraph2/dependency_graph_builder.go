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
	"fmt"
	"reflect"
	"sort"
	"strings"

	"github.com/google/gapid/core/app/benchmark"
	"github.com/google/gapid/core/app/status"
	"github.com/google/gapid/core/log"
	"github.com/google/gapid/core/math/interval"
	"github.com/google/gapid/gapis/api"
	"github.com/google/gapid/gapis/capture"
	"github.com/google/gapid/gapis/config"
	"github.com/google/gapid/gapis/memory"
)

var (
	dependencyGraphBuilderCounter = benchmark.Duration("DependencyGraph.Builder")
)

type refAccesses struct {
	m map[api.RefID]fragAccesses
}

func newRefAccesses() refAccesses {
	return refAccesses{m: make(map[api.RefID]fragAccesses)}
}

func (a refAccesses) Get(ref api.RefID) (fragAccesses, bool) {
	if v, ok := a.m[ref]; ok {
		return v, true
	}
	return fragAccesses{}, false
}

func (a refAccesses) Set(ref api.RefID, v fragAccesses) {
	a.m[ref] = v
}

type fragAccesses struct {
	m api.FragmentMap
}

func newFragAccesses(owner api.RefObject) fragAccesses {
	return fragAccesses{owner.NewFragmentMap()}
}

type fragAccess struct {
	// Access mode by each subcommand level
	accessStack []nodeFragAccess

	// Access mode for unflushed accesses
	pending AccessMode
}

type nodeFragAccess struct {
	node NodeID
	mode AccessMode
}

func (a fragAccess) GetMode(depth int, node NodeID) AccessMode {
	if depth >= len(a.accessStack) {
		return 0
	}
	v := a.accessStack[depth]
	if v.node == node {
		return v.mode
	}
	return 0
}

func (a *fragAccess) grow(depth int) {
	if depth >= len(a.accessStack) {
		n := 1
		for n <= depth {
			n *= 2
		}
		newAccessModes := make([]nodeFragAccess, n)
		copy(newAccessModes, a.accessStack)
		a.accessStack = newAccessModes
	}
}

func (a *fragAccess) AddRead(depth int, node NodeID) {
	a.grow(depth)
	v := &a.accessStack[depth]
	if v.node == node {
		v.mode |= ACCESS_READ
	} else {
		v.node = node
		v.mode = ACCESS_READ
	}
	a.pending |= ACCESS_READ
}

func (a *fragAccess) AddWrite(depth int, node NodeID) {
	a.grow(depth)
	v := &a.accessStack[depth]
	if v.node == node {
		v.mode |= ACCESS_WRITE
	} else {
		v.node = node
		v.mode = ACCESS_WRITE
	}
	a.pending |= ACCESS_WRITE
}

func (a *fragAccess) ClearBelow(depth int) {
	for i := depth; i < len(a.accessStack); i++ {
		a.accessStack[i].node = NodeNoID
		a.accessStack[i].mode = 0
	}
}

func (a fragAccesses) Get(f api.Fragment) (*fragAccess, bool) {
	if v, ok := a.m.Get(f); ok {
		return v.(*fragAccess), true
	}
	return nil, false
}

func (a fragAccesses) Set(f api.Fragment, v *fragAccess) {
	a.m.Set(f, v)
}

func (a fragAccesses) Delete(f api.Fragment) {
	a.m.Delete(f)
}

func (a fragAccesses) Clear() {
	a.m.Clear()
}

func (a fragAccesses) ForeachFrag(f func(api.Fragment, *fragAccess) error) error {
	return a.m.ForeachFrag(func(frag api.Fragment, v interface{}) error {
		if fa, ok := v.(*fragAccess); ok {
			return f(frag, fa)
		}
		return fmt.Errorf("fragAccesses FragmentMap contains unexpected value type %T", v)
	})
}

type currentCmdState struct {
	cmdID api.CmdID

	// cmd is the command currently being processed
	cmd api.Cmd

	// subCmdIdx stores the SubCmdIdx of the current subcommand
	subCmdIdx api.SubCmdIdx

	// nodeID is the NodeID of the dependency graph node corresponding to the currentCmdID
	nodeID NodeID

	// createCount is a count of how many CreateResource effects are associated with currentNodeID
	createCount int

	// isPostFence is true after the command's fence has executed, and memory write observations are visible
	isPostFence bool

	// fragmentAccesses map[api.RefID]map[api.Fragment]AccessMode
	refAccesses refAccesses
	// fragmentAccesses map[api.RefID]api.FragmentMap
	memoryAccesses  map[memory.PoolID]*memoryAccessList
	forwardAccesses []ForwardAccess

	accessedFragments map[api.RefID][]api.Fragment

	depth int
}

func newCurrentCmdState() *currentCmdState {
	return &currentCmdState{
		cmdID:             api.CmdNoID,
		nodeID:            NodeNoID,
		accessedFragments: make(map[api.RefID][]api.Fragment),
		refAccesses:       newRefAccesses(),
		memoryAccesses:    make(map[memory.PoolID]*memoryAccessList),
	}
}

func (s *currentCmdState) setSubCmdIdx(idx api.SubCmdIdx, nodeID NodeID) {
	s.subCmdIdx = append(s.subCmdIdx[:0], idx...)
	s.nodeID = nodeID
	s.forwardAccesses = s.forwardAccesses[:0]
}

func (s *currentCmdState) reset() {
	s.cmdID = api.CmdNoID
	s.cmd = nil
	s.createCount = 0
	s.accessedFragments = make(map[api.RefID][]api.Fragment)
	s.refAccesses = newRefAccesses()
	s.memoryAccesses = make(map[memory.PoolID]*memoryAccessList)
	s.setSubCmdIdx(api.SubCmdIdx{}, NodeNoID)
}

type NodeStats struct {
	NumFragReads         uint64
	NumFragWrites        uint64
	NumMemReads          uint64
	NumMemWrites         uint64
	NumForwardDepOpens   uint64
	NumForwardDepCloses  uint64
	NumForwardDepDrops   uint64
	UniqueFragReads      uint64
	UniqueFragWrites     uint64
	UniqueMemReads       uint64
	UniqueMemWrites      uint64
	UniqueDeps           uint64
	fragmentAccessRanges []interval.U64Span
	memoryAccessRanges   []interval.U64Span
	forwardAccessRanges  []interval.U64Span
}

type AccessMode uint

const (
	ACCESS_READ       AccessMode = 1 << 0
	ACCESS_WRITE      AccessMode = 1 << 1
	ACCESS_READ_WRITE AccessMode = ACCESS_READ | ACCESS_WRITE
)

type FragmentAccess struct {
	Node     NodeID
	Ref      api.RefID
	Fragment api.Fragment
	Mode     AccessMode
	Deps     []NodeID
}

type MemoryAccess struct {
	Node NodeID
	Pool memory.PoolID
	Span interval.U64Span
	Mode AccessMode
	Deps []NodeID
}

type ForwardAccessMode uint

const (
	FORWARD_OPEN ForwardAccessMode = iota + 1
	FORWARD_CLOSE
	FORWARD_DROP
)

type ForwardAccess struct {
	Node         NodeID
	DependencyID interface{}
	Mode         ForwardAccessMode
	Dep          *NodeID
}

type refWrites struct {
	frags     fragWrites
	nodeSlice []NodeID
	nodeSet   map[NodeID]struct{}
}

func newRefWrites(frags api.FragmentMap) *refWrites {
	return &refWrites{
		frags:     fragWrites{frags},
		nodeSlice: []NodeID{},
		nodeSet:   make(map[NodeID]struct{}),
	}
}

type fragWrites struct {
	m api.FragmentMap
}

func (w fragWrites) Get(f api.Fragment) (NodeID, bool) {
	v, ok := w.m.Get(f)
	if ok {
		return v.(NodeID), true
	}
	return NodeNoID, false
}

func (w fragWrites) Set(f api.Fragment, nodeID NodeID) {
	w.m.Set(f, nodeID)
}

func (w fragWrites) Clear() {
	w.m.Clear()
}

// The data needed to build a dependency graph by iterating through the commands in a trace
type dependencyGraphBuilder struct {
	// graph is the dependency graph being constructed
	// graph *dependencyGraph
	capture *capture.Capture

	config DependencyGraphConfig

	// current contains the state of the command node currently being processed
	current *currentCmdState

	// subCmdStack stores the SubCmdIdx of the nested subcommands
	subCmdStack []api.SubCmdIdx

	fragmentAccesses []FragmentAccess
	memoryAccesses   []MemoryAccess
	forwardAccesses  []ForwardAccess

	// fragmentWrites stores all non-overwritten writes to the state, excluding memory writes.
	// fragmentWrites[ref][frag] gives the latest node to write to the fragment
	// frag in the object corresponding to ref
	refWrites map[api.RefID]*refWrites

	// memoryWrites stores all non-overwritten writes to memory ranges
	memoryWrites map[memory.PoolID]*memoryWriteList

	// openForwardDependencies tracks pending forward dependencies, where the
	// first node has been processed, but the second node has not yet been
	// processed. The keys are the unique identifier for the forward
	// dependency.
	openForwardDependencies map[interface{}]*NodeID

	Nodes      []Node
	CmdNodeIDs map[api.CmdID]*api.SubCmdIdxTrie

	StateRefs map[api.RefID]reflect.Type

	NodeStats []NodeStats

	Stats struct {
		NumFragReads        uint64
		NumFragWrites       uint64
		NumMemReads         uint64
		NumMemWrites        uint64
		NumForwardDepOpens  uint64
		NumForwardDepCloses uint64
		NumForwardDepDrops  uint64
		UniqueFragReads     uint64
		UniqueFragWrites    uint64
		UniqueMemReads      uint64
		UniqueMemWrites     uint64
		UniqueDeps          uint64
		NumCmdNodes         uint64
		NumObsNodes         uint64
	}
}

// Build a new dependencyGraphBuilder.
func newDependencyGraphBuilder(ctx context.Context, config DependencyGraphConfig,
	c *capture.Capture, initialCmds []api.Cmd) *dependencyGraphBuilder {
	builder := &dependencyGraphBuilder{
		capture:                 c,
		config:                  config,
		current:                 newCurrentCmdState(),
		refWrites:               make(map[api.RefID]*refWrites),
		memoryWrites:            make(map[memory.PoolID]*memoryWriteList),
		openForwardDependencies: make(map[interface{}]*NodeID),
		CmdNodeIDs:              make(map[api.CmdID]*api.SubCmdIdxTrie),
		StateRefs:               make(map[api.RefID]reflect.Type),
	}
	return builder
}

// BeginCmd is called at the beginning of each API call
func (b *dependencyGraphBuilder) OnBeginCmd(ctx context.Context, cmdID api.CmdID, cmd api.Cmd) {
	b.debug(ctx, "OnBeginCmd [%d] %v", cmdID, cmd)
	b.current.cmdID = cmdID
	b.current.cmd = cmd
	b.current.nodeID = b.getCmdNodeID(cmdID, api.SubCmdIdx{})
	b.current.isPostFence = false
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

type accessInfo struct {
	mode         AccessMode // what has actually been done in this block
	prevAccessed uint32     // bits indicating whether this has been accessed by this node in previous blocks, without an intervening write by a different node
}

func (b *dependencyGraphBuilder) flushFragmentAccesses(ctx context.Context) {
	n := b.current.nodeID
	stats := &b.NodeStats[n]
	fa := interval.U64Span{}
	fa.Start = (uint64)(len(b.fragmentAccesses))
	for refID, frags := range b.current.accessedFragments {
		refFrags, _ := b.current.refAccesses.Get(refID)
		writes, hasWrites := b.refWrites[refID]

		ca, hasCa := refFrags.Get(api.CompleteFragment{})
		if hasCa && hasWrites && ca.pending&ACCESS_READ != 0 {
			// Read of complete fragment
			// => depend on each write
			writeNodes := writes.nodeSlice
			if len(writeNodes) > 0 {
				b.fragmentAccesses = append(b.fragmentAccesses, FragmentAccess{
					Node:     b.current.nodeID,
					Fragment: api.CompleteFragment{},
					Ref:      refID,
					Mode:     ACCESS_READ,
					Deps:     writeNodes,
				})
			}
			ca.pending &^= ACCESS_READ
		}

		for _, frag := range frags {
			if _, ok := frag.(api.CompleteFragment); ok {
				continue
			}
			fa, hasFa := refFrags.Get(frag)
			if hasFa && fa.pending != 0 {
				writeNodes := []NodeID{}
				if fa.pending&ACCESS_READ != 0 && hasWrites {
					if nodeID, ok := writes.frags.Get(frag); ok {
						writeNodes = []NodeID{nodeID}
					}
				}
				if fa.pending&ACCESS_WRITE != 0 {
					if writes == nil {
						writes = newRefWrites(refFrags.m.EmptyClone())
						b.refWrites[refID] = writes
					}
					writes.frags.Set(frag, b.current.nodeID)
					if _, ok := writes.nodeSet[b.current.nodeID]; !ok {
						writes.nodeSet[b.current.nodeID] = struct{}{}
						writes.nodeSlice = append(writes.nodeSlice, b.current.nodeID)
					}
				}
				if len(writeNodes) > 0 || fa.pending&ACCESS_WRITE != 0 {
					b.fragmentAccesses = append(b.fragmentAccesses, FragmentAccess{
						Node:     b.current.nodeID,
						Fragment: frag,
						Ref:      refID,
						Mode:     fa.pending,
						Deps:     writeNodes,
					})
				}
				fa.pending = 0
			}
		}

		if hasCa && ca.pending&ACCESS_WRITE != 0 {
			// Write of complete fragment
			// => replaces all writes
			if writes == nil {
				writes = newRefWrites(refFrags.m.EmptyClone())
				b.refWrites[refID] = writes
			}
			writes.frags.Clear()
			writes.frags.Set(api.CompleteFragment{}, b.current.nodeID)
			writes.nodeSlice = []NodeID{b.current.nodeID}
			writes.nodeSet = map[NodeID]struct{}{b.current.nodeID: struct{}{}}

			b.fragmentAccesses = append(b.fragmentAccesses, FragmentAccess{
				Node:     b.current.nodeID,
				Fragment: api.CompleteFragment{},
				Ref:      refID,
				Mode:     ACCESS_WRITE,
				Deps:     []NodeID{},
			})
			ca.pending &^= ACCESS_WRITE
		}
	}
	b.current.accessedFragments = make(map[api.RefID][]api.Fragment)
	fa.End = (uint64)(len(b.fragmentAccesses))
	if fa.End > fa.Start {
		stats.fragmentAccessRanges = append(stats.fragmentAccessRanges, fa)
	}
}

func (b *dependencyGraphBuilder) flushMemoryAccesses(ctx context.Context) {
	n := b.current.nodeID
	stats := &b.NodeStats[n]
	ma := interval.U64Span{}
	ma.Start = (uint64)(len(b.memoryAccesses))
	for poolID, accessList := range b.current.memoryAccesses {
		for _, access := range *accessList {
			writeNodes := []NodeID{}
			used := false
			if access.mode&ACCESS_READ != 0 {
				writeNodes = applyMemRead(b.memoryWrites, poolID, access.span)
				used = used || len(writeNodes) > 0
			}

			if access.mode&ACCESS_WRITE != 0 && poolID != 0 {
				used = used || applyMemWrite(b.memoryWrites, poolID, b.current.nodeID, access.span)
			}

			if used {
				b.memoryAccesses = append(b.memoryAccesses, MemoryAccess{
					Node: b.current.nodeID,
					Pool: poolID,
					Span: access.span,
					Mode: access.mode & ACCESS_READ_WRITE,
					Deps: writeNodes,
				})
			}
		}
	}
	b.current.memoryAccesses = make(map[memory.PoolID]*memoryAccessList)
	ma.End = (uint64)(len(b.memoryAccesses))
	if ma.End > ma.Start {
		stats.memoryAccessRanges = append(stats.memoryAccessRanges, ma)
	}
}

func (b *dependencyGraphBuilder) flushForwardAccesses(ctx context.Context) {
	n := b.current.nodeID
	stats := &b.NodeStats[n]
	if len(b.current.forwardAccesses) > 0 {
		fa := interval.U64Span{}
		fa.Start = (uint64)(len(b.forwardAccesses))
		b.forwardAccesses = append(b.forwardAccesses, b.current.forwardAccesses...)
		fa.End = (uint64)(len(b.forwardAccesses))
		stats.forwardAccessRanges = append(stats.forwardAccessRanges, fa)
	}
}

func (b *dependencyGraphBuilder) flushSubCmd(ctx context.Context) {
	// log.D(ctx, "flushSubCmd(%v)  frag: %d  mem: %d  rObs: %d  wObs: %d  f: %d",
	// 	append(api.SubCmdIdx{(uint64)(b.current.cmdID)}, b.current.subCmdIdx...),
	// 	len(b.current.fragmentAccesses),
	// 	len(b.current.memoryAccesses),
	// 	len(b.current.readObs),
	// 	len(b.current.writeObs),
	// 	len(b.current.forwardAccesses))

	b.flushFragmentAccesses(ctx)

	// Apply memory accesses
	b.flushMemoryAccesses(ctx)

	// Apply forward accesses
	b.flushForwardAccesses(ctx)
}

func (b *dependencyGraphBuilder) OnBeginSubCmd(ctx context.Context, subCmdIdx api.SubCmdIdx) {
	if b.config.MergeSubCmdNodes {
		return
	}
	b.flushSubCmd(ctx)

	b.subCmdStack = append(b.subCmdStack, b.current.subCmdIdx)
	nodeID := b.getCmdNodeID(b.current.cmdID, subCmdIdx)
	b.current.setSubCmdIdx(subCmdIdx, nodeID)
	b.current.depth++

	// log.D(ctx, "SetSubCmdIdx(%v)  frag: %d  mem: %d  rObs: %d  wObs: %d  f: %d",
	// 	append(api.SubCmdIdx{(uint64)(b.current.cmdID)}, subCmdIdx...),
	// 	len(b.current.fragmentAccesses),
	// 	len(b.current.memoryAccesses),
	// 	len(b.current.readObs),
	// 	len(b.current.writeObs),
	// 	len(b.current.forwardAccesses))
}

func (b *dependencyGraphBuilder) OnEndSubCmd(ctx context.Context) {
	if b.config.MergeSubCmdNodes {
		return
	}
	b.flushSubCmd(ctx)

	if len(b.subCmdStack) <= 0 {
		panic("EndSubCmd called while not processing any subcommand")
	}
	subCmdIdx := b.subCmdStack[len(b.subCmdStack)-1]
	b.subCmdStack = b.subCmdStack[:len(b.subCmdStack)-1]
	nodeID := b.getCmdNodeID(b.current.cmdID, subCmdIdx)
	b.current.setSubCmdIdx(subCmdIdx, nodeID)
	b.current.depth--

	// log.D(ctx, "SetSubCmdIdx(%v)  frag: %d  mem: %d  rObs: %d  wObs: %d  f: %d",
	// 	append(api.SubCmdIdx{(uint64)(b.current.cmdID)}, subCmdIdx...),
	// 	len(b.current.fragmentAccesses),
	// 	len(b.current.memoryAccesses),
	// 	len(b.current.readObs),
	// 	len(b.current.writeObs),
	// 	len(b.current.forwardAccesses))
}

// EndCmd is called at the end of each API call
func (b *dependencyGraphBuilder) OnEndCmd(ctx context.Context, cmdID api.CmdID, cmd api.Cmd) {
	b.debug(ctx, "OnEndCmd [%d] %v", cmdID, cmd)
	b.flushSubCmd(ctx)
	b.current.reset()
}

func (b *dependencyGraphBuilder) OnGet(ctx context.Context, owner api.RefObject, frag api.Fragment, valueRef api.RefObject) {
	b.NodeStats[b.current.nodeID].NumFragReads++
	b.Stats.NumFragReads++
	ownerID := owner.RefID()
	valueID := valueRef.RefID()
	if ownerID == api.NilRefID {
		return
	}
	if _, ok := b.StateRefs[ownerID]; !ok {
		if _, ok := owner.(api.State); ok {
			b.StateRefs[ownerID] = reflect.TypeOf(owner)
		} else {
			return
		}
	}
	b.debug(ctx, "  OnGet (%T %d)%v : %d", owner, ownerID, frag, valueID)

	// if ownerFrags, ok := b.current.fragmentAccesses[ownerID]; ok {
	if ownerFrags, ok := b.current.refAccesses.Get(ownerID); ok {
		fa, hasFa := ownerFrags.Get(frag)
		if hasFa {
			if fa.GetMode(b.current.depth, b.current.nodeID) != 0 {
				// This read is covered by an earlier access by this command
				return
			}
		}
		if a, ok := ownerFrags.Get(api.CompleteFragment{}); ok {
			if a.GetMode(b.current.depth, b.current.nodeID) != 0 {
				// This read is covered by an earlier complete access by this command
				return
			}
		}
		if !hasFa {
			fa = &fragAccess{}
			ownerFrags.Set(frag, fa)
		}
		fa.AddRead(b.current.depth, b.current.nodeID)
	} else {
		ownerFrags := newFragAccesses(owner)
		fa := &fragAccess{}
		fa.AddRead(b.current.depth, b.current.nodeID)
		ownerFrags.Set(frag, fa)
		b.current.refAccesses.Set(ownerID, ownerFrags)
	}
	b.current.accessedFragments[ownerID] = append(b.current.accessedFragments[ownerID], frag)
	if valueID != api.NilRefID {
		b.StateRefs[valueID] = reflect.TypeOf(valueRef)
	}
}

func (b *dependencyGraphBuilder) OnSet(ctx context.Context, owner api.RefObject, frag api.Fragment, oldValueRef api.RefObject, newValueRef api.RefObject) {
	b.NodeStats[b.current.nodeID].NumFragWrites++
	b.Stats.NumFragWrites++

	ownerID := owner.RefID()
	newValueID := newValueRef.RefID()
	if ownerID == api.NilRefID {
		return
	}
	if _, ok := b.StateRefs[ownerID]; !ok {
		if _, ok := owner.(api.State); ok {
			b.StateRefs[ownerID] = reflect.TypeOf(owner)
		} else {
			return
		}
	}
	b.debug(ctx, "  OnSet (%T %d)%v : %d → %d", owner, owner.RefID(), frag, oldValueRef.RefID(), newValueRef.RefID())

	if ownerFrags, ok := b.current.refAccesses.Get(ownerID); ok {
		fa, hasFa := ownerFrags.Get(frag)
		if hasFa {
			if fa.GetMode(b.current.depth, b.current.nodeID)&ACCESS_WRITE != 0 {
				// This write is covered by an earlier write by this command
				return
			}
		}
		ca, hasCa := ownerFrags.Get(api.CompleteFragment{})
		if hasCa {
			if ca.GetMode(b.current.depth, b.current.nodeID)&ACCESS_WRITE != 0 {
				// This write is covered by an earlier complete write by this command
				return
			}
		}
		if !hasFa {
			fa = &fragAccess{}
			ownerFrags.Set(frag, fa)
		}
		if _, ok := frag.(api.CompleteFragment); ok {
			ownerFrags.ForeachFrag(func(otherFrag api.Fragment, otherAcc *fragAccess) error {
				if otherFrag != frag {
					otherAcc.ClearBelow(b.current.depth + 1)
					otherAcc.pending &^= ACCESS_WRITE
				}
				return nil
			})
		} else {
			// At this point, frag is not CompleteFragment, and the write is
			// not covered by any previous writes by this node (neither to frag
			// nor to CompleteFragment).

			fa.ClearBelow(b.current.depth)
			if hasCa {
				ca.ClearBelow(b.current.depth)
			}
		}

		fa.AddWrite(b.current.depth, b.current.nodeID)
	} else {
		ownerFrags := newFragAccesses(owner)
		fa := &fragAccess{}
		fa.AddWrite(b.current.depth, b.current.nodeID)
		ownerFrags.Set(frag, fa)
		// b.current.fragmentAccesses[ownerID] = ownerFrags
		b.current.refAccesses.Set(ownerID, ownerFrags)
	}
	b.current.accessedFragments[ownerID] = append(b.current.accessedFragments[ownerID], frag)
	if newValueID != api.NilRefID {
		b.StateRefs[newValueID] = reflect.TypeOf(newValueRef)
	}
}

// OnWriteSlice is called when writing to a slice
func (b *dependencyGraphBuilder) OnWriteSlice(ctx context.Context, slice memory.Slice) {
	b.debug(ctx, "  OnWriteSlice: %v", slice)
	b.NodeStats[b.current.nodeID].NumMemWrites++
	b.Stats.NumMemWrites++
	span := interval.U64Span{
		Start: slice.Base(),
		End:   slice.Base() + slice.Size(),
	}
	if list, ok := b.current.memoryAccesses[slice.Pool()]; ok {
		list.AddWrite(span)
	} else {
		b.current.memoryAccesses[slice.Pool()] = &memoryAccessList{memoryAccess{ACCESS_WRITE, span}}
	}
}

// OnReadSlice is called when reading from a slice
func (b *dependencyGraphBuilder) OnReadSlice(ctx context.Context, slice memory.Slice) {
	b.debug(ctx, "  OnReadSlice: %v", slice)
	b.NodeStats[b.current.nodeID].NumMemReads++
	b.Stats.NumMemReads++
	span := interval.U64Span{
		Start: slice.Base(),
		End:   slice.Base() + slice.Size(),
	}
	if list, ok := b.current.memoryAccesses[slice.Pool()]; ok {
		list.AddRead(span)
	} else {
		b.current.memoryAccesses[slice.Pool()] = &memoryAccessList{memoryAccess{ACCESS_READ, span}}
	}
}

// observationSlice constructs a Slice from a CmdObservation
func observationSlice(obs api.CmdObservation) memory.Slice {
	return memory.NewSlice(obs.Range.Base, obs.Range.Base, obs.Range.Size, obs.Range.Size, obs.Pool, reflect.TypeOf(memory.Char(0)))
}

// OnWriteObs is called when a memory write observation becomes visible
func (b *dependencyGraphBuilder) OnWriteObs(ctx context.Context, obs []api.CmdObservation) {
	for i, o := range obs {
		b.addObs(ctx, i, o, true)
	}
}

// OnReadObs is called when a memory read observation becomes visible
func (b *dependencyGraphBuilder) OnReadObs(ctx context.Context, obs []api.CmdObservation) {
	for i, o := range obs {
		b.addObs(ctx, i, o, false)
	}
}

// OpenForwardDependency is called to begin a forward dependency.
// See `StateWatcher.OpenForwardDependency` for an explanation of forward dependencies.
func (b *dependencyGraphBuilder) OpenForwardDependency(ctx context.Context, dependencyID interface{}) {
	b.debug(ctx, "  OpenForwardDependency: %v", dependencyID)

	b.NodeStats[b.current.nodeID].NumForwardDepOpens++
	b.Stats.NumForwardDepOpens++

	dep := NodeNoID

	if _, ok := b.openForwardDependencies[dependencyID]; ok {
		log.I(ctx, "OpenForwardDependency: Forward dependency opened multiple times before being closed. DependencyID: %v, close node: %v", dependencyID, b.current.nodeID)
	} else {
		b.openForwardDependencies[dependencyID] = &dep
	}

	b.current.forwardAccesses = append(b.current.forwardAccesses, ForwardAccess{
		Node:         b.current.nodeID,
		DependencyID: dependencyID,
		Mode:         FORWARD_OPEN,
		Dep:          &dep,
	})
}

// CloseForwardDependency is called to end a forward dependency.
// See `StateWatcher.OpenForwardDependency` for an explanation of forward dependencies.
func (b *dependencyGraphBuilder) CloseForwardDependency(ctx context.Context, dependencyID interface{}) {
	b.debug(ctx, "  CloseForwardDependency: %v", dependencyID)

	b.NodeStats[b.current.nodeID].NumForwardDepCloses++
	b.Stats.NumForwardDepCloses++

	if openDep, ok := b.openForwardDependencies[dependencyID]; ok {
		delete(b.openForwardDependencies, dependencyID)
		*openDep = b.current.nodeID
	} else {
		log.I(ctx, "CloseForwardDependency: Forward dependency closed before being opened. DependencyID: %v, close node: %v", dependencyID, b.current.nodeID)
	}

	b.current.forwardAccesses = append(b.current.forwardAccesses, ForwardAccess{
		Node:         b.current.nodeID,
		DependencyID: dependencyID,
		Mode:         FORWARD_CLOSE,
		Dep:          nil,
	})
}

// DropForwardDependency is called to abandon a previously opened
// forward dependency, without actually adding the forward dependency.
// See `StateWatcher.OpenForwardDependency` for an explanation of forward dependencies.
func (b *dependencyGraphBuilder) DropForwardDependency(ctx context.Context, dependencyID interface{}) {
	b.debug(ctx, "  DropForwardDependency: %v", dependencyID)
	b.NodeStats[b.current.nodeID].NumForwardDepDrops++
	b.Stats.NumForwardDepDrops++
	delete(b.openForwardDependencies, dependencyID)
	b.current.forwardAccesses = append(b.current.forwardAccesses, ForwardAccess{
		Node:         b.current.nodeID,
		DependencyID: dependencyID,
		Mode:         FORWARD_DROP,
	})
}

func (b *dependencyGraphBuilder) addObs(ctx context.Context, index int, obs api.CmdObservation, isWrite bool) {
	nodeID := b.getObsNodeID(obs, b.current.cmdID, isWrite, index)
	if isWrite && !b.current.isPostFence {
		b.flushSubCmd(ctx)
		b.current.isPostFence = true
	}
	span := obs.Range.Span()
	applyMemWrite(b.memoryWrites, obs.Pool, nodeID, span)
	b.memoryAccesses = append(b.memoryAccesses,
		MemoryAccess{
			Node: nodeID,
			Pool: obs.Pool,
			Span: span,
			Mode: ACCESS_WRITE,
		})
}

// LogStats logs some interesting stats about the graph construction
func (b *dependencyGraphBuilder) LogStats(ctx context.Context, graph DependencyGraph) {
	log.I(ctx, "Dependency Graph Stats:")
	log.I(ctx, "          NumCmdNodes: %-8v  NumObsNodes: %v", b.Stats.NumCmdNodes, b.Stats.NumObsNodes)
	log.I(ctx, "          NumFragReads: %-8v  UniqueFragReads: %v", b.Stats.NumFragReads, b.Stats.UniqueFragReads)
	log.I(ctx, "          NumFragWrites: %-7v  UniqueFragWrites: %v", b.Stats.NumFragWrites, b.Stats.UniqueFragWrites)
	log.I(ctx, "          NumMemReads: %-9v  UniqueMemReads: %v", b.Stats.NumMemReads, b.Stats.UniqueMemReads)
	log.I(ctx, "          NumMemWrites: %-8v  UniqueMemWrites: %v", b.Stats.NumMemWrites, b.Stats.UniqueMemWrites)
	log.I(ctx, "          NumForwardDepOpens: %-4v  NumForwardDepCloses: %-4v  NumForwardDepDrops: %v", b.Stats.NumForwardDepOpens, b.Stats.NumForwardDepCloses, b.Stats.NumForwardDepDrops)
	log.I(ctx, "          UniqueDeps: %v", b.Stats.UniqueDeps)

	nodeIDs := make([]NodeID, len(b.Nodes))
	for i := range nodeIDs {
		nodeIDs[i] = (NodeID)(i)
	}

	sortBy := func(f func(n NodeID) uint64) {
		sort.Slice(nodeIDs, func(i, j int) bool {
			return f(nodeIDs[i]) > f(nodeIDs[j])
		})
	}

	logNode := func(v uint64, n NodeID) {
		var cmdStr string
		if node, ok := b.Nodes[n].(CmdNode); ok {
			if len(node.Index) == 1 {
				cmdID := (api.CmdID)(node.Index[0])
				cmd := graph.GetCommand(cmdID)
				cmdStr = fmt.Sprintf("%v", cmd)
			}
		}
		log.I(ctx, "%-9v  %v  %s", v, b.Nodes[n], cmdStr)
		s := b.NodeStats[n]
		log.I(ctx, "          NumFragReads: %-8v  UniqueFragReads: %v", s.NumFragReads, s.UniqueFragReads)
		log.I(ctx, "          NumFragWrites: %-7v  UniqueFragWrites: %v", s.NumFragWrites, s.UniqueFragWrites)
		log.I(ctx, "          NumMemReads: %-9v  UniqueMemReads: %v", s.NumMemReads, s.UniqueMemReads)
		log.I(ctx, "          NumMemWrites: %-8v  UniqueMemWrites: %v", s.NumMemWrites, s.UniqueMemWrites)
		log.I(ctx, "          NumForwardDepOpens: %-4v  NumForwardDepCloses: %-4v  NumForwardDepDrops: %v", s.NumForwardDepOpens, s.NumForwardDepCloses, s.NumForwardDepDrops)
		log.I(ctx, "          UniqueDeps: %v", s.UniqueDeps)
	}

	logTop := func(c uint, f func(n NodeID) uint64) {
		sortBy(f)
		for _, n := range nodeIDs[:c] {
			logNode(f(n), n)
		}
	}

	log.I(ctx, "Top Nodes by total accesses:")
	totalAccesses := func(n NodeID) uint64 {
		s := b.NodeStats[n]
		return s.NumFragReads +
			s.NumFragWrites +
			s.NumMemReads +
			s.NumMemWrites +
			s.NumForwardDepOpens +
			s.NumForwardDepCloses +
			s.NumForwardDepDrops
	}
	logTop(10, totalAccesses)

	log.I(ctx, "Top Nodes by unique accesses:")
	uniqueAccesses := func(n NodeID) uint64 {
		s := b.NodeStats[n]
		return s.UniqueFragReads +
			s.UniqueFragWrites +
			s.UniqueMemReads +
			s.UniqueMemWrites +
			s.NumForwardDepOpens +
			s.NumForwardDepCloses +
			s.NumForwardDepDrops
	}
	logTop(10, uniqueAccesses)
}

func (b *dependencyGraphBuilder) setDependencies(ctx context.Context, graph *dependencyGraph, node NodeID) {
	deps := map[NodeID]struct{}{}
	src := b.Nodes[node]
	var srcStr string
	_ = srcStr
	fmtIndex := func(idx api.SubCmdIdx) string {
		sb := strings.Builder{}
		for i, x := range idx {
			if i > 0 {
				sb.WriteString(".")
			}
			sb.WriteString(fmt.Sprintf("%v", x))
		}
		return sb.String()
	}
	dumpAccesses := false
	accessIndex := 0
	if n, ok := src.(CmdNode); ok {
		srcStr = fmtIndex(n.Index)
	}
	if dumpAccesses {
		log.D(ctx, "Accesses for %s", srcStr)
	}
	accessStr := func(a interface{}) string {
		switch a := a.(type) {
		case FragmentAccess:
			switch f := a.Fragment.(type) {
			case api.FieldFragment:
				return fmt.Sprintf("((%s)%v).%s", f.Field.ClassName(), a.Ref, f.Field.FieldName())
			case api.ArrayIndexFragment:
				return fmt.Sprintf("((%v)%v)[%v]", b.StateRefs[a.Ref], a.Ref, f.ArrayIndex)
			case api.MapIndexFragment:
				return fmt.Sprintf("((%v)%v)[%v]", b.StateRefs[a.Ref], a.Ref, f.MapIndex)
			case api.CompleteFragment:
				return fmt.Sprintf("((%v)%v)[*]", b.StateRefs[a.Ref], a.Ref)
			default:
				return fmt.Sprintf("((%v)%v)%v", b.StateRefs[a.Ref], a.Ref, f)
			}
		case MemoryAccess:
			return fmt.Sprintf("(%d)[%d:%d]", a.Pool, a.Span.Start, a.Span.End)
		case ForwardAccess:
			return fmt.Sprintf("FORWARD(%v)", a.DependencyID)
		}
		return "UNKNOWN_ACCESS"
	}
	modeStr := func(a interface{}) string {
		switch a := a.(type) {
		case FragmentAccess:
			switch a.Mode {
			case ACCESS_READ:
				return "R"
			case ACCESS_WRITE:
				return "W"
			case ACCESS_READ_WRITE:
				return "RW"
			}
		case MemoryAccess:
			preFence := "  "
			postFence := "  "
			switch a.Mode & ACCESS_READ_WRITE {
			case ACCESS_READ:
				preFence = "R "
			case ACCESS_WRITE:
				preFence = " W"
			case ACCESS_READ_WRITE:
				preFence = "RW"
			}
			return preFence + postFence
		case ForwardAccess:
			switch a.Mode {
			case FORWARD_OPEN:
				return "O"
			case FORWARD_CLOSE:
				return "C"
			case FORWARD_DROP:
				return "D"
			}
		}
		return "??"
	}
	logAccess := func(a interface{}) {
		if dumpAccesses {
			log.D(ctx, "%-7d %-4s %s", accessIndex, modeStr(a), accessStr(a))
			accessIndex++
		}
	}
	_ = logAccess

	addDep := func(d NodeID) {
		if _, ok := deps[d]; ok {
			return
		}
		deps[d] = struct{}{}
	}

	stats := &b.NodeStats[node]

	stats.UniqueFragReads = 0
	stats.UniqueFragWrites = 0
	stats.UniqueMemReads = 0
	stats.UniqueMemWrites = 0
	stats.UniqueDeps = 0

	for _, s := range b.NodeStats[node].fragmentAccessRanges {
		for i := s.Start; i < s.End; i++ {
			a := b.fragmentAccesses[i]
			if a.Node != node {
				continue
			}
			if a.Mode&ACCESS_READ != 0 {
				stats.UniqueFragReads++
			}
			if a.Mode&ACCESS_WRITE != 0 {
				stats.UniqueFragWrites++
			}
			logAccess(a)
			for _, d := range a.Deps {
				addDep(d)
			}
		}
	}
	for _, s := range b.NodeStats[node].memoryAccessRanges {
		for i := s.Start; i < s.End; i++ {
			a := b.memoryAccesses[i]
			if a.Node != node {
				continue
			}
			if a.Mode&ACCESS_READ != 0 {
				stats.UniqueMemReads++
			}
			if a.Mode&ACCESS_WRITE != 0 {
				stats.UniqueMemWrites++
			}
			logAccess(a)
			for _, d := range a.Deps {
				addDep(d)
			}
		}
	}
	for _, s := range b.NodeStats[node].forwardAccessRanges {
		for i := s.Start; i < s.End; i++ {
			a := b.forwardAccesses[i]
			if a.Node == node && a.Dep != nil && *a.Dep != NodeNoID && a.Mode == FORWARD_OPEN {
				logAccess(a)
				addDep(*a.Dep)
			}
		}
	}

	if n, ok := b.Nodes[node].(CmdNode); ok {
		cmdID := (api.CmdID)(n.Index[0])
		if len(n.Index) > 1 {
			// Add dependency from subcommand to top level command
			d := b.getCmdNodeID(cmdID, api.SubCmdIdx{})
			addDep(d)
		} else {
			// TEMPORARY HACK: Add dependencies from this command to all subcommands
			trie := b.CmdNodeIDs[cmdID]
			subCmdIndices := trie.PostOrderSortedKeys()
			for _, s := range subCmdIndices {
				d := b.getCmdNodeID(cmdID, s)
				addDep(d)
			}
		}
	}

	b.NodeStats[node].UniqueDeps = (uint64)(len(deps))

	b.Stats.UniqueFragReads += b.NodeStats[node].UniqueFragReads
	b.Stats.UniqueFragWrites += b.NodeStats[node].UniqueFragWrites
	b.Stats.UniqueMemReads += b.NodeStats[node].UniqueMemReads
	b.Stats.UniqueMemWrites += b.NodeStats[node].UniqueMemWrites
	b.Stats.UniqueDeps += b.NodeStats[node].UniqueDeps

	depSlice := make([]NodeID, len(deps))
	for d, _ := range deps {
		depSlice = append(depSlice, d)
	}
	graph.setDependencies(node, depSlice)
}

func (b *dependencyGraphBuilder) addNode(node Node) {
	if _, ok := node.(CmdNode); ok {
		b.Stats.NumCmdNodes++
	}
	if _, ok := node.(ObsNode); ok {
		b.Stats.NumObsNodes++
	}
	b.Nodes = append(b.Nodes, node)
	b.NodeStats = append(b.NodeStats, NodeStats{})
}

func (b *dependencyGraphBuilder) getCmdNodeID(cmdID api.CmdID, idx api.SubCmdIdx) NodeID {
	nodeID := (NodeID)(len(b.Nodes))
	fullIdx := make(api.SubCmdIdx, len(idx)+1)
	fullIdx[0] = (uint64)(cmdID)
	copy(fullIdx[1:], idx)
	node := CmdNode{fullIdx}
	if subCmdIDs, ok := b.CmdNodeIDs[cmdID]; ok {
		nodeID = subCmdIDs.GetDefault(idx, nodeID).(NodeID)
		if nodeID == (NodeID)(len(b.Nodes)) {
			b.addNode(node)
		}
	} else {
		b.addNode(node)
		trie := api.SubCmdIdxTrie{}
		trie.SetValue(idx, nodeID)
		b.CmdNodeIDs[cmdID] = &trie
	}
	return nodeID
}

func (b *dependencyGraphBuilder) getObsNodeID(cmdObservation api.CmdObservation,
	cmdID api.CmdID, isWrite bool, index int) NodeID {
	b.addNode(ObsNode{
		CmdObservation: cmdObservation,
		CmdID:          cmdID,
		IsWrite:        isWrite,
		Index:          index,
	})
	return (NodeID)(len(b.Nodes) - 1)
}

func BuildDependencyGraph(ctx context.Context, config DependencyGraphConfig,
	c *capture.Capture, initialCmds []api.Cmd, initialRanges interval.U64RangeList) (DependencyGraph, error) {
	ctx = status.Start(ctx, "BuildDependencyGraph")
	defer status.Finish(ctx)
	b := newDependencyGraphBuilder(ctx, config, c, initialCmds)
	var state *api.GlobalState
	if config.IncludeInitialCommands {
		state = c.NewUninitializedState(ctx).ReserveMemory(initialRanges)
	} else {
		state = c.NewState(ctx)
	}
	mutate := func(ctx context.Context, id api.CmdID, cmd api.Cmd) error {
		return cmd.Mutate(ctx, id, state, nil, b)
	}
	mutateD := func(ctx context.Context, id api.CmdID, cmd api.Cmd) error {
		return mutate(ctx, id.Derived(), cmd)
	}
	err := api.ForeachCmd(ctx, initialCmds, mutateD)
	if err != nil {
		return nil, err
	}
	err = api.ForeachCmd(ctx, c.Commands, mutate)
	if err != nil {
		return nil, err
	}

	graph := newDependencyGraph(ctx, config, c, initialCmds, b.Nodes)

	ctx = status.Start(ctx, "SetDependencies")
	defer status.Finish(ctx)

	for nodeID := range b.NodeStats {
		b.setDependencies(ctx, graph, (NodeID)(nodeID))
	}

	if config.ReverseDependencies {
		graph.buildDependenciesTo()
	}

	b.LogStats(ctx, graph)

	return graph, nil
}

func (b *dependencyGraphBuilder) debug(ctx context.Context, fmt string, args ...interface{}) {
	if config.DebugDependencyGraph || (len(b.current.subCmdIdx) == 4 && b.current.subCmdIdx[0] == 351 && b.current.subCmdIdx[3] == 1) {
		log.D(ctx, fmt, args...)
	}
}
