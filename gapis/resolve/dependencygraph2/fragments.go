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
	"math/bits"
	"reflect"

	"github.com/google/gapid/gapis/api"
)

type FragmentAccess struct {
	Node     NodeID
	Ref      api.RefID
	Fragment api.Fragment
	Mode     AccessMode
	Deps     []NodeID
}

type FragWatcher interface {
	OnReadFrag(ctx context.Context, cmdCtx CmdContext, owner api.RefObject, f api.Fragment, v api.RefObject)
	OnWriteFrag(ctx context.Context, cmdCtx CmdContext, owner api.RefObject, f api.Fragment, old api.RefObject, new api.RefObject)
	OnBeginCmd(ctx context.Context, cmdCtx CmdContext)
	OnEndCmd(ctx context.Context, cmdCtx CmdContext) map[NodeID][]FragmentAccess
	OnBeginSubCmd(ctx context.Context, cmdCtx CmdContext)
	OnEndSubCmd(ctx context.Context, cmdCtx CmdContext)
	Close()
}

func NewFragWatcher() *fragWatcher {
	return &fragWatcher{
		stateRefs:        make(map[api.RefID]reflect.Type),
		pendingFragments: make(map[api.RefID][]api.Fragment),
		refWrites:        make(map[api.RefID]*refWrites),
		refAccesses:      newRefAccesses(),
		nodeAccesses:     make(map[NodeID][]FragmentAccess),
	}
}

type fragWatcher struct {
	stateRefs        map[api.RefID]reflect.Type
	pendingFragments map[api.RefID][]api.Fragment
	refWrites        map[api.RefID]*refWrites
	refAccesses      refAccesses
	nodeAccesses     map[NodeID][]FragmentAccess
	stats            struct {
		// The distribution of the number of relevant writes for each complete read
		RelevantWriteDist Distribution
	}
}

func (b *fragWatcher) OnReadFrag(ctx context.Context, cmdCtx CmdContext,
	owner api.RefObject, frag api.Fragment, value api.RefObject) {
	ownerID := owner.RefID()
	valueID := value.RefID()
	depth := cmdCtx.depth
	nodeID := cmdCtx.nodeID

	// Ignore reads with nil owner
	if ownerID == api.NilRefID {
		return
	}

	// Ignore reads to variables not accessed through an api.State object
	if _, ok := b.stateRefs[ownerID]; !ok {
		if _, ok := owner.(api.State); ok {
			b.stateRefs[ownerID] = reflect.TypeOf(owner)
		} else {
			return
		}
	}

	if ownerFrags, ok := b.refAccesses.Get(ownerID); ok {
		// This object has been accessed since the last `Reset`

		fa, hasFa := ownerFrags.Get(frag)
		if hasFa {
			if fa.GetMode(depth, nodeID) != 0 {
				// This read is covered by an earlier access by this command
				return
			}
		}

		if a, ok := ownerFrags.Get(api.CompleteFragment{}); ok {
			if a.GetMode(depth, nodeID) != 0 {
				// This read is covered by an earlier complete access by this command
				return
			}
		}

		if !hasFa {
			// This fragment has not been accessed since the last `Reset`
			fa = &fragAccess{}
			ownerFrags.Set(frag, fa)
		}

		// Record the read, so that repeated reads of the same fragment are ignored.
		fa.AddRead(depth, nodeID)
	} else {
		// This object has not been accessed since the last `Reset`.
		ownerFrags := newFragAccesses(owner)
		fa := &fragAccess{}
		fa.AddRead(depth, nodeID)
		ownerFrags.Set(frag, fa)
		b.refAccesses.Set(ownerID, ownerFrags)
	}

	// Add this fragment to the set of fragments to be flushed
	b.pendingFragments[ownerID] = append(b.pendingFragments[ownerID], frag)

	// The value is now (indirectly) reachable through an api.State object.
	// Update `StateRefs` so that later reads/writes to fragments of
	// `valueRef` are not ignored.
	if valueID != api.NilRefID {
		b.stateRefs[valueID] = reflect.TypeOf(value)
	}
}

func (b *fragWatcher) OnWriteFrag(ctx context.Context, cmdCtx CmdContext,
	owner api.RefObject, frag api.Fragment, oldVal api.RefObject, newVal api.RefObject) {
	ownerID := owner.RefID()
	newValID := newVal.RefID()
	depth := cmdCtx.depth
	nodeID := cmdCtx.nodeID

	// Ignore writes with nil owner
	if ownerID == api.NilRefID {
		return
	}

	// Ignore writes to variables not accessed through an api.State object
	if _, ok := b.stateRefs[ownerID]; !ok {
		if _, ok := owner.(api.State); ok {
			b.stateRefs[ownerID] = reflect.TypeOf(owner)
		} else {
			return
		}
	}

	if ownerFrags, ok := b.refAccesses.Get(ownerID); ok {
		// This object has been accessed since the last `Reset`
		fa, hasFa := ownerFrags.Get(frag)
		if hasFa {
			if fa.GetMode(depth, nodeID)&ACCESS_WRITE != 0 {
				// This write is covered by an earlier write by this command
				return
			}
		}
		ca, hasCa := ownerFrags.Get(api.CompleteFragment{})
		if hasCa {
			if ca.GetMode(depth, nodeID)&ACCESS_WRITE != 0 {
				// This write is covered by an earlier complete write by this command
				return
			}
		}
		if !hasFa {
			// This fragment has not been accessed since the last `Reset`
			fa = &fragAccess{}
			ownerFrags.Set(frag, fa)
		}

		if _, ok := frag.(api.CompleteFragment); ok {
			// This is a write to the entire object.
			// Clear accesses to all fragments.
			ownerFrags.ForeachFrag(func(otherFrag api.Fragment, otherAcc *fragAccess) error {
				if otherFrag != frag {
					otherAcc.Clear()

					// Clear pending writes to individual fragments,
					// since this will be covered by the complete write
					otherAcc.pending &^= ACCESS_WRITE
				}
				return nil
			})
		} else {
			// `frag` is not CompleteFragment, and the write is not
			// covered by any previous writes by this node (neither
			// to frag nor to CompleteFragment).

			// Clear previous access to this fragment
			fa.Clear()

			// Clear previous access to the complete fragment, since
			// part of that access is invalidated by this read.
			if hasCa {
				ca.Clear()
			}
		}

		// Record the write, so that later reads/writes covered by this
		// write are ignored.
		fa.AddWrite(depth, nodeID)
	} else {
		// This object has not been accessed since the last `Reset`.
		ownerFrags := newFragAccesses(owner)
		fa := &fragAccess{}
		fa.AddWrite(depth, nodeID)
		ownerFrags.Set(frag, fa)
		b.refAccesses.Set(ownerID, ownerFrags)
	}

	// Add this fragment to the set of fragments to be flushed
	b.pendingFragments[ownerID] = append(b.pendingFragments[ownerID], frag)

	// The value is now (indirectly) reachable through an api.State object.
	// Update `StateRefs` so that later reads/writes to fragments of
	// `valueRef` are not ignored.
	if newValID != api.NilRefID {
		b.stateRefs[newValID] = reflect.TypeOf(newVal)
	}
}

func (b *fragWatcher) Flush(ctx context.Context, cmdCtx CmdContext) {
	nodeID := cmdCtx.nodeID
	fragAccesses := b.nodeAccesses[nodeID]

	// Compute the maximum possible of size of fragAccesses at the end of `Flush`.
	// Note: the `+ 2` is for the possible CompleteFragment reads/writes
	fragAccessesCap := len(fragAccesses) + 2
	for _, frags := range b.pendingFragments {
		fragAccessesCap += len(frags)
	}

	// Ensure that fragAccesses has sufficient capacity
	if fragAccessesCap > cap(fragAccesses) {
		// round up to next power of 2
		fragAccessesCap = 1 << uint(bits.Len(uint(fragAccessesCap))+1)

		newFragAccesses := make([]FragmentAccess, len(fragAccesses), fragAccessesCap)
		copy(newFragAccesses, fragAccesses)
		fragAccesses = newFragAccesses
	}

	for refID, frags := range b.pendingFragments {
		refFrags, _ := b.refAccesses.Get(refID)
		writes, hasWrites := b.refWrites[refID]

		ca, hasCa := refFrags.Get(api.CompleteFragment{})
		if hasCa && hasWrites && ca.pending&ACCESS_READ != 0 {
			// Read of complete fragment
			// => depend on each write
			writeNodes := writes.nodeSlice
			if len(writeNodes) > 0 {
				deps := make([]NodeID, len(writes.nodeSlice))
				copy(deps, writeNodes)
				fragAccesses = append(fragAccesses, FragmentAccess{
					Node:     nodeID,
					Fragment: api.CompleteFragment{},
					Ref:      refID,
					Mode:     ACCESS_READ,
					Deps:     deps,
				})
			}
			b.stats.RelevantWriteDist.Add(uint64(len(writeNodes)))
			ca.pending &^= ACCESS_READ
		}

		for _, frag := range frags {
			if _, ok := frag.(api.CompleteFragment); ok {
				continue
			}
			fa, hasFa := refFrags.Get(frag)
			if hasFa && fa.pending != 0 {
				writeNodes := []NodeID{}
				prevWrite := NodeNoID
				hasPrevWrite := false
				if hasWrites {
					prevWrite, hasPrevWrite = writes.frags.Get(frag)
				}
				if fa.pending&ACCESS_READ != 0 && hasPrevWrite {
					writeNodes = []NodeID{prevWrite}
				}
				if fa.pending&ACCESS_WRITE != 0 {
					if writes == nil {
						writes = newRefWrites(refFrags.m.EmptyClone())
						b.refWrites[refID] = writes
					}
					writes.frags.Set(frag, nodeID)

					if prevWrite != nodeID {
						if hasPrevWrite {
							if e, ok := writes.nodeSet[prevWrite]; ok {
								e.count--
								if e.count <= 0 {
									delete(writes.nodeSet, prevWrite)
									i := e.index
									l := len(writes.nodeSlice) - 1
									if i < l {
										writes.nodeSlice[i] = writes.nodeSlice[l]
									}
									writes.nodeSlice = writes.nodeSlice[:l]
								}
							}
						}
						e, ok := writes.nodeSet[nodeID]
						if ok {
							e.count++
						} else {
							index := len(writes.nodeSlice)
							writes.nodeSlice = append(writes.nodeSlice, nodeID)
							e = &nodeSetEntry{1, index}
							writes.nodeSet[nodeID] = e
						}
					}
				}
				if len(writeNodes) > 0 || fa.pending&ACCESS_WRITE != 0 {
					fragAccesses = append(fragAccesses, FragmentAccess{
						Node:     nodeID,
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
			writes.frags.Set(api.CompleteFragment{}, nodeID)
			writes.nodeSlice = []NodeID{nodeID}
			writes.nodeSet = map[NodeID]*nodeSetEntry{nodeID: &nodeSetEntry{1, 0}}

			fragAccesses = append(fragAccesses, FragmentAccess{
				Node:     nodeID,
				Fragment: api.CompleteFragment{},
				Ref:      refID,
				Mode:     ACCESS_WRITE,
				Deps:     []NodeID{},
			})
			ca.pending &^= ACCESS_WRITE
		}
	}
	b.nodeAccesses[nodeID] = fragAccesses
	b.pendingFragments = make(map[api.RefID][]api.Fragment)
}

func (b *fragWatcher) OnBeginCmd(ctx context.Context, cmdCtx CmdContext) {}

func (b *fragWatcher) OnEndCmd(ctx context.Context, cmdCtx CmdContext) map[NodeID][]FragmentAccess {
	b.Flush(ctx, cmdCtx)
	acc := b.nodeAccesses
	b.pendingFragments = make(map[api.RefID][]api.Fragment)
	b.refAccesses = newRefAccesses()
	b.nodeAccesses = make(map[NodeID][]FragmentAccess)
	return acc
}

func (b *fragWatcher) OnBeginSubCmd(ctx context.Context, cmdCtx CmdContext) {
	b.Flush(ctx, cmdCtx)
}

func (b *fragWatcher) OnEndSubCmd(ctx context.Context, cmdCtx CmdContext) {
	b.Flush(ctx, cmdCtx)
}

func (b *fragWatcher) Close() {}

type nodeSetEntry struct {
	count int
	index int
}

type refWrites struct {
	frags     fragWrites
	nodeSlice []NodeID
	nodeSet   map[NodeID]*nodeSetEntry
}

func newRefWrites(frags api.FragmentMap) *refWrites {
	return &refWrites{
		frags:     fragWrites{frags},
		nodeSlice: []NodeID{},
		nodeSet:   make(map[NodeID]*nodeSetEntry),
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
		n := 1 << uint(bits.Len(uint(depth)))
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

func (a *fragAccess) Clear() {
	for i := range a.accessStack {
		a.accessStack[i] = nodeFragAccess{}
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
