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
	"sort"

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

type NodeStats struct {
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
}

type AccessMode uint

const (
	ACCESS_READ       AccessMode = 1 << 0
	ACCESS_WRITE      AccessMode = 1 << 1
	ACCESS_READ_WRITE AccessMode = ACCESS_READ | ACCESS_WRITE
)

// The data needed to build a dependency graph by iterating through the commands in a trace
type dependencyGraphBuilder struct {
	// graph is the dependency graph being constructed
	// graph *dependencyGraph
	capture *capture.Capture

	config DependencyGraphConfig

	fragWatcher    FragWatcher
	memWatcher     MemWatcher
	forwardWatcher ForwardWatcher

	subCmdStack []CmdContext

	pendingNodes []NodeID

	NodeStats []NodeStats

	graph *dependencyGraph

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
		DepDist             Distribution
	}
}

// Build a new dependencyGraphBuilder.
func newDependencyGraphBuilder(ctx context.Context, config DependencyGraphConfig,
	c *capture.Capture, initialCmds []api.Cmd) *dependencyGraphBuilder {
	builder := &dependencyGraphBuilder{
		capture: c,
		config:  config,
		graph:   newDependencyGraph(ctx, config, c, initialCmds, []Node{}),
	}
	builder.fragWatcher = NewFragWatcher()
	builder.memWatcher = NewMemWatcher(builder.addNode)
	builder.forwardWatcher = NewForwardWatcher()
	return builder
}

// BeginCmd is called at the beginning of each API call
func (b *dependencyGraphBuilder) OnBeginCmd(ctx context.Context, cmdID api.CmdID, cmd api.Cmd) {
	if len(b.subCmdStack) > 0 {
		log.E(ctx, "OnBeginCmd called while processing another command")
		b.subCmdStack = b.subCmdStack[:0]
	}
	idx := api.SubCmdIdx{}
	nodeID := b.getCmdNodeID(cmdID, idx)
	cmdCtx := CmdContext{cmdID, cmd, idx, nodeID, 0}
	b.fragWatcher.OnBeginCmd(ctx, cmdCtx)
	b.memWatcher.OnBeginCmd(ctx, cmdCtx)
	b.forwardWatcher.OnBeginCmd(ctx, cmdCtx)
	b.subCmdStack = append(b.subCmdStack, cmdCtx)
}

// EndCmd is called at the end of each API call
func (b *dependencyGraphBuilder) OnEndCmd(ctx context.Context, cmdID api.CmdID, cmd api.Cmd) {
	if len(b.subCmdStack) > 1 {
		log.E(ctx, "OnEndCmd called while still processing subcommands")
	}
	cmdCtx := b.cmdCtx()

	b.fragWatcher.Flush(ctx, cmdCtx)
	fragNodeAccesses := b.fragWatcher.NodeAccesses()
	b.fragWatcher.OnEndCmd(ctx, cmdCtx)

	b.memWatcher.Flush(ctx, cmdCtx)
	memNodeAccesses := b.memWatcher.NodeAccesses()
	b.memWatcher.OnEndCmd(ctx, cmdCtx)

	b.forwardWatcher.Flush(ctx, cmdCtx)
	forwardNodeAccesses := b.forwardWatcher.NodeAccesses()
	b.forwardWatcher.OnEndCmd(ctx, cmdCtx)

	for _, n := range b.pendingNodes {
		b.addDependencies(n, fragNodeAccesses[n], memNodeAccesses[n], forwardNodeAccesses[n])
	}
	b.pendingNodes = b.pendingNodes[:0]
	b.subCmdStack = b.subCmdStack[:0]
}

func (b *dependencyGraphBuilder) OnBeginSubCmd(ctx context.Context, subCmdIdx api.SubCmdIdx) {
	if b.config.MergeSubCmdNodes {
		return
	}
	if len(b.subCmdStack) == 0 {
		log.E(ctx, "OnBeginSubCmd called while not processing any command")
	}
	oldCmdCtx := b.cmdCtx()
	nodeID := b.getCmdNodeID(oldCmdCtx.cmdID, subCmdIdx)
	newCmdCtx := CmdContext{
		cmdID:     oldCmdCtx.cmdID,
		cmd:       oldCmdCtx.cmd,
		subCmdIdx: subCmdIdx,
		nodeID:    nodeID,
		depth:     oldCmdCtx.depth + 1,
	}

	b.fragWatcher.Flush(ctx, oldCmdCtx)
	b.fragWatcher.OnBeginSubCmd(ctx, newCmdCtx)

	b.memWatcher.Flush(ctx, oldCmdCtx)
	b.memWatcher.OnBeginSubCmd(ctx, newCmdCtx)

	b.forwardWatcher.Flush(ctx, oldCmdCtx)
	b.forwardWatcher.OnBeginSubCmd(ctx, newCmdCtx)

	b.subCmdStack = append(b.subCmdStack, newCmdCtx)
}

func (b *dependencyGraphBuilder) OnEndSubCmd(ctx context.Context) {
	if b.config.MergeSubCmdNodes {
		return
	}
	if len(b.subCmdStack) < 2 {
		log.E(ctx, "OnEndSubCmd called while not processing any subcommand")
	}
	cmdCtx := b.cmdCtx()

	b.fragWatcher.Flush(ctx, cmdCtx)
	b.fragWatcher.OnEndSubCmd(ctx, cmdCtx)

	b.memWatcher.Flush(ctx, cmdCtx)
	b.memWatcher.OnEndSubCmd(ctx, cmdCtx)

	b.forwardWatcher.Flush(ctx, cmdCtx)
	b.forwardWatcher.OnEndSubCmd(ctx, cmdCtx)

	b.subCmdStack = b.subCmdStack[:len(b.subCmdStack)-1]
}

func (b *dependencyGraphBuilder) OnGet(ctx context.Context, owner api.RefObject, frag api.Fragment, valueRef api.RefObject) {
	b.NodeStats[b.cmdCtx().nodeID].NumFragReads++
	b.Stats.NumFragReads++

	b.fragWatcher.OnReadFrag(ctx, b.cmdCtx(), owner, frag, valueRef)
}

func (b *dependencyGraphBuilder) OnSet(ctx context.Context, owner api.RefObject, frag api.Fragment, oldValueRef api.RefObject, newValueRef api.RefObject) {
	b.NodeStats[b.cmdCtx().nodeID].NumFragWrites++
	b.Stats.NumFragWrites++

	b.fragWatcher.OnWriteFrag(ctx, b.cmdCtx(), owner, frag, oldValueRef, newValueRef)
}

// OnWriteSlice is called when writing to a slice
func (b *dependencyGraphBuilder) OnWriteSlice(ctx context.Context, slice memory.Slice) {
	b.NodeStats[b.cmdCtx().nodeID].NumMemWrites++
	b.Stats.NumMemWrites++
	b.memWatcher.OnWriteSlice(ctx, b.cmdCtx(), slice)
}

// OnReadSlice is called when reading from a slice
func (b *dependencyGraphBuilder) OnReadSlice(ctx context.Context, slice memory.Slice) {
	b.NodeStats[b.cmdCtx().nodeID].NumMemReads++
	b.Stats.NumMemReads++
	b.memWatcher.OnReadSlice(ctx, b.cmdCtx(), slice)
}

// OnWriteObs is called when a memory write observation becomes visible
func (b *dependencyGraphBuilder) OnWriteObs(ctx context.Context, obs []api.CmdObservation) {
	b.memWatcher.OnWriteObs(ctx, b.cmdCtx(), obs)
}

// OnReadObs is called when a memory read observation becomes visible
func (b *dependencyGraphBuilder) OnReadObs(ctx context.Context, obs []api.CmdObservation) {
	b.memWatcher.OnReadObs(ctx, b.cmdCtx(), obs)
}

// OpenForwardDependency is called to begin a forward dependency.
// See `StateWatcher.OpenForwardDependency` for an explanation of forward dependencies.
func (b *dependencyGraphBuilder) OpenForwardDependency(ctx context.Context, dependencyID interface{}) {
	b.NodeStats[b.cmdCtx().nodeID].NumForwardDepOpens++
	b.Stats.NumForwardDepOpens++

	b.forwardWatcher.OpenForwardDependency(ctx, b.cmdCtx(), dependencyID)
}

// CloseForwardDependency is called to end a forward dependency.
// See `StateWatcher.OpenForwardDependency` for an explanation of forward dependencies.
func (b *dependencyGraphBuilder) CloseForwardDependency(ctx context.Context, dependencyID interface{}) {
	b.NodeStats[b.cmdCtx().nodeID].NumForwardDepCloses++
	b.Stats.NumForwardDepCloses++

	b.forwardWatcher.CloseForwardDependency(ctx, b.cmdCtx(), dependencyID)
}

// DropForwardDependency is called to abandon a previously opened
// forward dependency, without actually adding the forward dependency.
// See `StateWatcher.OpenForwardDependency` for an explanation of forward dependencies.
func (b *dependencyGraphBuilder) DropForwardDependency(ctx context.Context, dependencyID interface{}) {
	b.NodeStats[b.cmdCtx().nodeID].NumForwardDepDrops++
	b.Stats.NumForwardDepDrops++

	b.forwardWatcher.DropForwardDependency(ctx, b.cmdCtx(), dependencyID)
}

// LogStats logs some interesting stats about the graph construction
func (b *dependencyGraphBuilder) LogStats(ctx context.Context) {
	log.I(ctx, "Dependency Graph Stats:")
	log.I(ctx, "          NumCmdNodes: %-8v  NumObsNodes: %v", b.Stats.NumCmdNodes, b.Stats.NumObsNodes)
	log.I(ctx, "          NumFragReads: %-8v  UniqueFragReads: %v", b.Stats.NumFragReads, b.Stats.UniqueFragReads)
	log.I(ctx, "          NumFragWrites: %-7v  UniqueFragWrites: %v", b.Stats.NumFragWrites, b.Stats.UniqueFragWrites)
	log.I(ctx, "          NumMemReads: %-9v  UniqueMemReads: %v", b.Stats.NumMemReads, b.Stats.UniqueMemReads)
	log.I(ctx, "          NumMemWrites: %-8v  UniqueMemWrites: %v", b.Stats.NumMemWrites, b.Stats.UniqueMemWrites)
	log.I(ctx, "          NumForwardDepOpens: %-4v  NumForwardDepCloses: %-4v  NumForwardDepDrops: %v", b.Stats.NumForwardDepOpens, b.Stats.NumForwardDepCloses, b.Stats.NumForwardDepDrops)
	log.I(ctx, "          UniqueDeps: %v", b.Stats.UniqueDeps)

	nodeIDs := make([]NodeID, len(b.graph.nodes))
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
		if node, ok := b.graph.nodes[n].(CmdNode); ok {
			if len(node.Index) == 1 {
				cmdID := (api.CmdID)(node.Index[0])
				cmd := b.graph.GetCommand(cmdID)
				cmdStr = fmt.Sprintf("%v", cmd)
			}
		}
		log.I(ctx, "%-9v  %v  %s", v, b.graph.nodes[n], cmdStr)
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

// func (b *dependencyGraphBuilder) setDependencies(ctx context.Context, graph *dependencyGraph, node NodeID) {
// 	deps := map[NodeID]struct{}{}
// 	src := b.Nodes[node]
// 	var srcStr string
// 	_ = srcStr
// 	fmtIndex := func(idx api.SubCmdIdx) string {
// 		sb := strings.Builder{}
// 		for i, x := range idx {
// 			if i > 0 {
// 				sb.WriteString(".")
// 			}
// 			sb.WriteString(fmt.Sprintf("%v", x))
// 		}
// 		return sb.String()
// 	}
// 	dumpAccesses := false
// 	accessIndex := 0
// 	if n, ok := src.(CmdNode); ok {
// 		srcStr = fmtIndex(n.Index)
// 	}
// 	if dumpAccesses {
// 		log.D(ctx, "Accesses for %s", srcStr)
// 	}
// 	accessStr := func(a interface{}) string {
// 		switch a := a.(type) {
// 		case FragmentAccess:
// 			switch f := a.Fragment.(type) {
// 			case api.FieldFragment:
// 				return fmt.Sprintf("((%s)%v).%s", f.Field.ClassName(), a.Ref, f.Field.FieldName())
// 			case api.ArrayIndexFragment:
// 				return fmt.Sprintf("((%v)%v)[%v]", b.StateRefs[a.Ref], a.Ref, f.ArrayIndex)
// 			case api.MapIndexFragment:
// 				return fmt.Sprintf("((%v)%v)[%v]", b.StateRefs[a.Ref], a.Ref, f.MapIndex)
// 			case api.CompleteFragment:
// 				return fmt.Sprintf("((%v)%v)[*]", b.StateRefs[a.Ref], a.Ref)
// 			default:
// 				return fmt.Sprintf("((%v)%v)%v", b.StateRefs[a.Ref], a.Ref, f)
// 			}
// 		case MemoryAccess:
// 			return fmt.Sprintf("(%d)[%d:%d]", a.Pool, a.Span.Start, a.Span.End)
// 		case ForwardAccess:
// 			return fmt.Sprintf("FORWARD(%v)", a.DependencyID)
// 		}
// 		return "UNKNOWN_ACCESS"
// 	}
// 	modeStr := func(a interface{}) string {
// 		switch a := a.(type) {
// 		case FragmentAccess:
// 			switch a.Mode {
// 			case ACCESS_READ:
// 				return "R"
// 			case ACCESS_WRITE:
// 				return "W"
// 			case ACCESS_READ_WRITE:
// 				return "RW"
// 			}
// 		case MemoryAccess:
// 			preFence := "  "
// 			postFence := "  "
// 			switch a.Mode & ACCESS_READ_WRITE {
// 			case ACCESS_READ:
// 				preFence = "R "
// 			case ACCESS_WRITE:
// 				preFence = " W"
// 			case ACCESS_READ_WRITE:
// 				preFence = "RW"
// 			}
// 			return preFence + postFence
// 		case ForwardAccess:
// 			switch a.Mode {
// 			case FORWARD_OPEN:
// 				return "O"
// 			case FORWARD_CLOSE:
// 				return "C"
// 			case FORWARD_DROP:
// 				return "D"
// 			}
// 		}
// 		return "??"
// 	}
// 	logAccess := func(a interface{}) {
// 		if dumpAccesses {
// 			log.D(ctx, "%-7d %-4s %s", accessIndex, modeStr(a), accessStr(a))
// 			accessIndex++
// 		}
// 	}
// 	_ = logAccess

// 	addDep := func(d NodeID) {
// 		if _, ok := deps[d]; ok {
// 			return
// 		}
// 		deps[d] = struct{}{}
// 	}

// 	stats := &b.NodeStats[node]

// 	stats.UniqueFragReads = 0
// 	stats.UniqueFragWrites = 0
// 	stats.UniqueMemReads = 0
// 	stats.UniqueMemWrites = 0
// 	stats.UniqueDeps = 0

// 	for _, s := range b.NodeStats[node].fragmentAccessRanges {
// 		for i := s.Start; i < s.End; i++ {
// 			a := b.fragmentAccesses[i]
// 			if a.Node != node {
// 				continue
// 			}
// 			if a.Mode&ACCESS_READ != 0 {
// 				stats.UniqueFragReads++
// 			}
// 			if a.Mode&ACCESS_WRITE != 0 {
// 				stats.UniqueFragWrites++
// 			}
// 			logAccess(a)
// 			for _, d := range a.Deps {
// 				addDep(d)
// 			}
// 		}
// 	}
// 	for _, s := range b.NodeStats[node].memoryAccessRanges {
// 		for i := s.Start; i < s.End; i++ {
// 			a := b.memoryAccesses[i]
// 			if a.Node != node {
// 				continue
// 			}
// 			if a.Mode&ACCESS_READ != 0 {
// 				stats.UniqueMemReads++
// 			}
// 			if a.Mode&ACCESS_WRITE != 0 {
// 				stats.UniqueMemWrites++
// 			}
// 			logAccess(a)
// 			for _, d := range a.Deps {
// 				addDep(d)
// 			}
// 		}
// 	}
// 	for _, s := range b.NodeStats[node].forwardAccessRanges {
// 		for i := s.Start; i < s.End; i++ {
// 			a := b.forwardAccesses[i]
// 			if a.Node == node && a.Dep != nil && *a.Dep != NodeNoID && a.Mode == FORWARD_OPEN {
// 				logAccess(a)
// 				addDep(*a.Dep)
// 			}
// 		}
// 	}

// 	if n, ok := b.Nodes[node].(CmdNode); ok {
// 		cmdID := (api.CmdID)(n.Index[0])
// 		if len(n.Index) > 1 {
// 			// Add dependency from subcommand to top level command
// 			d := b.getCmdNodeID(cmdID, api.SubCmdIdx{})
// 			addDep(d)
// 		} else {
// 			// TEMPORARY HACK: Add dependencies from this command to all subcommands
// 			trie := b.CmdNodeIDs[cmdID]
// 			subCmdIndices := trie.PostOrderSortedKeys()
// 			for _, s := range subCmdIndices {
// 				d := b.getCmdNodeID(cmdID, s)
// 				addDep(d)
// 			}
// 		}
// 	}

// 	b.NodeStats[node].UniqueDeps = (uint64)(len(deps))

// 	b.Stats.UniqueFragReads += b.NodeStats[node].UniqueFragReads
// 	b.Stats.UniqueFragWrites += b.NodeStats[node].UniqueFragWrites
// 	b.Stats.UniqueMemReads += b.NodeStats[node].UniqueMemReads
// 	b.Stats.UniqueMemWrites += b.NodeStats[node].UniqueMemWrites
// 	b.Stats.UniqueDeps += b.NodeStats[node].UniqueDeps

// 	depSlice := make([]NodeID, len(deps))
// 	for d, _ := range deps {
// 		depSlice = append(depSlice, d)
// 	}
// 	graph.setDependencies(node, depSlice)
// }

func (b *dependencyGraphBuilder) addNode(node Node) NodeID {
	if _, ok := node.(CmdNode); ok {
		b.Stats.NumCmdNodes++
	}
	if _, ok := node.(ObsNode); ok {
		b.Stats.NumObsNodes++
	}
	nodeID := b.graph.addNode(node)
	b.pendingNodes = append(b.pendingNodes, nodeID)
	b.NodeStats = append(b.NodeStats, make([]NodeStats, int(nodeID)+1-len(b.NodeStats))...)
	return nodeID
}

func (b *dependencyGraphBuilder) getCmdNodeID(cmdID api.CmdID, idx api.SubCmdIdx) NodeID {
	nodeID := b.graph.GetCmdNodeID(cmdID, idx)
	if nodeID != NodeNoID {
		return nodeID
	}
	fullIdx := append(api.SubCmdIdx{(uint64)(cmdID)}, idx...)
	node := CmdNode{fullIdx}
	return b.addNode(node)
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

	ctx = status.Start(ctx, "SetDependencies")
	defer status.Finish(ctx)

	if config.ReverseDependencies {
		b.graph.buildDependenciesTo()
	}

	b.LogStats(ctx)

	return b.graph, nil
}

func (b *dependencyGraphBuilder) debug(ctx context.Context, fmt string, args ...interface{}) {
	if config.DebugDependencyGraph || (len(b.cmdCtx().subCmdIdx) == 4 && b.cmdCtx().subCmdIdx[0] == 351 && b.cmdCtx().subCmdIdx[3] == 1) {
		log.D(ctx, fmt, args...)
	}
}

func (b *dependencyGraphBuilder) addDependencies(nodeID NodeID,
	fragAccesses []FragmentAccess,
	memAccesses []MemoryAccess,
	forwardAccesses []ForwardAccess) {
	deps := make(map[NodeID]struct{})
	stats := &b.NodeStats[nodeID]
	for _, a := range fragAccesses {
		if a.Mode&ACCESS_READ != 0 {
			stats.UniqueFragReads++
		}
		if a.Mode&ACCESS_WRITE != 0 {
			stats.UniqueFragWrites++
		}
		for _, d := range a.Deps {
			deps[d] = struct{}{}
		}
	}
	for _, a := range memAccesses {
		if a.Mode&ACCESS_READ != 0 {
			stats.UniqueMemReads++
		}
		if a.Mode&ACCESS_WRITE != 0 {
			stats.UniqueMemWrites++
		}
		for _, d := range a.Deps {
			deps[d] = struct{}{}
		}
	}
	openForwardDeps := 0
	for _, a := range forwardAccesses {
		if a.Mode == FORWARD_OPEN {
			d := a.Nodes.Close
			if d == NodeNoID || d > nodeID {
				// Forward dep going to later node.
				// Dependency will be added when processing the CLOSE
				openForwardDeps++
			} else {
				// Forward dep actually going to earlier node
				// (possibly from subcommand to parent).
				// Add dependency now, since dependency has already been processed.
				deps[d] = struct{}{}
			}
		} else if a.Mode == FORWARD_CLOSE && a.Nodes.Open < a.Nodes.Close {
			// Close is on a later node than open,
			// so dependency hasn't been added yet
			b.graph.addDependency(a.Nodes.Open, a.Nodes.Close)
		}
	}
	depSlice := make([]NodeID, 0, len(deps)+openForwardDeps)
	for d := range deps {
		depSlice = append(depSlice, d)
	}
	sort.Slice(depSlice, func(i, j int) bool { return depSlice[i] < depSlice[j] })
	b.graph.setDependencies(nodeID, depSlice)

	stats.UniqueDeps = uint64(len(depSlice))
	b.Stats.DepDist.Add(stats.UniqueDeps)
	b.Stats.UniqueFragReads += stats.UniqueFragReads
	b.Stats.UniqueFragWrites += stats.UniqueFragWrites
	b.Stats.UniqueMemReads += stats.UniqueMemReads
	b.Stats.UniqueMemWrites += stats.UniqueMemWrites
}

func (b *dependencyGraphBuilder) cmdCtx() CmdContext {
	if len(b.subCmdStack) == 0 {
		return CmdContext{}
	}
	return b.subCmdStack[len(b.subCmdStack)-1]
}

type Distribution struct {
	SmallBins []uint64
	LargeBins map[uint64]uint64
}

func (d Distribution) Add(x uint64) {
	if x < uint64(len(d.SmallBins)) {
		d.SmallBins[x]++
	} else {
		if d.LargeBins == nil {
			d.LargeBins = make(map[uint64]uint64)
		}
		d.LargeBins[x]++
	}
}

type CmdContext struct {
	cmdID     api.CmdID
	cmd       api.Cmd
	subCmdIdx api.SubCmdIdx
	nodeID    NodeID
	depth     int
}

type CmdWatcher interface {
	OnBeginCmd(ctx context.Context, cmdCtx CmdContext)
	OnEndCmd(ctx context.Context, cmdCtx CmdContext)
	OnBeginSubCmd(ctx context.Context, cmdCtx CmdContext)
	OnEndSubCmd(ctx context.Context, cmdCtx CmdContext)
}
