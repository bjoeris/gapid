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

	"github.com/google/gapid/gapis/api"
	"github.com/google/gapid/gapis/capture"

	"github.com/google/gapid/gapis/memory"
)

func NewAsyncFragWatcher(bufSize int, batchSize int) *asyncFragWatcher {
	var worker Worker
	if batchSize > 1 {
		worker = newBatchWorker(bufSize, batchSize)
	} else {
		worker = newSimpleWorker(bufSize)
	}
	return &asyncFragWatcher{*NewFragWatcher(), worker}
}

func NewAsyncMemWatcher(bufSize int, batchSize int) *asyncMemWatcher {
	var worker Worker
	if batchSize > 1 {
		worker = newBatchWorker(bufSize, batchSize)
	} else {
		worker = newSimpleWorker(bufSize)
	}
	return &asyncMemWatcher{*NewMemWatcher(), worker}
}

func NewAsyncForwardWatcher(bufSize int, batchSize int) *asyncForwardWatcher {
	var worker Worker
	if batchSize > 1 {
		worker = newBatchWorker(bufSize, batchSize)
	} else {
		worker = newSimpleWorker(bufSize)
	}
	return &asyncForwardWatcher{*NewForwardWatcher(), worker}
}

func NewAsyncGraphBuilder(ctx context.Context, config DependencyGraphConfig,
	c *capture.Capture, initialCmds []api.Cmd, bufSize int) *asyncGraphBuilder {
	return &asyncGraphBuilder{*NewGraphBuilder(ctx, config, c, initialCmds),
		newSimpleWorker(bufSize)}
}

type asyncFragWatcher struct {
	fragWatcher
	worker Worker
}

func (b *asyncFragWatcher) Close() {
	b.worker.Close()
}

func (b *asyncFragWatcher) OnReadFrag(ctx context.Context, cmdCtx CmdContext, owner api.RefObject, f api.Fragment, v api.RefObject, track bool) {
	b.worker.AddTask(func() {
		b.fragWatcher.OnReadFrag(ctx, cmdCtx, owner, f, v, track)
	})
}

func (b *asyncFragWatcher) OnWriteFrag(ctx context.Context, cmdCtx CmdContext, owner api.RefObject, f api.Fragment, old api.RefObject, new api.RefObject, track bool) {
	b.worker.AddTask(func() {
		b.fragWatcher.OnWriteFrag(ctx, cmdCtx, owner, f, old, new, track)
	})
}

func (b *asyncFragWatcher) OnBeginCmd(ctx context.Context, cmdCtx CmdContext) {
	b.worker.AddTask(func() {
		b.fragWatcher.OnBeginCmd(ctx, cmdCtx)
	})
}

func (b *asyncFragWatcher) OnEndCmd(ctx context.Context, cmdCtx CmdContext) map[NodeID][]FragmentAccess {
	c := make(chan map[NodeID][]FragmentAccess)
	b.worker.AddTask(func() {
		c <- b.fragWatcher.OnEndCmd(ctx, cmdCtx)
	})
	b.worker.Flush()
	return <-c
}
func (b *asyncFragWatcher) OnBeginSubCmd(ctx context.Context, cmdCtx CmdContext) {
	b.worker.AddTask(func() {
		b.fragWatcher.OnBeginSubCmd(ctx, cmdCtx)
	})
}
func (b *asyncFragWatcher) OnEndSubCmd(ctx context.Context, cmdCtx CmdContext) {
	b.worker.AddTask(func() {
		b.fragWatcher.OnEndSubCmd(ctx, cmdCtx)
	})
}

type asyncMemWatcher struct {
	memWatcher
	worker Worker
}

func (b *asyncMemWatcher) Close() {
	b.worker.Close()
}

func (b *asyncMemWatcher) OnWriteSlice(ctx context.Context, cmdCtx CmdContext, slice memory.Slice) {
	b.worker.AddTask(func() {
		b.memWatcher.OnWriteSlice(ctx, cmdCtx, slice)
	})
}

func (b *asyncMemWatcher) OnReadSlice(ctx context.Context, cmdCtx CmdContext, slice memory.Slice) {
	b.worker.AddTask(func() {
		b.memWatcher.OnReadSlice(ctx, cmdCtx, slice)
	})
}

func (b *asyncMemWatcher) OnWriteObs(ctx context.Context, cmdCtx CmdContext, obs []api.CmdObservation, nodeIDs []NodeID) {
	b.worker.AddTask(func() {
		b.memWatcher.OnWriteObs(ctx, cmdCtx, obs, nodeIDs)
	})
}

func (b *asyncMemWatcher) OnReadObs(ctx context.Context, cmdCtx CmdContext, obs []api.CmdObservation, nodeIDs []NodeID) {
	b.worker.AddTask(func() {
		b.memWatcher.OnReadObs(ctx, cmdCtx, obs, nodeIDs)
	})
}

func (b *asyncMemWatcher) OnBeginCmd(ctx context.Context, cmdCtx CmdContext) {
	b.worker.AddTask(func() {
		b.memWatcher.OnBeginCmd(ctx, cmdCtx)
	})
}

func (b *asyncMemWatcher) OnEndCmd(ctx context.Context, cmdCtx CmdContext) map[NodeID][]MemoryAccess {
	c := make(chan map[NodeID][]MemoryAccess)
	b.worker.AddTask(func() {
		c <- b.memWatcher.OnEndCmd(ctx, cmdCtx)
	})
	b.worker.Flush()
	return <-c
}
func (b *asyncMemWatcher) OnBeginSubCmd(ctx context.Context, cmdCtx CmdContext) {
	b.worker.AddTask(func() {
		b.memWatcher.OnBeginSubCmd(ctx, cmdCtx)
	})
}
func (b *asyncMemWatcher) OnEndSubCmd(ctx context.Context, cmdCtx CmdContext) {
	b.worker.AddTask(func() {
		b.memWatcher.OnEndSubCmd(ctx, cmdCtx)
	})
}

type asyncForwardWatcher struct {
	forwardWatcher
	worker Worker
}

func (b *asyncForwardWatcher) Close() {
	b.worker.Close()
}

func (b *asyncForwardWatcher) OpenForwardDependency(ctx context.Context, cmdCtx CmdContext, dependencyID interface{}) {
	b.worker.AddTask(func() {
		b.forwardWatcher.OpenForwardDependency(ctx, cmdCtx, dependencyID)
	})
}
func (b *asyncForwardWatcher) CloseForwardDependency(ctx context.Context, cmdCtx CmdContext, dependencyID interface{}) {
	b.worker.AddTask(func() {
		b.forwardWatcher.CloseForwardDependency(ctx, cmdCtx, dependencyID)
	})
}
func (b *asyncForwardWatcher) DropForwardDependency(ctx context.Context, cmdCtx CmdContext, dependencyID interface{}) {
	b.worker.AddTask(func() {
		b.forwardWatcher.DropForwardDependency(ctx, cmdCtx, dependencyID)
	})
}

func (b *asyncForwardWatcher) OnBeginCmd(ctx context.Context, cmdCtx CmdContext) {
	b.worker.AddTask(func() {
		b.forwardWatcher.OnBeginCmd(ctx, cmdCtx)
	})
}

func (b *asyncForwardWatcher) OnEndCmd(ctx context.Context, cmdCtx CmdContext) map[NodeID][]ForwardAccess {
	c := make(chan map[NodeID][]ForwardAccess)
	b.worker.AddTask(func() {
		c <- b.forwardWatcher.OnEndCmd(ctx, cmdCtx)
	})
	b.worker.Flush()
	return <-c
}
func (b *asyncForwardWatcher) OnBeginSubCmd(ctx context.Context, cmdCtx CmdContext) {
	b.worker.AddTask(func() {
		b.forwardWatcher.OnBeginSubCmd(ctx, cmdCtx)
	})
}
func (b *asyncForwardWatcher) OnEndSubCmd(ctx context.Context, cmdCtx CmdContext) {
	b.worker.AddTask(func() {
		b.forwardWatcher.OnEndSubCmd(ctx, cmdCtx)
	})
}

type asyncGraphBuilder struct {
	graphBuilder
	worker Worker
}

func (b *asyncGraphBuilder) AddDependencies(
	fragAcc map[NodeID][]FragmentAccess,
	memAcc map[NodeID][]MemoryAccess,
	forwardAcc map[NodeID][]ForwardAccess) {

	b.worker.AddTask(func() {
		b.graphBuilder.AddDependencies(fragAcc, memAcc, forwardAcc)
	})
}

func (b *asyncGraphBuilder) AddNodeDependencies(nodeID NodeID,
	fragAccesses []FragmentAccess,
	memAccesses []MemoryAccess,
	forwardAccesses []ForwardAccess) {
	b.worker.AddTask(func() {
		b.graphBuilder.AddNodeDependencies(nodeID, fragAccesses, memAccesses, forwardAccesses)
	})
}

func (b *asyncGraphBuilder) BuildReverseDependencies() {
	b.worker.AddTask(func() {
		b.graphBuilder.BuildReverseDependencies()
	})
}

func (b *asyncGraphBuilder) GetCmdNodeID(cmdID api.CmdID, idx api.SubCmdIdx) NodeID {
	c := make(chan NodeID)
	b.worker.AddTask(func() {
		c <- b.graphBuilder.GetCmdNodeID(cmdID, idx)
	})
	b.worker.Flush()
	return <-c
}

func (b *asyncGraphBuilder) GetObsNodeIDs(cmdID api.CmdID, obs []api.CmdObservation, isWrite bool) []NodeID {
	c := make(chan []NodeID)
	b.worker.AddTask(func() {
		c <- b.graphBuilder.GetObsNodeIDs(cmdID, obs, isWrite)
	})
	b.worker.Flush()
	return <-c
}

func (b *asyncGraphBuilder) GetCmdContext(cmdID api.CmdID, cmd api.Cmd, idx api.SubCmdIdx, depth int) CmdContext {
	c := make(chan CmdContext)
	b.worker.AddTask(func() {
		c <- b.graphBuilder.GetCmdContext(cmdID, cmd, idx, depth)
	})
	b.worker.Flush()
	return <-c
}

func (b *asyncGraphBuilder) GetNodeStats(nodeID NodeID) *NodeStats {
	c := make(chan *NodeStats)
	b.worker.AddTask(func() {
		c <- b.graphBuilder.GetNodeStats(nodeID)
	})
	b.worker.Flush()
	return <-c
}

func (b *asyncGraphBuilder) GetGraph() *dependencyGraph {
	c := make(chan *dependencyGraph)
	b.worker.AddTask(func() {
		c <- b.graphBuilder.GetGraph()
	})
	b.worker.Flush()
	return <-c
}

func (b *asyncGraphBuilder) Close() {
	b.worker.Close()
}

type Worker interface {
	AddTask(func())
	Flush()
	Close()
}

type simpleWorker struct {
	workChan chan func()
}

func newSimpleWorker(bufSize int) simpleWorker {
	worker := simpleWorker{make(chan func(), bufSize)}
	go func() {
		for {
			f, ok := <-worker.workChan
			if !ok {
				return
			}
			f()
		}
	}()
	return worker
}

func (w simpleWorker) AddTask(f func()) {
	w.workChan <- f
}

func (w simpleWorker) Flush() {}

func (w simpleWorker) Close() {
	close(w.workChan)
}

type batchWorker struct {
	batches    [][]func()
	workChan   chan []func()
	batchIndex int
	batchSize  int
}

func newBatchWorker(bufSize int, batchSize int) *batchWorker {
	numBatches := (bufSize + batchSize - 1) / batchSize
	worker := &batchWorker{
		// Make sure there are enough batch buffers for the worst case:
		//   * 1 batch being written
		//   * `numBatches` in the channel buffer
		//   * 1 batch being executed
		batches:    make([][]func(), numBatches+2),
		workChan:   make(chan []func(), numBatches),
		batchIndex: 0,
		batchSize:  batchSize,
	}
	go func() {
		for {
			fs, ok := <-worker.workChan
			if !ok {
				return
			}
			for _, f := range fs {
				f()
			}
		}
	}()
	return worker
}

func (w *batchWorker) AddTask(f func()) {
	i := w.batchIndex
	batch := w.batches[i]
	batch = append(batch, f)
	if len(batch) >= w.batchSize {
		w.workChan <- batch
		w.batches[i] = batch[:0]
		w.batchIndex = (i + 1) % len(w.batches)
	} else {
		w.batches[i] = batch
	}
}

func (w *batchWorker) Flush() {
	i := w.batchIndex
	batch := w.batches[i]
	w.workChan <- batch
	w.batches[i] = batch[:0]
	w.batchIndex = (i + 1) % len(w.batches)
}

func (w *batchWorker) Close() {
	w.Flush()
	close(w.workChan)
}
