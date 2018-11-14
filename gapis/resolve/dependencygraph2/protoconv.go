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

	"github.com/google/gapid/core/data/protoconv"
	"github.com/google/gapid/gapis/api"
	"github.com/google/gapid/gapis/memory/memory_pb"
	"github.com/google/gapid/gapis/resolve/dependencygraph2/dependencygraph2_pb"
)

func init() {
	protoconv.Register(
		func(ctx context.Context, in *dependencyGraph) (*dependencygraph2_pb.DependencyGraph, error) {
			refs := &protoconv.ToProtoContext{}
			return in.toProto(ctx, refs)
		},
		func(ctx context.Context, in *dependencygraph2_pb.DependencyGraph) (*dependencyGraph, error) {
			refs := &protoconv.FromProtoContext{}
			v, err := dependencyGraphFromProto(ctx, in, refs)
			return v, err
		},
	)
}

func (g *dependencyGraph) toProto(ctx context.Context, refs *protoconv.ToProtoContext) (*dependencygraph2_pb.DependencyGraph, error) {
	to := &dependencygraph2_pb.DependencyGraph{}
	n := g.NumNodes()
	to.Nodes = make([]*dependencygraph2_pb.Node, n)
	for i := 0; i < n; i++ {
		m, err := g.nodeToProto(ctx, NodeID(i), refs)
		if err != nil {
			return nil, err
		}
		to.Nodes[i] = m
	}
	return to, nil
}

func (g *dependencyGraph) nodeToProto(ctx context.Context, nodeID NodeID, refs *protoconv.ToProtoContext) (*dependencygraph2_pb.Node, error) {
	to := &dependencygraph2_pb.Node{}
	to.NodeID = uint64(nodeID)
	node := g.GetNode(nodeID)

	switch v := node.(type) {
	case CmdNode:
		indices := make([]uint64, len(v.Index))
		copy(indices, v.Index)
		to.Type = &dependencygraph2_pb.Node_CmdNode{
			CmdNode: &dependencygraph2_pb.CmdNode{
				Indices: indices,
				// InitCmdID: uint64(v.InitCmdID),
			}}
	case ObsNode:
		// msg, err := protoconv.ToProto(ctx, v.CmdObservation)
		// if err != nil {
		// 	return nil, err
		// }
		// obs_pb, ok := msg.(*memory_pb.Observation)
		// if !ok {
		// 	return nil, fmt.Errorf("CmdObservation to Proto returned unexpected type %T", msg)
		// }
		obs := v.CmdObservation
		obs_pb := &memory_pb.Observation{
			Pool: uint32(obs.Pool),
			Base: obs.Range.Base,
			Size: obs.Range.Size,
		}
		to.Type = &dependencygraph2_pb.Node_ObsNode{
			ObsNode: &dependencygraph2_pb.ObsNode{
				Observation: obs_pb,
				CmdID:       uint64(v.CmdID),
				IsWrite:     v.IsWrite,
				Index:       uint64(v.Index)}}
	}

	acc := g.GetNodeAccesses(nodeID)
	to.FragmentAccesses = make([]*dependencygraph2_pb.FragmentAccess, len(acc.FragmentAccesses))
	for i, a := range acc.FragmentAccesses {
		to.FragmentAccesses[i] = a.toProto(refs)
	}

	to.MemoryAccesses = make([]*dependencygraph2_pb.MemoryAccess, len(acc.MemoryAccesses))
	for i, a := range acc.MemoryAccesses {
		to.MemoryAccesses[i] = a.toProto(refs)
	}

	to.ForwardAccesses = make([]*dependencygraph2_pb.ForwardAccess, len(acc.ForwardAccesses))
	for i, a := range acc.ForwardAccesses {
		to.ForwardAccesses[i] = a.toProto(refs)
	}

	to.ParentNode = uint64(acc.ParentNode)
	to.InitCmdNodes = make([]uint64, len(acc.InitCmdNodes))
	for i, n := range acc.InitCmdNodes {
		to.InitCmdNodes[i] = uint64(n)
	}

	deps := g.dependenciesFrom[nodeID]
	to.Dependencies = make([]uint64, len(deps))
	for i, d := range deps {
		to.Dependencies[i] = uint64(d)
	}

	if g.config.ReverseDependencies {
		revDeps := g.dependenciesTo[nodeID]
		to.ReverseDependencies = make([]uint64, len(revDeps))
		for i, d := range revDeps {
			to.ReverseDependencies[i] = uint64(d)
		}
	}
	return to, nil
}

func (a *FragmentAccess) toProto(refs *protoconv.ToProtoContext) *dependencygraph2_pb.FragmentAccess {
	to := &dependencygraph2_pb.FragmentAccess{}
	to.NodeID = uint64(a.Node)
	to.RefID = uint64(a.Ref)
	to.Fragment = fragmentToProto(a.Fragment, refs)
	to.Mode = dependencygraph2_pb.AccessMode(a.Mode)
	to.Dependencies = make([]uint64, len(a.Deps))
	for i, d := range a.Deps {
		to.Dependencies[i] = uint64(d)
	}
	return to
}

func (a *MemoryAccess) toProto(refs *protoconv.ToProtoContext) *dependencygraph2_pb.MemoryAccess {
	to := &dependencygraph2_pb.MemoryAccess{}
	to.NodeID = uint64(a.Node)
	to.PoolID = uint64(a.Pool)
	to.Start = a.Span.Start
	to.End = a.Span.End
	to.Mode = dependencygraph2_pb.AccessMode(a.Mode)
	to.Dependencies = make([]uint64, len(a.Deps))
	for i, d := range a.Deps {
		to.Dependencies[i] = uint64(d)
	}
	return to
}

func (a *ForwardAccess) toProto(refs *protoconv.ToProtoContext) *dependencygraph2_pb.ForwardAccess {
	to := &dependencygraph2_pb.ForwardAccess{}
	to.Open = uint64(a.Nodes.Open)
	to.Close = uint64(a.Nodes.Close)
	to.Drop = uint64(a.Nodes.Drop)
	to.DependencyID = fmt.Sprintf("%v", a.DependencyID)
	to.Mode = dependencygraph2_pb.ForwardAccessMode(a.Mode)
	return to
}

func fragmentToProto(frag api.Fragment, refs *protoconv.ToProtoContext) *dependencygraph2_pb.Fragment {
	to := &dependencygraph2_pb.Fragment{}
	switch v := frag.(type) {
	case api.FieldFragment:
		to.Type = &dependencygraph2_pb.Fragment_Field{
			Field: &dependencygraph2_pb.FieldFragment{
				Name:  v.Field.FieldName(),
				Class: v.Field.ClassName(),
				Index: uint32(v.FieldIndex())}}
	case api.ArrayIndexFragment:
		to.Type = &dependencygraph2_pb.Fragment_ArrayIndex{
			uint64(v.ArrayIndex)}
	case api.MapIndexFragment:
		key_pb := &dependencygraph2_pb.MapIndexFragment{}
		switch k := v.MapIndex.(type) {
		case uint64:
			key_pb.Type = &dependencygraph2_pb.MapIndexFragment_Uint64Index{k}
		case string:
			key_pb.Type = &dependencygraph2_pb.MapIndexFragment_StringIndex{k}
		default:
			key_pb.Type = &dependencygraph2_pb.MapIndexFragment_StringIndex{
				fmt.Sprintf("%v", k)}
		}
		to.Type = &dependencygraph2_pb.Fragment_MapIndex{key_pb}
	case api.CompleteFragment:
		to.Type = &dependencygraph2_pb.Fragment_Complete{
			&dependencygraph2_pb.CompleteFragment{}}
	}
	return to
}

func dependencyGraphFromProto(ctx context.Context, in *dependencygraph2_pb.DependencyGraph, refs *protoconv.FromProtoContext) (*dependencyGraph, error) {
	panic("Dependency graph deserialization not yet implemented")
}
