// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

package shard

import (
	"context"

	"github.com/CeresDB/ceresdbproto/pkg/clusterpb"
	"github.com/CeresDB/ceresmeta/server/cluster"
	"github.com/CeresDB/ceresmeta/server/coordinator/eventdispatch"
	"github.com/looplab/fsm"
	"github.com/pkg/errors"
)

// Shard FSM Const Definition
// It contains the event name and the state name
const (
	StateLeader          = "LEADER"
	StateFollower        = "FOLLOWER"
	StatePendingLeader   = "PENDING_LEADER"
	StatePendingFollower = "PENDING_FOLLOWER"

	EventPrepareTransferFollower = "PrepareTransferFollower"
	EventTransferFollower        = "TransferFollower"
	EventTransferFollowerFailed  = "TransferFollowerFailed"

	EventPrepareTransferLeader = "PrepareTransferLeader"
	EventTransferLeader        = "TransferLeader"
	EventTransferLeaderFailed  = "TransferLeaderFailed"
)

// Declare the source state array of FSM, avoid creating arrays repeatedly every time you create an FSM
var (
	leaderFsmEvent = fsm.Events{
		{Name: EventPrepareTransferFollower, Src: []string{StateLeader}, Dst: StatePendingFollower},
		{Name: EventTransferFollower, Src: []string{StatePendingFollower}, Dst: StateFollower},
		{Name: EventTransferFollowerFailed, Src: []string{StatePendingFollower}, Dst: StateLeader},
	}
	leaderFsmCallbacks = fsm.Callbacks{
		EventPrepareTransferFollower: prepareTransferFollowerCallback,
		EventTransferFollower:        transferFollowerCallback,
		EventTransferFollowerFailed:  transferFollowerFailedCallback,
	}
)

var (
	followerFsmEvent = fsm.Events{
		{Name: EventPrepareTransferLeader, Src: []string{StateFollower}, Dst: StatePendingLeader},
		{Name: EventTransferLeader, Src: []string{StatePendingLeader}, Dst: StateLeader},
		{Name: EventTransferLeaderFailed, Src: []string{StatePendingLeader}, Dst: StateFollower},
	}
	followerFsmCallbacks = fsm.Callbacks{
		EventPrepareTransferLeader: prepareTransferLeaderCallback,
		EventTransferLeader:        transferLeaderCallback,
		EventTransferLeaderFailed:  transferLeaderFailed,
	}
)

type LeaderCallbackRequest struct {
	Ctx           context.Context
	Cluster       *cluster.Cluster
	EventDispatch eventdispatch.Dispatch

	Node    string
	ShardID uint32
}

type FollowerCallbackRequest struct {
	Ctx           context.Context
	Cluster       *cluster.Cluster
	EventDispatch eventdispatch.Dispatch

	Node    string
	ShardID uint32
}

// NewShardFSM
/**
```
┌────┐                   ┌────┐
│ RW ├─────────┐         │ R  ├─────────┐
├────┘         │         ├────┘         │
│    Leader    ◀─────────│PendingLeader │
│              │         │              │
└───────┬──▲───┘         └───────▲─┬────┘
        │  │                     │ │
┌────┐  │  │             ┌────┐  │ │
│ R  ├──▼──┴───┐         │ R  ├──┴─▼────┐
├────┘         │         ├────┘         │
│   Pending    ├─────────▶   Follower   │
│   Follower   │         │              │
└──────────────┘         └──────────────┘
```
*/
func NewShardFSM(role clusterpb.ShardRole) *fsm.FSM {
	switch role {
	case clusterpb.ShardRole_LEADER:
		leaderShardFsm := fsm.NewFSM(
			StateLeader,
			leaderFsmEvent,
			leaderFsmCallbacks,
		)
		return leaderShardFsm
	case clusterpb.ShardRole_FOLLOWER:
		followerShardFsm := fsm.NewFSM(
			StateFollower,
			followerFsmEvent,
			followerFsmCallbacks,
		)
		return followerShardFsm
	}
	return nil
}

// nolint
func prepareTransferFollowerCallback(_ *fsm.Event) {
	// TODO: add ShardRole_PENDING_FOLLOWER enum
}

func transferFollowerCallback(event *fsm.Event) {
	request := event.Args[0].(*LeaderCallbackRequest)
	ctx := request.Ctx
	c := request.Cluster
	d := request.EventDispatch
	node := request.Node
	shardID := request.ShardID

	req := &eventdispatch.CloseShardRequest{ShardID: shardID}
	if err := d.CloseShard(ctx, node, req); err != nil {
		event.Cancel(errors.Wrap(err, EventPrepareTransferFollower))
	}

	if err := updateShardRole(ctx, c, shardID, clusterpb.ShardRole_FOLLOWER); err != nil {
		event.Cancel(errors.Wrap(err, EventTransferFollower))
	}
}

func transferFollowerFailedCallback(event *fsm.Event) {
	request := event.Args[0].(*LeaderCallbackRequest)
	ctx := request.Ctx
	c := request.Cluster
	d := request.EventDispatch
	node := request.Node
	shardID := request.ShardID

	req := &eventdispatch.CloseShardRequest{ShardID: shardID}
	// Transfer failed, stop transfer and reset state.
	if err := d.CloseShard(ctx, node, req); err != nil {
		event.Cancel(errors.Wrap(err, EventTransferFollowerFailed))
	}

	if err := updateShardRole(ctx, c, shardID, clusterpb.ShardRole_LEADER); err != nil {
		event.Cancel(errors.Wrap(err, EventTransferFollowerFailed))
	}
}

// nolint
func prepareTransferLeaderCallback(_ *fsm.Event) {
	// TODO: add ShardRole_PENDING_LEADER enum
}

func transferLeaderCallback(event *fsm.Event) {
	request := event.Args[0].(*FollowerCallbackRequest)
	ctx := request.Ctx
	d := request.EventDispatch
	c := request.Cluster
	node := request.Node
	shardID := request.ShardID

	// Send event to CeresDB, waiting for response.
	// TODO: add shardInfo in FollowerCallbackRequest
	req := &eventdispatch.OpenShardRequest{Shard: &cluster.ShardInfo{
		ID:      shardID,
		Role:    clusterpb.ShardRole_LEADER,
		Version: 0,
	}}
	if err := d.OpenShard(ctx, node, req); err != nil {
		event.Cancel(errors.Wrap(err, EventTransferLeader))
	}

	if err := updateShardRole(ctx, c, shardID, clusterpb.ShardRole_LEADER); err != nil {
		event.Cancel(errors.Wrap(err, EventPrepareTransferLeader))
	}
}

func transferLeaderFailed(event *fsm.Event) {
	request := event.Args[0].(*FollowerCallbackRequest)
	ctx := request.Ctx
	d := request.EventDispatch
	c := request.Cluster
	node := request.Node
	shardID := request.ShardID

	req := &eventdispatch.CloseShardRequest{ShardID: shardID}
	// Transfer failed, stop transfer and reset state.
	if err := d.CloseShard(ctx, node, req); err != nil {
		event.Cancel(errors.Wrap(err, EventTransferLeaderFailed))
	}

	if err := updateShardRole(ctx, c, shardID, clusterpb.ShardRole_FOLLOWER); err != nil {
		event.Cancel(errors.Wrap(err, EventPrepareTransferLeader))
	}
}

func updateShardRole(ctx context.Context, c *cluster.Cluster, shardID uint32, role clusterpb.ShardRole) error {
	shardViews, err := c.GetClusterShardView()
	if err != nil {
		return errors.WithMessage(err, "updateShardRole failed")
	}
	for _, shard := range shardViews {
		if shard.GetId() == shardID {
			shard.ShardRole = role
		}
	}
	if err := c.UpdateClusterTopology(ctx, c.GetClusterState(), shardViews); err != nil {
		return errors.WithMessage(err, "updateShardRole failed")
	}
	return nil
}
