// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

package cluster

import (
	"github.com/CeresDB/ceresdbproto/pkg/clusterpb"
	"github.com/looplab/fsm"
)

type Shard struct {
	meta    []*clusterpb.Shard
	nodes   []*clusterpb.Node
	tables  map[uint64]*Table // table_id => table
	version uint64
}

func (s *Shard) dropTableLocked(tableID uint64) {
	delete(s.tables, tableID)
}

type ShardTablesWithRole struct {
	shardID   uint32
	shardRole clusterpb.ShardRole
	tables    []*Table
	version   uint64
}

// Shard FSM Const Definition
// It contains the event name and the state name
const (
	EventTransferLeader          = "TransferLeader"
	EventTransferFollowerFailed  = "TransferFollowerFailed"
	EventTransferFollower        = "TransferFollower"
	EventTransferLeaderFailed    = "TransferLeaderFailed"
	EventPrepareTransferFollower = "PrepareTransferFollower"
	EventPrepareTransferLeader   = "PrepareTransferLeader"
	StateLeader                  = "LEADER"
	StateFollower                = "FOLLOWER"
	StatePendingLeader           = "PENDING_LEADER"
	StatePendingFollower         = "PENDING_FOLLOWER"
)

// Declare the source state array of FSM, avoid creating arrays repeatedly every time you create an FSM
var (
	events = fsm.Events{
		{Name: EventTransferLeader, Src: []string{StatePendingLeader}, Dst: StateLeader},
		{Name: EventTransferFollowerFailed, Src: []string{StatePendingFollower}, Dst: StateLeader},
		{Name: EventTransferFollower, Src: []string{StatePendingFollower}, Dst: StateFollower},
		{Name: EventTransferLeaderFailed, Src: []string{StatePendingLeader}, Dst: StateFollower},
		{Name: EventPrepareTransferFollower, Src: []string{StateLeader}, Dst: StatePendingFollower},
		{Name: EventPrepareTransferLeader, Src: []string{StateFollower}, Dst: StatePendingLeader},
	}

	callbacks = fsm.Callbacks{}
)

// NewFSM
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
func NewFSM(role clusterpb.ShardRole) *fsm.FSM {
	shardFsm := fsm.NewFSM(
		StateFollower,
		events,
		callbacks,
	)
	if role == clusterpb.ShardRole_LEADER {
		shardFsm.SetState(StateLeader)
	}
	if role == clusterpb.ShardRole_FOLLOWER {
		shardFsm.SetState(StateFollower)
	}
	return shardFsm
}
