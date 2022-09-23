// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

package cluster

import (
	"testing"

	"github.com/CeresDB/ceresdbproto/pkg/clusterpb"
	"github.com/stretchr/testify/assert"
)

func TestTransferLeader(t *testing.T) {
	fsm := NewFSM(clusterpb.ShardRole_FOLLOWER)

	err := fsm.Event(EventPrepareTransferLeader)
	assert.NoError(t, err)
	assert.Equal(t, StatePendingLeader, fsm.Current())

	err = fsm.Event(EventTransferLeader)
	assert.NoError(t, err)
	assert.Equal(t, StateLeader, fsm.Current())
}
