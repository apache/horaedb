// Copyright 2023 CeresDB Project Authors. Licensed under Apache-2.0.

package http

import (
	"encoding/json"
	"fmt"
	"net/http"

	"github.com/CeresDB/ceresmeta/pkg/log"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"
)

type EtcdAPI struct {
	etcdClient    *clientv3.Client
	forwardClient *ForwardClient
}

func NewEtcdAPI(etcdClient *clientv3.Client, forwardClient *ForwardClient) EtcdAPI {
	return EtcdAPI{
		etcdClient:    etcdClient,
		forwardClient: forwardClient,
	}
}

type AddMemberRequest struct {
	MemberAddrs []string `json:"memberAddrs"`
}

func (a *EtcdAPI) addMember(writer http.ResponseWriter, req *http.Request) {
	var addMemberRequest AddMemberRequest
	err := json.NewDecoder(req.Body).Decode(&addMemberRequest)
	if err != nil {
		log.Error("decode request body failed", zap.Error(err))
		respondError(writer, ErrParseRequest, fmt.Sprintf("err: %s", err.Error()))
		return
	}

	resp, err := a.etcdClient.MemberAdd(req.Context(), addMemberRequest.MemberAddrs)
	if err != nil {
		log.Error("member add as learner failed", zap.Error(err))
		respondError(writer, ErrAddLearner, fmt.Sprintf("err: %s", err.Error()))
		return
	}

	respond(writer, resp)
}

func (a *EtcdAPI) getMember(writer http.ResponseWriter, req *http.Request) {
	resp, err := a.etcdClient.MemberList(req.Context())
	if err != nil {
		log.Error("list member failed", zap.Error(err))
		respondError(writer, ErrListMembers, fmt.Sprintf("err: %s", err.Error()))
		return
	}

	respond(writer, resp)
}

type UpdateMemberRequest struct {
	OldMemberName string   `json:"oldMemberName"`
	NewMemberAddr []string `json:"newMemberAddr"`
}

func (a *EtcdAPI) updateMember(writer http.ResponseWriter, req *http.Request) {
	var updateMemberRequest UpdateMemberRequest
	err := json.NewDecoder(req.Body).Decode(&updateMemberRequest)
	if err != nil {
		log.Error("decode request body failed", zap.Error(err))
		respondError(writer, ErrParseRequest, fmt.Sprintf("err: %s", err.Error()))
		return
	}

	memberListResp, err := a.etcdClient.MemberList(req.Context())
	if err != nil {
		log.Error("list members failed", zap.Error(err))
		respondError(writer, ErrListMembers, fmt.Sprintf("err: %s", err.Error()))
		return
	}

	for _, member := range memberListResp.Members {
		if member.Name == updateMemberRequest.OldMemberName {
			_, err := a.etcdClient.MemberUpdate(req.Context(), member.ID, updateMemberRequest.NewMemberAddr)
			if err != nil {
				log.Error("remove learner failed", zap.Error(err))
				respondError(writer, ErrRemoveMembers, fmt.Sprintf("err: %s", err.Error()))
				return
			}
			respond(writer, "ok")
			return
		}
	}

	respondError(writer, ErrGetMember, fmt.Sprintf("member not found, member name: %s", updateMemberRequest.OldMemberName))
}

type RemoveMemberRequest struct {
	MemberName string `json:"memberName"`
}

func (a *EtcdAPI) removeMember(writer http.ResponseWriter, req *http.Request) {
	var removeMemberRequest RemoveMemberRequest
	err := json.NewDecoder(req.Body).Decode(&removeMemberRequest)
	if err != nil {
		log.Error("decode request body failed", zap.Error(err))
		respondError(writer, ErrParseRequest, fmt.Sprintf("err: %s", err.Error()))
		return
	}

	memberListResp, err := a.etcdClient.MemberList(req.Context())
	if err != nil {
		log.Error("list members failed", zap.Error(err))
		respondError(writer, ErrListMembers, fmt.Sprintf("err: %s", err.Error()))
		return
	}

	for _, member := range memberListResp.Members {
		if member.Name == removeMemberRequest.MemberName {
			_, err := a.etcdClient.MemberRemove(req.Context(), member.ID)
			if err != nil {
				log.Error("remove learner failed", zap.Error(err))
				respondError(writer, ErrRemoveMembers, fmt.Sprintf("err: %s", err.Error()))
				return
			}
			respond(writer, "ok")
			return
		}
	}

	respondError(writer, ErrGetMember, fmt.Sprintf("member not found, member name: %s", removeMemberRequest.MemberName))
}

type PromoteLearnerRequest struct {
	LearnerName string `json:"learnerName"`
}

func (a *EtcdAPI) promoteLearner(writer http.ResponseWriter, req *http.Request) {
	var promoteLearnerRequest PromoteLearnerRequest
	err := json.NewDecoder(req.Body).Decode(&promoteLearnerRequest)
	if err != nil {
		log.Error("decode request body failed", zap.Error(err))
		respondError(writer, ErrParseRequest, fmt.Sprintf("err: %s", err.Error()))
		return
	}

	memberListResp, err := a.etcdClient.MemberList(req.Context())
	if err != nil {
		log.Error("list members failed", zap.Error(err))
		respondError(writer, ErrListMembers, fmt.Sprintf("err: %s", err.Error()))
		return
	}

	for _, member := range memberListResp.Members {
		if member.Name == promoteLearnerRequest.LearnerName {
			_, err := a.etcdClient.MemberPromote(req.Context(), member.ID)
			if err != nil {
				log.Error("remove learner failed", zap.Error(err))
				respondError(writer, ErrRemoveMembers, fmt.Sprintf("err: %s", err.Error()))
				return
			}
			respond(writer, "ok")
			return
		}
	}

	respondError(writer, ErrGetMember, fmt.Sprintf("learner not found, learner name: %s", promoteLearnerRequest.LearnerName))
}

type MoveLeaderRequest struct {
	MemberName string `json:"memberName"`
}

func (a *EtcdAPI) moveLeader(writer http.ResponseWriter, req *http.Request) {
	resp, isLeader, err := a.forwardClient.forwardToLeader(req)
	if err != nil {
		log.Error("forward to leader failed", zap.Error(err))
		respondError(writer, ErrForwardToLeader, fmt.Sprintf("err: %s", err.Error()))
		return
	}

	if !isLeader {
		respondForward(writer, resp)
		return
	}

	var moveLeaderRequest MoveLeaderRequest
	err = json.NewDecoder(req.Body).Decode(&moveLeaderRequest)
	if err != nil {
		log.Error("decode request body failed", zap.Error(err))
		respondError(writer, ErrParseRequest, fmt.Sprintf("err: %s", err.Error()))
		return
	}

	memberListResp, err := a.etcdClient.MemberList(req.Context())
	if err != nil {
		log.Error("list members failed", zap.Error(err))
		respondError(writer, ErrListMembers, fmt.Sprintf("err: %s", err.Error()))
		return
	}

	for _, member := range memberListResp.Members {
		if member.Name == moveLeaderRequest.MemberName {
			moveLeaderResp, err := a.etcdClient.MoveLeader(req.Context(), member.ID)
			if err != nil {
				log.Error("remove learner failed", zap.Error(err))
				respondError(writer, ErrRemoveMembers, fmt.Sprintf("err: %s", err.Error()))
				return
			}
			log.Info("move leader", zap.String("moveLeaderResp", fmt.Sprintf("%v", moveLeaderResp)))
			respond(writer, "ok")
			return
		}
	}

	respondError(writer, ErrGetMember, fmt.Sprintf("member not found, member name: %s", moveLeaderRequest.MemberName))
}
