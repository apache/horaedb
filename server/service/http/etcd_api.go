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

func (a *EtcdAPI) addMember(req *http.Request) apiFuncResult {
	var addMemberRequest AddMemberRequest
	err := json.NewDecoder(req.Body).Decode(&addMemberRequest)
	if err != nil {
		log.Error("decode request body failed", zap.Error(err))
		return errResult(ErrParseRequest, fmt.Sprintf("err: %s", err.Error()))
	}

	resp, err := a.etcdClient.MemberAdd(req.Context(), addMemberRequest.MemberAddrs)
	if err != nil {
		log.Error("member add as learner failed", zap.Error(err))
		return errResult(ErrAddLearner, fmt.Sprintf("err: %s", err.Error()))
	}

	return okResult(resp)
}

func (a *EtcdAPI) getMember(req *http.Request) apiFuncResult {
	resp, err := a.etcdClient.MemberList(req.Context())
	if err != nil {
		log.Error("list member failed", zap.Error(err))
		return errResult(ErrListMembers, fmt.Sprintf("err: %s", err.Error()))
	}

	return okResult(resp)
}

type UpdateMemberRequest struct {
	OldMemberName string   `json:"oldMemberName"`
	NewMemberAddr []string `json:"newMemberAddr"`
}

func (a *EtcdAPI) updateMember(req *http.Request) apiFuncResult {
	var updateMemberRequest UpdateMemberRequest
	err := json.NewDecoder(req.Body).Decode(&updateMemberRequest)
	if err != nil {
		log.Error("decode request body failed", zap.Error(err))
		return errResult(ErrParseTopology, fmt.Sprintf("err: %s", err.Error()))
	}

	memberListResp, err := a.etcdClient.MemberList(req.Context())
	if err != nil {
		log.Error("list members failed", zap.Error(err))
		return errResult(ErrListMembers, fmt.Sprintf("err: %s", err.Error()))
	}

	for _, member := range memberListResp.Members {
		if member.Name == updateMemberRequest.OldMemberName {
			_, err := a.etcdClient.MemberUpdate(req.Context(), member.ID, updateMemberRequest.NewMemberAddr)
			if err != nil {
				log.Error("remove learner failed", zap.Error(err))
				return errResult(ErrRemoveMembers, fmt.Sprintf("err: %s", err.Error()))
			}
			return okResult("ok")
		}
	}

	return errResult(ErrGetMember, fmt.Sprintf("member not found, member name: %s", updateMemberRequest.OldMemberName))
}

type RemoveMemberRequest struct {
	MemberName string `json:"memberName"`
}

func (a *EtcdAPI) removeMember(req *http.Request) apiFuncResult {
	var removeMemberRequest RemoveMemberRequest
	err := json.NewDecoder(req.Body).Decode(&removeMemberRequest)
	if err != nil {
		log.Error("decode request body failed", zap.Error(err))
		return errResult(ErrParseRequest, fmt.Sprintf("err: %s", err.Error()))
	}

	memberListResp, err := a.etcdClient.MemberList(req.Context())
	if err != nil {
		log.Error("list members failed", zap.Error(err))
		return errResult(ErrListMembers, fmt.Sprintf("err: %s", err.Error()))
	}

	for _, member := range memberListResp.Members {
		if member.Name == removeMemberRequest.MemberName {
			_, err := a.etcdClient.MemberRemove(req.Context(), member.ID)
			if err != nil {
				log.Error("remove learner failed", zap.Error(err))
				return errResult(ErrRemoveMembers, fmt.Sprintf("err: %s", err.Error()))
			}

			return okResult("ok")
		}
	}

	return errResult(ErrGetMember, fmt.Sprintf("member not found, member name: %s", removeMemberRequest.MemberName))
}

type PromoteLearnerRequest struct {
	LearnerName string `json:"learnerName"`
}

func (a *EtcdAPI) promoteLearner(req *http.Request) apiFuncResult {
	var promoteLearnerRequest PromoteLearnerRequest
	err := json.NewDecoder(req.Body).Decode(&promoteLearnerRequest)
	if err != nil {
		log.Error("decode request body failed", zap.Error(err))
		return errResult(ErrParseRequest, fmt.Sprintf("err: %s", err.Error()))
	}

	memberListResp, err := a.etcdClient.MemberList(req.Context())
	if err != nil {
		log.Error("list members failed", zap.Error(err))
		return errResult(ErrListMembers, fmt.Sprintf("err: %s", err.Error()))
	}

	for _, member := range memberListResp.Members {
		if member.Name == promoteLearnerRequest.LearnerName {
			_, err := a.etcdClient.MemberPromote(req.Context(), member.ID)
			if err != nil {
				log.Error("remove learner failed", zap.Error(err))
				return errResult(ErrRemoveMembers, fmt.Sprintf("err: %s", err.Error()))
			}
			return okResult("ok")
		}
	}

	return errResult(ErrGetMember, fmt.Sprintf("learner not found, learner name: %s", promoteLearnerRequest.LearnerName))
}

type MoveLeaderRequest struct {
	MemberName string `json:"memberName"`
}

func (a *EtcdAPI) moveLeader(req *http.Request) apiFuncResult {
	var moveLeaderRequest MoveLeaderRequest
	err := json.NewDecoder(req.Body).Decode(&moveLeaderRequest)
	if err != nil {
		log.Error("decode request body failed", zap.Error(err))
		return errResult(ErrParseRequest, fmt.Sprintf("err: %s", err.Error()))
	}

	memberListResp, err := a.etcdClient.MemberList(req.Context())
	if err != nil {
		log.Error("list members failed", zap.Error(err))
		return errResult(ErrListMembers, fmt.Sprintf("err: %s", err.Error()))
	}

	for _, member := range memberListResp.Members {
		if member.Name == moveLeaderRequest.MemberName {
			moveLeaderResp, err := a.etcdClient.MoveLeader(req.Context(), member.ID)
			if err != nil {
				log.Error("remove learner failed", zap.Error(err))
				return errResult(ErrRemoveMembers, fmt.Sprintf("err: %s", err.Error()))
			}
			log.Info("move leader", zap.String("moveLeaderResp", fmt.Sprintf("%v", moveLeaderResp)))
			return okResult("ok")
		}
	}

	return errResult(ErrGetMember, fmt.Sprintf("member not found, member name: %s", moveLeaderRequest.MemberName))
}
