/*
 * Copyright 2022 The HoraeDB Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package http

import (
	"encoding/json"
	"fmt"
	"net/http"

	"github.com/CeresDB/horaemeta/pkg/log"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"
)

type EtcdAPI struct {
	etcdClient    *clientv3.Client
	forwardClient *ForwardClient
}

type AddMemberRequest struct {
	MemberAddrs []string `json:"memberAddrs"`
}

type UpdateMemberRequest struct {
	OldMemberName string   `json:"oldMemberName"`
	NewMemberAddr []string `json:"newMemberAddr"`
}

type RemoveMemberRequest struct {
	MemberName string `json:"memberName"`
}

type PromoteLearnerRequest struct {
	LearnerName string `json:"learnerName"`
}

type MoveLeaderRequest struct {
	MemberName string `json:"memberName"`
}

func NewEtcdAPI(etcdClient *clientv3.Client, forwardClient *ForwardClient) EtcdAPI {
	return EtcdAPI{
		etcdClient:    etcdClient,
		forwardClient: forwardClient,
	}
}

func (a *EtcdAPI) addMember(req *http.Request) apiFuncResult {
	var addMemberRequest AddMemberRequest
	err := json.NewDecoder(req.Body).Decode(&addMemberRequest)
	if err != nil {
		log.Error("decode request body failed", zap.Error(err))
		return errResult(ErrParseRequest, err.Error())
	}

	resp, err := a.etcdClient.MemberAdd(req.Context(), addMemberRequest.MemberAddrs)
	if err != nil {
		log.Error("member add as learner failed", zap.Error(err))
		return errResult(ErrAddLearner, err.Error())
	}

	return okResult(resp)
}

func (a *EtcdAPI) getMember(req *http.Request) apiFuncResult {
	resp, err := a.etcdClient.MemberList(req.Context())
	if err != nil {
		log.Error("list member failed", zap.Error(err))
		return errResult(ErrListMembers, err.Error())
	}

	return okResult(resp)
}

func (a *EtcdAPI) updateMember(req *http.Request) apiFuncResult {
	var updateMemberRequest UpdateMemberRequest
	err := json.NewDecoder(req.Body).Decode(&updateMemberRequest)
	if err != nil {
		log.Error("decode request body failed", zap.Error(err))
		return errResult(ErrParseTopology, err.Error())
	}

	memberListResp, err := a.etcdClient.MemberList(req.Context())
	if err != nil {
		log.Error("list members failed", zap.Error(err))
		return errResult(ErrListMembers, err.Error())
	}

	for _, member := range memberListResp.Members {
		if member.Name == updateMemberRequest.OldMemberName {
			_, err := a.etcdClient.MemberUpdate(req.Context(), member.ID, updateMemberRequest.NewMemberAddr)
			if err != nil {
				log.Error("remove learner failed", zap.Error(err))
				return errResult(ErrRemoveMembers, err.Error())
			}
			return okResult("ok")
		}
	}

	return errResult(ErrGetMember, fmt.Sprintf("member not found, member name: %s", updateMemberRequest.OldMemberName))
}

func (a *EtcdAPI) removeMember(req *http.Request) apiFuncResult {
	var removeMemberRequest RemoveMemberRequest
	err := json.NewDecoder(req.Body).Decode(&removeMemberRequest)
	if err != nil {
		log.Error("decode request body failed", zap.Error(err))
		return errResult(ErrParseRequest, err.Error())
	}

	memberListResp, err := a.etcdClient.MemberList(req.Context())
	if err != nil {
		log.Error("list members failed", zap.Error(err))
		return errResult(ErrListMembers, err.Error())
	}

	for _, member := range memberListResp.Members {
		if member.Name == removeMemberRequest.MemberName {
			_, err := a.etcdClient.MemberRemove(req.Context(), member.ID)
			if err != nil {
				log.Error("remove learner failed", zap.Error(err))
				return errResult(ErrRemoveMembers, err.Error())
			}

			return okResult("ok")
		}
	}

	return errResult(ErrGetMember, fmt.Sprintf("member not found, member name: %s", removeMemberRequest.MemberName))
}

func (a *EtcdAPI) promoteLearner(req *http.Request) apiFuncResult {
	var promoteLearnerRequest PromoteLearnerRequest
	err := json.NewDecoder(req.Body).Decode(&promoteLearnerRequest)
	if err != nil {
		log.Error("decode request body failed", zap.Error(err))
		return errResult(ErrParseRequest, err.Error())
	}

	memberListResp, err := a.etcdClient.MemberList(req.Context())
	if err != nil {
		log.Error("list members failed", zap.Error(err))
		return errResult(ErrListMembers, err.Error())
	}

	for _, member := range memberListResp.Members {
		if member.Name == promoteLearnerRequest.LearnerName {
			_, err := a.etcdClient.MemberPromote(req.Context(), member.ID)
			if err != nil {
				log.Error("remove learner failed", zap.Error(err))
				return errResult(ErrRemoveMembers, err.Error())
			}
			return okResult("ok")
		}
	}

	return errResult(ErrGetMember, fmt.Sprintf("learner not found, learner name: %s", promoteLearnerRequest.LearnerName))
}

func (a *EtcdAPI) moveLeader(req *http.Request) apiFuncResult {
	var moveLeaderRequest MoveLeaderRequest
	err := json.NewDecoder(req.Body).Decode(&moveLeaderRequest)
	if err != nil {
		log.Error("decode request body failed", zap.Error(err))
		return errResult(ErrParseRequest, err.Error())
	}

	memberListResp, err := a.etcdClient.MemberList(req.Context())
	if err != nil {
		log.Error("list members failed", zap.Error(err))
		return errResult(ErrListMembers, err.Error())
	}

	for _, member := range memberListResp.Members {
		if member.Name == moveLeaderRequest.MemberName {
			moveLeaderResp, err := a.etcdClient.MoveLeader(req.Context(), member.ID)
			if err != nil {
				log.Error("remove learner failed", zap.Error(err))
				return errResult(ErrRemoveMembers, err.Error())
			}
			log.Info("move leader", zap.String("moveLeaderResp", fmt.Sprintf("%v", moveLeaderResp)))
			return okResult("ok")
		}
	}

	return errResult(ErrGetMember, fmt.Sprintf("member not found, member name: %s", moveLeaderRequest.MemberName))
}
