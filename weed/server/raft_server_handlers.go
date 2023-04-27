package weed_server

import (
	"net/http"

	"weed/operation"
)

func (s *RaftServer) HandleFunc(pattern string, handler func(http.ResponseWriter, *http.Request)) {
	s.router.HandleFunc(pattern, handler)
}

func (s *RaftServer) statusHandler(w http.ResponseWriter, r *http.Request) {
	ret := operation.ClusterStatusResult{
		IsLeader: s.IsLeader(),
		Peers:    s.Peers(),
	}
	if leader, e := s.Leader(); e == nil {
		ret.Leader = leader
	}
	writeJsonQuiet(w, r, http.StatusOK, ret)
}
