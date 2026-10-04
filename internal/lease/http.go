package lease

import (
	"context"
	"encoding/json"
	"errors"
	"net"
	"net/http"
	"time"
)

// Server exposes the lease API over HTTP on top of a Node.
type Server struct {
	node *Node
	http *http.Server
	ln   net.Listener
}

type acquireRequest struct {
	Resource   string `json:"resource"`
	Holder     string `json:"holder"`
	TTLSeconds int64  `json:"ttl_seconds"`
}

type renewRequest struct {
	Resource   string `json:"resource"`
	Holder     string `json:"holder"`
	Token      uint64 `json:"token"`
	TTLSeconds int64  `json:"ttl_seconds"`
}

type releaseRequest struct {
	Resource string `json:"resource"`
	Holder   string `json:"holder"`
	Token    uint64 `json:"token"`
}

type errorResponse struct {
	Error  string `json:"error"`
	Leader string `json:"leader,omitempty"`
}

type lockResponse struct {
	Held              bool   `json:"held"`
	Resource          string `json:"resource"`
	Holder            string `json:"holder,omitempty"`
	Token             uint64 `json:"token,omitempty"`
	ExpiresAtNanos    int64  `json:"expires_at_nanos,omitempty"`
	FencingTokenBound uint64 `json:"fencing_token_bound"`
}

// ServeHTTP starts the HTTP API bound to the node's HTTP address.
func ServeHTTP(node *Node) (*Server, error) {
	s := &Server{node: node}
	mux := http.NewServeMux()
	mux.HandleFunc("POST /acquire", s.handleAcquire)
	mux.HandleFunc("POST /renew", s.handleRenew)
	mux.HandleFunc("POST /release", s.handleRelease)
	mux.HandleFunc("GET /lock", s.handleQuery)
	ln, err := net.Listen("tcp", node.cfg.HTTPAddr)
	if err != nil {
		return nil, err
	}
	s.ln = ln
	s.http = &http.Server{Handler: mux, ReadHeaderTimeout: 5 * time.Second}
	go s.http.Serve(ln)
	return s, nil
}

// Close stops accepting requests and waits for in-flight ones to finish.
func (s *Server) Close() error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	return s.http.Shutdown(ctx)
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	json.NewEncoder(w).Encode(v)
}

func (s *Server) handleAcquire(w http.ResponseWriter, r *http.Request) {
	var req acquireRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSON(w, http.StatusBadRequest, errorResponse{Error: "invalid JSON: " + err.Error()})
		return
	}
	if req.Resource == "" || req.Holder == "" {
		writeJSON(w, http.StatusBadRequest, errorResponse{Error: "resource and holder are required"})
		return
	}
	ttl := time.Duration(req.TTLSeconds) * time.Second
	if ttl < MinTTL || ttl > MaxTTL {
		writeJSON(w, http.StatusBadRequest, errorResponse{Error: "ttl_seconds must be within [1, 300]"})
		return
	}
	res, err := s.node.Propose(OpAcquire, req.Resource, req.Holder, 0, ttl)
	s.respond(w, res, err)
}

func (s *Server) handleRenew(w http.ResponseWriter, r *http.Request) {
	var req renewRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSON(w, http.StatusBadRequest, errorResponse{Error: "invalid JSON: " + err.Error()})
		return
	}
	if req.Resource == "" || req.Holder == "" || req.Token == 0 {
		writeJSON(w, http.StatusBadRequest, errorResponse{Error: "resource, holder and token are required"})
		return
	}
	ttl := time.Duration(req.TTLSeconds) * time.Second
	if ttl < MinTTL || ttl > MaxTTL {
		writeJSON(w, http.StatusBadRequest, errorResponse{Error: "ttl_seconds must be within [1, 300]"})
		return
	}
	res, err := s.node.Propose(OpRenew, req.Resource, req.Holder, req.Token, ttl)
	s.respond(w, res, err)
}

func (s *Server) handleRelease(w http.ResponseWriter, r *http.Request) {
	var req releaseRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSON(w, http.StatusBadRequest, errorResponse{Error: "invalid JSON: " + err.Error()})
		return
	}
	if req.Resource == "" || req.Holder == "" || req.Token == 0 {
		writeJSON(w, http.StatusBadRequest, errorResponse{Error: "resource, holder and token are required"})
		return
	}
	res, err := s.node.Propose(OpRelease, req.Resource, req.Holder, req.Token, 0)
	s.respond(w, res, err)
}

func (s *Server) handleQuery(w http.ResponseWriter, r *http.Request) {
	resource := r.URL.Query().Get("resource")
	if resource == "" {
		writeJSON(w, http.StatusBadRequest, errorResponse{Error: "resource query parameter is required"})
		return
	}
	resp := lockResponse{Resource: resource, FencingTokenBound: s.node.FSM.TokenBound(resource)}
	if l := s.node.FSM.Lookup(resource, time.Now()); l != nil {
		resp.Held = true
		resp.Holder = l.Holder
		resp.Token = l.Token
		resp.ExpiresAtNanos = l.ExpiresAtNanos
	}
	writeJSON(w, http.StatusOK, resp)
}

func (s *Server) respond(w http.ResponseWriter, res *Result, err error) {
	switch {
	case errors.Is(err, ErrNotLeader):
		writeJSON(w, http.StatusConflict, errorResponse{Error: "not leader", Leader: s.node.LeaderHTTPAddr()})
	case err != nil:
		writeJSON(w, http.StatusServiceUnavailable, errorResponse{Error: err.Error()})
	case !res.OK:
		writeJSON(w, http.StatusConflict, errorResponse{Error: res.Err})
	default:
		writeJSON(w, http.StatusOK, res)
	}
}
