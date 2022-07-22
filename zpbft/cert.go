package main

import (
	"sync"
	"sync/atomic"
	"time"
)

type Log struct {
	Op []byte
}

type Stage = int32

const (
	PrepareStage = iota
	CommitStage
	ReplyStage
)

type CmdCert struct {
	seq    int64
	digest []byte
	start  time.Time
	replys map[int64][]byte
}

type LogCert struct {
	seq      int64
	view     int64
	primary  int64
	req      *RequestArgs
	digest   []byte
	prepareShared bool
	committed bool
	prepares map[int64]*PrepareArgs  // 用作prepare计数
	commits  map[int64]*CommitArgs   // 用作commit计数
	prepareShares map[int64]*PrepareShareArgs  // 用作prepareShare计数
	commitShares  map[int64]*CommitShareArgs   // 用作commitShare计数
	prepareQ []*PrepareArgs
	prepareShareQ []*PrepareShareArgs
	commitQ  []*CommitArgs
	commitShareQ  []*CommitShareArgs
	stage    Stage
	mu       sync.Mutex
	prepared       sync.Mutex
	committedMutex sync.Mutex
}

func (lc *LogCert) set(req *RequestArgs, digest []byte, view int64,primary int64) {
	lc.mu.Lock()
	defer lc.mu.Unlock()
	lc.req = req
	lc.digest = digest
	lc.view	= view
	lc.primary = primary
}

func (lc *LogCert) logCommitted() {
	lc.mu.Lock()
	defer lc.mu.Unlock()
	if lc.req != nil{
		//lc.req.Req.Operator = nil
		//lc.req = nil
		lc.digest = nil
		lc.committed = true
		//lc.prepares = nil
		//lc.commits = nil
		//lc.prepareShares = nil
		//lc.commitShares = nil
		//lc.prepareQ = nil
		//lc.prepareShareQ = nil
		//lc.commitQ = nil
		//lc.commitShareQ = nil
	}
}

func (lc *LogCert) get() (*RequestArgs, []byte, int64, int64) {
	lc.mu.Lock()
	defer lc.mu.Unlock()
	return lc.req, lc.digest, lc.view, lc.primary
}

func (lc *LogCert) pushPrepare(args *PrepareArgs) {
	lc.mu.Lock()
	defer lc.mu.Unlock()
	lc.prepareQ = append(lc.prepareQ, args)
}

func (lc *LogCert) pushPrepareShare(args *PrepareShareArgs) {
	lc.mu.Lock()
	defer lc.mu.Unlock()
	lc.prepareShareQ = append(lc.prepareShareQ, args)
}

func (lc *LogCert) pushCommit(args *CommitArgs) {
	lc.mu.Lock()
	defer lc.mu.Unlock()
	lc.commitQ = append(lc.commitQ, args)
}

func (lc *LogCert) pushCommitShare(args *CommitShareArgs) {
	lc.mu.Lock()
	defer lc.mu.Unlock()
	lc.commitShareQ = append(lc.commitShareQ, args)
}

func (lc *LogCert) popAllPrepares() []*PrepareArgs {
	lc.mu.Lock()
	defer lc.mu.Unlock()
	argsQ := lc.prepareQ
	lc.prepareQ = nil
	return argsQ
}

func (lc *LogCert) popAllPrepareShares() []*PrepareShareArgs {
	lc.mu.Lock()
	defer lc.mu.Unlock()
	argsQ := lc.prepareShareQ
	lc.prepareShareQ = nil
	return argsQ
}

func (lc *LogCert) popAllCommits() []*CommitArgs {
	lc.mu.Lock()
	defer lc.mu.Unlock()
	argsQ := lc.commitQ
	lc.commitQ = nil
	return argsQ
}

func (lc *LogCert) popAllCommitShares() []*CommitShareArgs {
	lc.mu.Lock()
	defer lc.mu.Unlock()
	argsQ := lc.commitShareQ
	lc.commitShareQ = nil
	return argsQ
}

func (lc *LogCert) getStage() Stage {
	return atomic.LoadInt32(&lc.stage)
}

func (lc *LogCert) setStage(stage Stage) {
	atomic.StoreInt32(&lc.stage, stage)
}

func (lc *LogCert) prepareShareVoted(nodeId int64) bool {
	lc.mu.Lock()
	defer lc.mu.Unlock()
	_, ok := lc.prepareShares[nodeId]
	return ok
}

func (lc *LogCert) prepareVoted(nodeId int64) bool {
	lc.mu.Lock()
	defer lc.mu.Unlock()
	_, ok := lc.prepares[nodeId]
	return ok
}

func (lc *LogCert) commitShareVoted(nodeId int64) bool {
	lc.mu.Lock()
	defer lc.mu.Unlock()
	_, ok := lc.commitShares[nodeId]
	return ok
}

func (lc *LogCert) commitVoted(nodeId int64) bool {
	lc.mu.Lock()
	defer lc.mu.Unlock()
	_, ok := lc.commits[nodeId]
	return ok
}

func (lc *LogCert) prepareShareVote(args *PrepareShareArgs) {
	lc.mu.Lock()
	defer lc.mu.Unlock()
	lc.prepareShares[args.Msg.NodeId] = args
}

func (lc *LogCert) prepareVote(args *PrepareArgs) {
	lc.mu.Lock()
	defer lc.mu.Unlock()
	lc.prepares[args.Msg.NodeId] = args
}

func (lc *LogCert) commitShareVote(args *CommitShareArgs) {
	lc.mu.Lock()
	defer lc.mu.Unlock()
	lc.commitShares[args.Msg.NodeId] = args
}

func (lc *LogCert) commitVote(args *CommitArgs) {
	lc.mu.Lock()
	defer lc.mu.Unlock()
	lc.commits[args.Msg.NodeId] = args
}

func (lc *LogCert) prepareShareBallot() int {
	lc.mu.Lock()
	defer lc.mu.Unlock()
	return len(lc.prepareShares)
}

func (lc *LogCert) prepareBallot() int {
	lc.mu.Lock()
	defer lc.mu.Unlock()
	return len(lc.prepares)
}

func (lc *LogCert) commitShareBallot() int {
	lc.mu.Lock()
	defer lc.mu.Unlock()
	return len(lc.commitShares)
}

func (lc *LogCert) commitBallot() int {
	lc.mu.Lock()
	defer lc.mu.Unlock()
	return len(lc.commits)
}

type RequestMsg struct {
	Operator  []byte
	Timestamp int64
	ClientId  int64
}

type PrePrepareMsg struct {
	View   int64
	Seq    int64
	Digest []byte
	NodeId int64
	PrimaryNodeId int64
	TxNum int64
}

type PrepareMsg struct {
	View   int64
	Seq    int64
	Digest []byte
	NodeId int64
	PrimaryNodeId int64
	TxNum int64
}

type PrepareShareMsg struct {
	View   int64
	Seq    int64
	Digest []byte
	NodeId int64
	PrimaryNodeId int64
}

type CommitMsg struct {
	View   int64
	Seq    int64
	Digest []byte
	NodeId int64
	PrimaryNodeId int64
	TxNum int64
}

type CommitShareMsg struct {
	View   int64
	Seq    int64
	Digest []byte
	NodeId int64
	PrimaryNodeId int64
}

type ReplyMsg struct {
	View      int64
	Seq       int64
	Timestamp int64
	ClientId  int64
	NodeId    int64
	Result    []byte
}

type RequestArgs struct {
	Req  *RequestMsg
	Sign []byte
	TxNum int
}

type RequestReply struct {
	Seq int64
	Ok  bool
}

type PrePrepareArgs struct {
	Msg     *PrePrepareMsg
	Sign    []byte
	ReqArgs *RequestArgs
}

type PrepareArgs struct {
	Msg  *PrepareMsg
	Sign []byte
}

type PrepareShareArgs struct {
	Msg  *PrepareShareMsg
	Sign []byte
}

type CommitArgs struct {
	Msg  *CommitMsg
	Sign []byte
}

type CommitShareArgs struct {
	Msg  *CommitShareMsg
	Sign []byte
}

type ReplyArgs struct {
	Msg  *ReplyMsg
	Sign []byte
}

type CloseCliCliArgs struct {
	ClientId int64
}
