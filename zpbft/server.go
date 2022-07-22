package main

import (
	"crypto/rand"
	"fmt"
	"log"
	"math/big"
	"net/http"
	"net/rpc"
	"sync"
	"sync/atomic"
	"time"
)

type Server struct {
	node      *Node
	seqCh     chan int64
	logs      []*Log
	seq2cert  map[int64]*LogCert
	id2srvCli map[int64]*rpc.Client
	id2cliCli map[int64]*rpc.Client
	mu        sync.Mutex
	txPoolMu        sync.Mutex
	eachInstanceViewLocallyMutex        sync.Mutex
	localNodeSendingTxsMutex        sync.Mutex
	tpsMutex sync.Mutex
	viewEndTimeMu sync.Mutex
	seqInc    int64
	view      int64
	eachInstanceViewLocally map[int64]int64
	currentView int64
	viewCommittedInstance map[int64]int64 //
	localViewCommitted LocalView
	randomDelay int64
	startTime time.Time
	endTime time.Time
	proposers []int64
	isProposer bool
	txPool int64
	localNodeSendingTxs int64
	currentConfirmedTx int64
	delay int64
	cumulative int64
	tps []float64
	roundEndTime []time.Time
	latencyPerRound []float64
	viewEndTime []time.Time
	viewStartTime []time.Time
	latencyPerView []float64
	delayPerView []int64
	sysStartToViewStart []float64
	sysStartToViewEnd []float64
	rotateOrNot bool
	randomDelayOrNot bool
}



func (s *Server) pushTxToPool() {
	// 后8位为节点id
	if s.isProposer{
		for{
			s.txPoolMu.Lock()
			s.txPool += int64(KConfig.Load/(100*KConfig.ProposerNum))
			s.cumulative += int64(KConfig.Load/(100*KConfig.ProposerNum))
			s.txPoolMu.Unlock()
			time.Sleep(10*time.Millisecond)
		}
	}
}
func (s *Server) delayReset(){
	if s.randomDelayOrNot {
		Debug("random delay set==============")
		randomDelay, _ := rand.Int(rand.Reader, big.NewInt(int64(KConfig.Delay)))
		s.randomDelay = randomDelay.Int64()
	}else{
		Debug("random delay not set==============")
		s.randomDelay = int64(KConfig.Delay)
	}

}
func (s *Server) rotateProposers(viewNum int, proposersNum int){
	index := viewNum % len(KConfig.PeerIps)
	for i := 0; i < proposersNum;i++{
		s.proposers[i] = KConfig.PeerIds[(index + i) % len(KConfig.PeerIps)]
	}
}

func (s *Server) assignSeq() int64 {
	// 后8位为节点id
	return atomic.AddInt64(&s.seqInc, 1e10)
}

func (s *Server) getCertOrNew(seq int64) *LogCert {
	s.mu.Lock()
	defer s.mu.Unlock()
	cert, ok := s.seq2cert[seq]
	if !ok {
		cert = &LogCert{
			seq:      seq,
			prepares: make(map[int64]*PrepareArgs),
			prepareShares: make(map[int64]*PrepareShareArgs),
			commits:  make(map[int64]*CommitArgs),
			commitShares: make(map[int64]*CommitShareArgs),
			prepareQ: make([]*PrepareArgs, 0),
			prepareShareQ: make([]*PrepareShareArgs, 0),
			commitQ:  make([]*CommitArgs, 0),
			commitShareQ:  make([]*CommitShareArgs, 0),
		}
		s.seq2cert[seq] = cert
	}
	return cert
}

func (s *Server) RequestRpc(args *RequestArgs, reply *RequestReply) error {
	// 放入请求队列直接返回，后续异步通知客户端

	Debug("RequestRpc, from: %d", args.Req.ClientId)
	//构造请求
	req := &RequestMsg{
		Operator:  make([]byte, KConfig.BatchTxNum*KConfig.TxSize),
		Timestamp: time.Now().UnixNano(),
		ClientId:  args.Req.ClientId,
	}
	node := GetNode(args.Req.ClientId)
	digest := Sha256Digest(req)
	sign := RsaSignWithSha256(digest, node.priKey)

	args = &RequestArgs{
		Req:  req,
		Sign: sign,
	}
	// 验证RequestMsg
	// node := GetNode(args.Req.ClientId)
	// digest := Sha256Digest(args.Req)
	// ok := RsaVerifyWithSha256(digest, args.Sign, node.pubKey)
	// if !ok {
	// 	Warn("RequestMsg verify error, from: %d", args.Req.ClientId)
	// 	reply.Ok = false
	// 	return nil
	// }

	// leader 分配seq
	seq := s.assignSeq()
	//主节点新建logCert，设置参数，并存储在seq2cert中
	s.getCertOrNew(seq).set(args, digest, s.view,s.node.id)
	//向共识线程发送开始共识信号
	s.seqCh <- seq

	// 返回信息
	reply.Seq = seq
	reply.Ok = true

	return nil
}
//主节点发送pre-prepare消息给从节点
func (s *Server) PrePrepare(seq int64) {
	//共识线程从seq2cert中获取请求参数，并构造pre-prepare消息

	req, digest, viewNum, primary := s.getCertOrNew(seq).get()

	view := s.localViewCommitted.getView(viewNum)
	s.sysStartToViewStart[viewNum-1] = view.startTime.Sub(s.startTime).Seconds()

	msg := &PrePrepareMsg{
		View:   viewNum,
		Seq:    seq,
		Digest: digest,
		NodeId: s.node.id,
		PrimaryNodeId: primary,
		TxNum: int64(req.TxNum),
	}
	digest = Sha256Digest(msg)
	sign := RsaSignWithSha256(digest, s.node.priKey)
	// 配置rpc参数
	args := &PrePrepareArgs{
		Msg:     msg,
		Sign:    sign,
		ReqArgs: req,
	}
	//probabilityDelay, _ := rand.Int(rand.Reader, big.NewInt(22))
	//if probabilityDelay.Int64() <= 20 {
	time.Sleep(time.Duration(s.randomDelay)*time.Millisecond)
	//}
	// 等待发完2f个节点再进入下一阶段
	for id, srvCli := range s.id2srvCli {
		id1, srvCli1 := id, srvCli
		go func() { // 异步发送
			//var reply bool
			var returnArgs PrepareShareArgs
			err := srvCli1.Call("Server.PrePrepareRpc", args, &returnArgs)
			if err != nil {
				Error("Server.PrePrepareRpc %d error: %v", id1, err)
			}
			//returnMsg := returnArgs.Msg
			//Debug("PrepareShareRpc, seq: %d, from: %d", msg.Seq, id1)
			// 这里先不验证，因为可能 req 消息还未收到，先存下投票信息后期验证
			if &returnArgs == nil{
				Debug("ERROR,ERROR,ERROR,ERROR,ERROR,ERROR")
			}
			cert := s.getCertOrNew(msg.Seq)
			cert.pushPrepareShare(&returnArgs)
			s.verifyBallot(cert)

		}()
	}
    //s.getCertOrNew(seq).set(nil, digest, view, primary)
	Debug("PrePrepare %d ok", seq)
	//s.Prepare(seq)
}
//从节点接收pre-prepare消息
func (s *Server) PrePrepareRpc(args *PrePrepareArgs, returnArgs *PrepareShareArgs) error {
	msg := args.Msg
	//Debug("PrePrepareRpc, seq: %d, from: %d", msg.Seq, msg.NodeId)
	// 预设返回失败
	//*reply = false
	// 验证PrePrepareMsg
	Debug("pre-prepare view = %d, primaryNodeId = %d",msg.View, msg.PrimaryNodeId)
	node := GetNode(msg.NodeId)
	digest := Sha256Digest(msg)
	ok := RsaVerifyWithSha256(digest, args.Sign, node.pubKey)
	if !ok {
		Warn("PrePrepareMsg verify error, seq: %d, from: %d", msg.Seq, msg.NodeId)
		return nil
	}
	//如果不是proposer
	if !s.isProposerOrNot(int(msg.View), msg.PrimaryNodeId){
		fmt.Print(KConfig.ProposerIds)
		Debug("\nprimaryNode id = %d", msg.PrimaryNodeId)
		return nil
	}
	// 验证RequestMsg
	reqArgs := args.ReqArgs
	//node = GetNode(reqArgs.Req.ClientId)
	node = GetNode(msg.NodeId)
	digest = Sha256Digest(reqArgs.Req)
	if !SliceEqual(digest, msg.Digest) {
		Warn("PrePrepareMsg error, req.digest != msg.Digest")
		return nil
	}
	//ok = RsaVerifyWithSha256(digest, reqArgs.Sign, node.pubKey)
	//if !ok {
	//	Warn("RequestMsg verify error, seq: %d, from: %d", msg.Seq, msg.NodeId)
	//	return nil
	//}
	// 设置证明
	//从节点收到pre-prepare消息之后，将对应的请求存储到seq2cert中
	cert := s.getCertOrNew(msg.Seq)
	if cert.committed {
		return nil
	}
	//cert.set(reqArgs, digest, msg.View,msg.PrimaryNodeId)
	cert.set(nil, digest, msg.View, msg.PrimaryNodeId)
	_, digest, viewNum, primary:= s.getCertOrNew(msg.Seq).get()

	view := s.localViewCommitted.getView(viewNum)
	s.sysStartToViewStart[viewNum-1] = view.startTime.Sub(s.startTime).Seconds()
	s.viewStartTime[viewNum-1] = view.startTime

	returnMsg := &PrepareShareMsg{
		View:   viewNum,
		Seq:    cert.seq,
		Digest: digest,
		NodeId: s.node.id,
		PrimaryNodeId: primary,
	}
	digest = Sha256Digest(returnMsg)
	sign := RsaSignWithSha256(digest, s.node.priKey)
	returnArgs.Msg = returnMsg
	returnArgs.Sign = sign
	// 配置rpc参数,相比PrePrepare无需req
	// 进入PrepareShare投票
	//go s.PrepareShare(cert.seq)
	// 计票
	//s.verifyBallot(cert)
	// 返回成功
	//*reply = true
	return nil
}
//从节点发送prepareShare消息
//func (s *Server) PrepareShare(seq int64) {
//	_, digest, view, primary:= s.getCertOrNew(seq).get()
//	msg := &PrepareShareMsg{
//		View:   view,
//		Seq:    seq,
//		Digest: digest,
//		NodeId: s.node.id,
//		PrimaryNodeId: primary,
//	}
//	digest = Sha256Digest(msg)
//	sign := RsaSignWithSha256(digest, s.node.priKey)
//	// 配置rpc参数,相比PrePrepare无需req
//	args := &PrepareShareArgs{
//		Msg:  msg,
//		Sign: sign,
//	}
//	for id, srvCli := range s.id2srvCli {
//		if id == msg.PrimaryNodeId {
//			id1, srvCli1 := id, srvCli
//			go func() { // 异步发送
//				var reply bool
//				err := srvCli1.Call("Server.PrepareShareRpc", args, &reply)
//				if err != nil {
//					Error("Server.PrepareShareRpc %d error: %v", id1, err)
//				}
//			}()
//		}
//	}
//}
//主节点接收PrepareShare消息，将PrepareShare签名聚合
//func (s *Server) PrepareShareRpc(args *PrepareShareArgs, reply *bool) error {
//	msg := args.Msg
//	Debug("PrepareShareRpc, seq: %d, from: %d", msg.Seq, msg.NodeId)
//
//	// 这里先不验证，因为可能 req 消息还未收到，先存下投票信息后期验证
//	cert := s.getCertOrNew(msg.Seq)
//	cert.pushPrepareShare(args)
//	s.verifyBallot(cert)
//
//	*reply = true
//	return nil
//}

//主节点发送prepare消息
func (s *Server) Prepare(seq int64) {
	//time.Sleep(time.Duration(s.randomDelay)*time.Millisecond)
	req, digest, view, primary := s.getCertOrNew(seq).get()
	msg := &PrepareMsg{
		View:   view,
		Seq:    seq,
		Digest: digest,
		NodeId: s.node.id,
		PrimaryNodeId: primary,
		TxNum: int64(req.TxNum),
	}
	digest = Sha256Digest(msg)
	sign := RsaSignWithSha256(digest, s.node.priKey)
	// 配置rpc参数,相比PrePrepare无需req
	args := &PrepareArgs{
		Msg:  msg,
		Sign: sign,
	}
	for id, srvCli := range s.id2srvCli {
		id1, srvCli1 := id, srvCli
		go func() { // 异步发送
			//var reply bool
			var returnArgs CommitShareArgs
			err := srvCli1.Call("Server.PrepareRpc", args, &returnArgs)
			if err != nil {
				Error("Server.PrepareRpc %d error: %v", id1, err)
			}
			//returnMsg := returnArgs.Msg
			//Debug("CommitShareRpc, seq: %d, from: %d", returnMsg.Seq, returnMsg.NodeId)

			// 这里先不验证，因为可能 req 消息还未收到，先存下投票信息后期验证
			cert := s.getCertOrNew(msg.Seq)
			cert.pushCommitShare(&returnArgs)
			s.verifyBallot(cert)
		}()
	}
}
//从节点接收prepare消息
func (s *Server) PrepareRpc(args *PrepareArgs, returnArgs *CommitShareArgs) error {
	msg := args.Msg
	//time.Sleep(time.Duration(s.randomDelay)*time.Millisecond)
	//Debug("PrepareRpc, seq: %d, from: %d", msg.Seq, msg.NodeId)

	// 这里先不验证，因为可能 req 消息还未收到，先存下投票信息后期验证
	Debug("prepare view = %d, primaryNodeId = %d",msg.View, msg.PrimaryNodeId)
	cert := s.getCertOrNew(msg.Seq)

	node := GetNode(msg.PrimaryNodeId)
	digest := Sha256Digest(msg)
	ok := RsaVerifyWithSha256(digest, args.Sign, node.pubKey)
	if !ok {
		Warn("PrepareShareMsg verify error, seq: %d, from: %d", msg.Seq, msg.NodeId)
		return nil
	}
	if !s.isProposerOrNot(int(msg.View),msg.PrimaryNodeId){
		return nil
	}
	cert.prepared.Lock()
	cert.prepareShared = true
	cert.prepared.Unlock()

	_, digest, view, primary := s.getCertOrNew(cert.seq).get()
	returnMsg := &CommitShareMsg{
		View:   view,
		Seq:    cert.seq,
		Digest: digest,
		NodeId: s.node.id,
		PrimaryNodeId: primary,
	}
	digest = Sha256Digest(returnMsg)
	sign := RsaSignWithSha256(digest, s.node.priKey)
	// 配置rpc参数,相比PrePrepare无需req
	returnArgs.Msg = returnMsg
	returnArgs.Sign = sign

	//go s.CommitShare(cert.seq)
	// 验证PrepareMsg
	//cert.pushPrepare(args)
	//s.verifyBallot(cert)
	//
	//*reply = true
	return nil
}
//从节点发送CommitShare消息
//func (s *Server) CommitShare(seq int64) {
//	// cmd := s.getCertOrNew(seq).getCmd() // req一定存在
//	_, digest, view, primary := s.getCertOrNew(seq).get()
//	msg := &CommitShareMsg{
//		View:   view,
//		Seq:    seq,
//		Digest: digest,
//		NodeId: s.node.id,
//		PrimaryNodeId: primary,
//	}
//	digest = Sha256Digest(msg)
//	sign := RsaSignWithSha256(digest, s.node.priKey)
//	// 配置rpc参数,相比PrePrepare无需req
//	args := &CommitShareArgs{
//		Msg:  msg,
//		Sign: sign,
//	}
//	for id, srvCli := range s.id2srvCli {
//		if id == msg.PrimaryNodeId {
//			id1, srvCli1 := id, srvCli
//			go func() { // 异步发送
//				var reply bool
//				err := srvCli1.Call("Server.CommitShareRpc", args, &reply)
//				if err != nil {
//					Error("Server.CommitShareRpc %d error: %v", id1, err)
//				}
//			}()
//		}
//	}
//}
//主节点接收commitShare，并聚合
//func (s *Server) CommitShareRpc(args *CommitShareArgs, reply *bool) error {
//	msg := args.Msg
//	Debug("CommitShareRpc, seq: %d, from: %d", msg.Seq, msg.NodeId)
//
//	// 这里先不验证，因为可能 req 消息还未收到，先存下投票信息后期验证
//	cert := s.getCertOrNew(msg.Seq)
//	cert.pushCommitShare(args)
//	s.verifyBallot(cert)
//
//	*reply = true
//	return nil
//}


//主节点将聚合的commitShare消息广播出去
func (s *Server) Commit(seq int64, views int64) {
	// cmd := s.getCertOrNew(seq).getCmd() // req一定存在
	//time.Sleep(time.Duration(s.randomDelay)*time.Millisecond)
	Debug("primary node calling commit of view %d", views)
	_, digest, view, primary := s.getCertOrNew(seq).get()

	msg := &CommitMsg{
		View:   view,
		Seq:    seq,
		Digest: digest,
		NodeId: s.node.id,
		PrimaryNodeId: primary,
		//TxNum: int64(req.TxNum),
	}
	digest = Sha256Digest(msg)
	sign := RsaSignWithSha256(digest, s.node.priKey)
	// 配置rpc参数,相比PrePrepare无需req
	args := &CommitArgs{
		Msg:  msg,
		Sign: sign,
	}
	for id, srvCli := range s.id2srvCli {
		id1, srvCli1 := id, srvCli
		go func() { // 异步发送
			var reply bool
			err := srvCli1.Call("Server.CommitRpc", args, &reply)
			if err != nil {
				Error("Server.CommitRpc %d error: %v", id1, err)
			}
			//Debug("Send commit msg of view %d to Server %d %v", view, id1, reply)
		}()
	}
}
//从节点接收commit消息
func (s *Server) CommitRpc(args *CommitArgs, reply *bool) error {
	msg := args.Msg
	/*
	 * From TX :
	 * 更新本地节点维护的其他节点的view
	*/
	Debug("commit view = %d, primaryNodeId = %d",msg.View,msg.PrimaryNodeId)
	s.eachInstanceViewLocallyMutex.Lock()
	if msg.View > s.eachInstanceViewLocally[msg.PrimaryNodeId]{
		s.eachInstanceViewLocally[msg.PrimaryNodeId] ++
	}
	s.eachInstanceViewLocallyMutex.Unlock()
	if !s.isProposerOrNot(int(msg.View), msg.PrimaryNodeId){
		return nil
	}

	/*
	 * From TX :
	 * 判断一个view中，所有instance是否已经完成。
	 * 通过view.refreshViewCommittedOrNot方法判断是否完成，
	 * 若完成，则将view.committedOrNot更新为true
	*/
	s.localViewCommitted.localViewMutex.Lock()
	view := s.localViewCommitted.getView(msg.View)
	if view.committedOrNot{
		s.localViewCommitted.localViewMutex.Unlock()
		return nil
	}
	view.committedInstance[msg.PrimaryNodeId] = true
	//Debug("view committed or not is %t",view.committedOrNot)
	if !view.committedOrNot{
		view.refreshViewCommittedOrNot(int64(len(s.proposers)))
		if view.committedOrNot{
			s.viewEndTimeMu.Lock()
			s.viewEndTime[msg.View-1] = time.Now()
			s.viewEndTimeMu.Unlock()
			s.sysStartToViewEnd[msg.View-1] = time.Now().Sub(s.startTime).Seconds()
			Debug("############### view %d completed ###############", msg.View)
			//Debug("view completed delay = %f", time.Now().Sub(s.startTime).Seconds() / float64(msg.View))
			//s.viewEndTime[msg.View-1] = time.Now()
			//if msg.View == 1{
			//	s.latencyPerView[msg.View-1] = s.viewEndTime[msg.View-1].Sub(s.startTime).Seconds()
			//}else{
			//	s.latencyPerView[msg.View-1] = s.viewEndTime[msg.View-1].Sub(s.viewEndTime[msg.View-2]).Seconds()
			//}
			//fmt.Print(s.latencyPerView)
			//fmt.Print(s.viewEndTime)
		}
	}
	s.localViewCommitted.localViewMutex.Unlock()

	if msg.View == 100 {
		for i := 0; i < 100; i++ {
			s.latencyPerView[i] = s.sysStartToViewEnd[i] - s.sysStartToViewStart[i]
		}
		//fmt.Print(s.sysStartToViewStart)
		//fmt.Print(s.sysStartToViewEnd)
		//fmt.Print(s.latencyPerView)
	}


	// 这里先不验证，因为可能 req 消息还未收到，先存下投票信息后期验证
	cert := s.getCertOrNew(msg.Seq)
	//cert.pushCommit(args)

	node := GetNode(msg.PrimaryNodeId)
	digest := Sha256Digest(msg)
	ok := RsaVerifyWithSha256(digest, args.Sign, node.pubKey)
	if !ok {
		Warn("PrepareShareMsg verify error, seq: %d, from: %d", msg.Seq, msg.NodeId)
		return nil
	}
	//cert.commitShared = true
	//s.verifyBallot(cert)
	cert.logCommitted()
	cert.setStage(ReplyStage)
	*reply = true
	return nil
}

func (s *Server) verifyBallot(cert *LogCert) {
	req, reqDigest, view, _ := cert.get()

	// cmd 为空则不进行后续阶段
	if req == nil {
		Debug("march, cmd is nil")
		return
	}
	cert.committedMutex.Lock()
	if cert.committed{
		return
	}
	cert.committedMutex.Unlock()

	switch cert.getStage() {
	case PrepareStage:
		//从prepareQ中取出所有prepare消息
		argsQ := cert.popAllPrepareShares()
		for _, args := range argsQ {
			msg := args.Msg
			//Debug("msg nodeId = %d", msg.NodeId)
			if cert.prepareShareVoted(msg.NodeId) { // 已投票
				continue
			}
			if view != msg.View {
				Warn("PrepareShareMsg error, view(%d) != msg.View(%d)", view, msg.View)
				continue
			}
			if !SliceEqual(reqDigest, msg.Digest) {
				Warn("PrePrepareMsg error, req.digest != msg.Digest")
				continue
			}
			// 验证PrepareShareMsg
			node := GetNode(msg.NodeId)
			digest := Sha256Digest(msg)
			ok := RsaVerifyWithSha256(digest, args.Sign, node.pubKey)
			if !ok {
				Warn("PrepareShareMsg verify error, seq: %d, from: %d", msg.Seq, msg.NodeId)
				continue
			}
			//投prepare票
			cert.prepareShareVote(args)
		}
		// 2f + 1 (包括自身) 后进入 commit 阶段
		if cert.prepareShareBallot() >= 2*KConfig.FaultNum {
			cert.setStage(CommitStage)
			go s.Prepare(cert.seq)
		}
		fallthrough // 进入后续判断
	case CommitStage:
		argsQ := cert.popAllCommitShares()
		for _, args := range argsQ {
			msg := args.Msg
			if cert.commitShareVoted(msg.NodeId) { // 已投票
				continue
			}
			//if view != msg.View {
			//	Warn("commitShareMsg error, view(%d) != msg.View(%d)", view, msg.View)
			//	continue
			//}
			//if !SliceEqual(reqDigest, msg.Digest) {
			//	Warn("PrePrepareMsg error, req.digest != msg.Digest")
			//	continue
			//}
			// 验证PrepareMsg
			node := GetNode(msg.NodeId)
			digest := Sha256Digest(msg)
			ok := RsaVerifyWithSha256(digest, args.Sign, node.pubKey)
			if !ok {
				Warn("PrepareMsg verify error, seq: %d, from: %d", msg.Seq, msg.NodeId)
				continue
			}
			cert.commitShareVote(args)
		}
		// 2f + 1 (包括自身) 后进入 reply 阶段
		if cert.commitShareBallot() >= 2*KConfig.FaultNum {
			cert.setStage(ReplyStage)
			cert.committedMutex.Lock()
			cert.committed = true
			cert.committedMutex.Unlock()

			s.eachInstanceViewLocallyMutex.Lock()
			if cert.view > s.eachInstanceViewLocally[cert.primary]{
				s.eachInstanceViewLocally[cert.primary] ++
			}
			s.eachInstanceViewLocallyMutex.Unlock()
			//s.localViewCommitted.views[cert.view].committedInstance[cert.primary] = true
			//view := s.localViewCommitted.views[cert.view]
			s.localViewCommitted.localViewMutex.Lock()
			//新建一个view
			view := s.localViewCommitted.getView(cert.view)
			view.committedInstance[cert.primary] = true

			if !view.committedOrNot{
				view.refreshViewCommittedOrNot(int64(len(s.proposers)))
				Debug("current view num = %d", view.viewNum)
				if view.committedOrNot{
					s.viewEndTimeMu.Lock()
					s.viewEndTime[cert.view-1] = time.Now()
					s.viewEndTimeMu.Unlock()
					s.currentConfirmedTx += int64(req.TxNum)
					s.sysStartToViewEnd[cert.view-1] = time.Now().Sub(s.startTime).Seconds()
					Debug("view completed delay = %f", time.Now().Sub(s.startTime).Seconds() / float64(cert.view))
					Debug("============= view %d completed =============", cert.view)
					Debug("random delay of this server is = %d ms", s.randomDelay)
				}
			}
			s.localViewCommitted.localViewMutex.Unlock()

			s.tpsMutex.Lock()
			/*
			* From TX:
			* 在一个线程进入时，会将s.tps[cert.view-1]的初始0修改，
			* 因此在这里用s.tps[cert.view-1] == 0这个判断条件
			* 保证只会有一个线程修改相关的变量
			 */
			if s.tps[cert.view-1] == 0{
				go s.Commit(cert.seq, cert.view)
				Debug("cert.req.TxNum = %d", cert.req.TxNum)
				s.txPoolMu.Lock()
				Debug("local node sending txs number = %d txPool = %d", s.localNodeSendingTxs, s.txPool)
				s.txPoolMu.Unlock()
				fmt.Print(s.tps)
				s.roundEndTime[cert.view-1] = time.Now()

				if cert.view == 1{
					s.tps[cert.view-1] = float64(cert.req.TxNum) / s.roundEndTime[cert.view-1].Sub(s.startTime).Seconds()
					s.latencyPerRound[cert.view-1] = s.roundEndTime[cert.view-1].Sub(s.startTime).Seconds()
				}else{
					s.tps[cert.view-1] = float64(cert.req.TxNum) / s.roundEndTime[cert.view-1].Sub(s.roundEndTime[cert.view-2]).Seconds()
					s.latencyPerRound[cert.view-1] = s.roundEndTime[cert.view-1].Sub(s.roundEndTime[cert.view-2]).Seconds()
				}
				if cert.view % 5 == 0{
					s.viewEndTimeMu.Lock()
					for i := 0; i < int(cert.view); i++{
						//s.latencyPerView[i] = s.viewEndTime[i].Sub(s.viewStartTime[i]).Seconds()
						s.latencyPerView[i] = s.sysStartToViewEnd[i] - s.sysStartToViewStart[i]
					}

					//fmt.Print(s.sysStartToViewStart)
					//fmt.Print(s.sysStartToViewEnd)
					//fmt.Print(s.latencyPerView)
					Debug("TPS : %f ; Latency : %f",float64(s.localNodeSendingTxs)/s.roundEndTime[cert.view - 1].Sub(s.startTime).Seconds(),s.roundEndTime[cert.view - 1].Sub(s.startTime).Seconds()/float64(cert.view))
					s.viewEndTimeMu.Unlock()
				}

				if s.view < 100{
					s.delayPerView[s.view] = s.randomDelay
					s.makeReq()
					//randomDelay, _ := rand.Int(rand.Reader, big.NewInt(int64(KConfig.Delay)))
					//s.randomDelay = randomDelay.Int64()
					s.delayReset()
					Debug("Entering a new view = %d", s.view)
				}
			}
			s.seq2cert[cert.seq].req.Req.Operator = nil
			s.tpsMutex.Unlock()
			//go s.Reply(cert.seq)
		}
	}
}

func (s *Server) Reply(seq int64) {
	Debug("Reply %d", seq)
	req, _, view,_ := s.getCertOrNew(seq).get()
	msg := &ReplyMsg{
		View:      view,
		Seq:       seq,
		Timestamp: time.Now().UnixNano(),
		ClientId:  req.Req.ClientId,
		NodeId:    s.node.id,
		// Result:    req.Req.Operator,
	}
	digest := Sha256Digest(msg)
	sign := RsaSignWithSha256(digest, s.node.priKey)
	replyArgs := &ReplyArgs{
		Msg:  msg,
		Sign: sign,
	}
	var reply bool
	cliCli := s.getCliCli(req.Req.ClientId)
	if cliCli == nil {
		Warn("can't connect client %d", req.Req.ClientId)
		return
	}
	err := cliCli.Call("Client.ReplyRpc", replyArgs, &reply)
	if err != nil {
		Warn("Client.ReplyRpc error: %v", err)
		s.closeCliCli(req.Req.ClientId)
	}
}

func (s *Server) getCliCli(clientId int64) *rpc.Client {
	s.mu.Lock()
	defer s.mu.Unlock()
	cliCli, ok := s.id2cliCli[clientId]
	if !ok || cliCli == nil {
		node := GetNode(clientId)
		var err error
		cliCli, err = rpc.DialHTTP("tcp", node.addr)
		if err != nil {
			Warn("connect client %d error: %v", node.addr, err)
			return nil
		}
		s.id2cliCli[clientId] = cliCli
	}
	return cliCli
}

func (s *Server) closeCliCli(clientId int64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	Info("close connect with client %d", clientId)
	cliCli, ok := s.id2cliCli[clientId]
	if ok && cliCli != nil {
		cliCli = nil
		delete(s.id2cliCli, clientId)
	}
}

func (s *Server) CloseCliCliRPC(args *CloseCliCliArgs, reply *bool) error {
	s.closeCliCli(args.ClientId)
	*reply = true
	return nil
}

func (s *Server) connect() {
	ok := false
	for !ok {
		time.Sleep(time.Second) // 每隔一秒进行连接
		Info("build connect...")
		ok = true
		for id, node := range KConfig.Id2Node {
			if node == s.node {
				continue
			}
			if s.id2srvCli[id] == nil {
				cli, err := rpc.DialHTTP("tcp", node.addr)
				if err != nil {
					Warn("connect %s error: %v", node.addr, err)
					ok = false
				} else {
					s.id2srvCli[id] = cli
				}

			}
		}
	}
	Info("== connect success ==")
}

func (s *Server) isProposerOrNot(viewNum int,nodeId int64)bool{
	if !s.rotateOrNot{
		for _,proposerId := range s.proposers{
			if nodeId == proposerId{
				return true
			}
		}
	}else{
		index := viewNum % len(KConfig.PeerIds)
		for i := 0; i < len(s.proposers);i++{
			if KConfig.PeerIds[(index + i) % len(KConfig.PeerIds)] == nodeId{
				return true
			}
		}
	}
	return false
}

func (s *Server) makeReq(){
	var realBatchTxNum int
	s.txPoolMu.Lock()
	if s.txPool > int64(KConfig.BatchTxNum){
		realBatchTxNum = KConfig.BatchTxNum
		if s.view != 200{
			s.txPool -= int64(KConfig.BatchTxNum)
		}else{
			realBatchTxNum = 0
			s.txPoolMu.Unlock()
			time.Sleep(2000*time.Millisecond)
			s.txPoolMu.Lock()
		}
		s.localNodeSendingTxsMutex.Lock()
		s.localNodeSendingTxs += int64(realBatchTxNum)
		s.localNodeSendingTxsMutex.Unlock()

		s.txPoolMu.Unlock()
	}else{
		if s.txPool == 0{
			s.txPoolMu.Unlock()
			time.Sleep(100*time.Millisecond)
			s.txPoolMu.Lock()
		}
		realBatchTxNum = int(s.txPool)
		if s.view != 200 {
			s.txPool = 0
		}else{
			realBatchTxNum = 0
			s.txPoolMu.Unlock()
			time.Sleep(2000*time.Millisecond)
			s.txPoolMu.Lock()
		}
		s.localNodeSendingTxsMutex.Lock()
		s.localNodeSendingTxs += int64(realBatchTxNum)
		s.localNodeSendingTxsMutex.Unlock()

		Debug("txPool = %d", s.txPool)
		s.txPoolMu.Unlock()
	}

		//time.Sleep(100*time.Millisecond)


	Debug("batchTxNum = %d, txSize = %d",realBatchTxNum,KConfig.TxSize)
	req := &RequestMsg{
		Operator:  make([]byte, realBatchTxNum*KConfig.TxSize),
		Timestamp: time.Now().UnixNano(),
		//ClientId:  args.Req.ClientId,
	}
	// digest := Sha256Digest(req)
	// sign := RsaSignWithSha256(digest, c.node.priKey)
	//args := &RequestArgs{
	//	Req: req,
    //    TxNum: realBatchTxNum,
	//	// Sign: sign,
	//}
	Debug("RequestRpc, from: %d", s.node.id)
	//构造请求

	//node := GetNode(args.Req.ClientId)
	node := GetNode(s.node.id)
	digest := Sha256Digest(req)
	sign := RsaSignWithSha256(digest, node.priKey)

	args := &RequestArgs{
		Req:  req,
		TxNum: realBatchTxNum,
		Sign: sign,
	}
	// 验证RequestMsg
	// node := GetNode(args.Req.ClientId)
	// digest := Sha256Digest(args.Req)
	// ok := RsaVerifyWithSha256(digest, args.Sign, node.pubKey)
	// if !ok {
	// 	Warn("RequestMsg verify error, from: %d", args.Req.ClientId)
	// 	reply.Ok = false
	// 	return nil
	// }
	// leader 分配seq

	seq := s.assignSeq()
	Debug("assign sequence = %d", seq)
	//主节点新建logCert，设置参数，并存储在seq2cert中
	s.view++
	s.getCertOrNew(seq).set(args, digest, s.view,s.node.id)
	//向共识线程发送开始共识信号
	Debug("*****************    view = %d", s.view)
	s.seqCh <- seq
}

func (s *Server) workLoop() {
	Info("== start work loop ==")

    startTime := time.Now()
    if s.isProposer{
		s.makeReq()
	}
	fmt.Printf("start time = %v ", startTime)
	for seq := range s.seqCh {
		s.PrePrepare(seq)
	}
}

func (s *Server)calculateTPS(){
	for i:= 0; i < 100;i++{
		timeNow := time.Now()
		s.tps[i] = float64(s.localNodeSendingTxs) / timeNow.Sub(s.startTime).Seconds()
		time.Sleep(1*time.Second)
	}
}

func (s *Server) Start() {
	s.connect()
	s.workLoop()
}

func RunServer(id int64, delayRange int64) {
	//view := View{
	//	committedInstance: make(map[int64]bool),
	//}
	views := make(map[int64]View, 0)
	var localView LocalView
	localView.views = views
	localView.currentStableViewHeight = 0

	//randomDelay, _ := rand.Int(rand.Reader, big.NewInt(int64(KConfig.Delay)))
	//rand.Seed(time.)
	//randomDelay := rand.Intn(10)

	server := &Server{
		node:      KConfig.Id2Node[id],
		seqCh:     make(chan int64, ChanSize),
		logs:      make([]*Log, 0),
		eachInstanceViewLocally: make(map[int64]int64),
		viewCommittedInstance: make(map[int64]int64),
		seq2cert:  make(map[int64]*LogCert),
		id2srvCli: make(map[int64]*rpc.Client),
		id2cliCli: make(map[int64]*rpc.Client),
		localViewCommitted: localView,
		//randomDelay: randomDelay.Int64(),
		randomDelay: 0,
		startTime: time.Now(),
		proposers: make([]int64, KConfig.ProposerNum),
		isProposer: KConfig.IsProposer,
		delay: int64(KConfig.Delay),
		tps: make([]float64, 100),
		roundEndTime: make([]time.Time, 100),
		latencyPerRound: make([]float64, 100),
		viewEndTime: make([]time.Time, 100),
		viewStartTime: make([]time.Time, 100),
		latencyPerView: make([]float64, 100),
		delayPerView: make([]int64, 100),
		sysStartToViewStart: make([]float64, 100),
		sysStartToViewEnd: make([]float64, 100),
		rotateOrNot: KConfig.RotateOrNot,
		randomDelayOrNot: KConfig.RandomDelayOrNot,
	}
	server.delayReset()
	Debug("random delay is %d ms",server.randomDelay)
	for _, nodeId := range KConfig.PeerIds{
		server.eachInstanceViewLocally[nodeId] = 0
	}
	for i := 0; i < KConfig.ProposerNum;i++{
		server.proposers[i] = KConfig.ProposerIds[i]
		//Debug("proposer id = %d",server.proposers[i])
	}
	// 每个分配序号后缀为节点id(8位)
	server.seqInc = server.node.id
	// 当前暂无view-change, view暂且设置为server id
	//server.view = server.node.id
	server.view = 0
	server.txPool = 0
	server.currentConfirmedTx = 0
	server.localNodeSendingTxs = 0
	server.cumulative = 0

	go server.Start()
	go server.pushTxToPool()

	rpc.Register(server)
	rpc.HandleHTTP()
	if err := http.ListenAndServe(server.node.addr, nil); err != nil {
		log.Fatal("server error: ", err)
	}
}
