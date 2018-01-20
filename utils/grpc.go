package utils

import (
	"errors"
	"github.com/tiglabs/containerfs/logger"
	"github.com/tiglabs/containerfs/proto/mp"
	"github.com/tiglabs/containerfs/proto/vp"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
	"time"
)

// GetLeader ...
func GetMetaNodeLeader(hosts []string, UUID string) (string, error) {

	logger.Debug("GetMetaNodeLeader hosts %v", hosts)

	var leader string
	var flag bool
	for _, host := range hosts {
		conn, err := grpc.Dial(host, grpc.WithInsecure(), grpc.WithBlock(), grpc.WithTimeout(time.Millisecond*300), grpc.FailOnNonTempDialError(true))
		if err != nil {
			logger.Error("GetMetaNodeLeader Failed host %v,err %v", host, err)
			continue
		}
		defer conn.Close()
		mc := mp.NewMetaNodeClient(conn)
		pGetMetaLeaderReq := &mp.GetMetaLeaderReq{
			VolID: UUID,
		}
		ctx, _ := context.WithTimeout(context.Background(), time.Second)
		pGetMetaLeaderAck, err := mc.GetMetaLeader(ctx, pGetMetaLeaderReq)
		if err != nil {
			logger.Error("GetMetaNodeLeader GetMetaLeader failed host %v", host)
			time.Sleep(time.Millisecond * 300)
			continue
		}
		if pGetMetaLeaderAck.Ret != 0 {
			logger.Error("GetMetaNodeLeader GetMetaLeader failed host %v", host)
			continue
		}
		leader = pGetMetaLeaderAck.Leader
		flag = true
		break
	}
	if !flag {
		logger.Debug("GetMetaNodeLeader hosts %v", hosts)

		return "", errors.New("Get leader failed")
	}
	logger.Debug("GetMetaNodeLeader success ")

	return leader, nil

}

// Dial ...
func Dial(host string) (*grpc.ClientConn, error) {
	var conn *grpc.ClientConn
	var err error
	conn, err = grpc.Dial(host, grpc.WithInsecure(), grpc.WithBlock(), grpc.WithTimeout(time.Millisecond*300), grpc.FailOnNonTempDialError(true))
	if err != nil {
		time.Sleep(300 * time.Millisecond)
		conn, err = grpc.Dial(host, grpc.WithInsecure(), grpc.WithBlock(), grpc.WithTimeout(time.Millisecond*300), grpc.FailOnNonTempDialError(true))
		if err != nil {
			time.Sleep(300 * time.Millisecond)
			conn, err = grpc.Dial(host, grpc.WithInsecure(), grpc.WithBlock(), grpc.WithTimeout(time.Millisecond*300), grpc.FailOnNonTempDialError(true))
		}
	}
	return conn, err
}

// GetLeader ...
func GetVolMgrLeader(hosts []string) (string, error) {

	var leader string
	var flag bool
	for _, ip := range hosts {
		conn, err := grpc.Dial(ip, grpc.WithInsecure(), grpc.WithBlock(), grpc.WithTimeout(time.Millisecond*300), grpc.FailOnNonTempDialError(true))
		if err != nil {
			continue
		}
		defer conn.Close()
		vc := vp.NewVolMgrClient(conn)
		pGetVolMgrRGReq := &vp.GetVolMgrRGReq{}
		ctx, _ := context.WithTimeout(context.Background(), time.Second)
		pGetVolMgrRGAck, err := vc.GetVolMgrRG(ctx, pGetVolMgrRGReq)
		if err != nil {
			time.Sleep(time.Millisecond * 300)
			continue
		}
		if pGetVolMgrRGAck.Ret != 0 {
			//time.Sleep(time.Millisecond * 300)
			continue
		}
		leader = pGetVolMgrRGAck.Leader
		flag = true
		break
	}
	if !flag {
		return "", errors.New("Get leader failed")
	}
	return leader, nil

}

// DialMeta ...
func DialVolMgr(hosts []string) (string, *grpc.ClientConn, error) {
	var conn *grpc.ClientConn
	var err error

	leader, err := GetVolMgrLeader(hosts)
	if err != nil {
		return "", nil, err
	}
	conn, err = grpc.Dial(leader, grpc.WithInsecure(), grpc.WithBlock(), grpc.WithTimeout(time.Millisecond*300), grpc.FailOnNonTempDialError(true))
	if err != nil {
		time.Sleep(300 * time.Millisecond)
		leader, err = GetVolMgrLeader(hosts)
		if err != nil {
			return "", nil, err
		}
		conn, err = grpc.Dial(leader, grpc.WithInsecure(), grpc.WithBlock(), grpc.WithTimeout(time.Millisecond*300), grpc.FailOnNonTempDialError(true))
		if err != nil {
			time.Sleep(300 * time.Millisecond)
			leader, err = GetVolMgrLeader(hosts)
			if err != nil {
				return "", nil, err
			}
			conn, err = grpc.Dial(leader, grpc.WithInsecure(), grpc.WithBlock(), grpc.WithTimeout(time.Millisecond*300), grpc.FailOnNonTempDialError(true))
		}
	}
	return leader, conn, err
}
