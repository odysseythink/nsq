package nsqd

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"path"
	"regexp"
	"strings"
	"sync"
	"time"

	"github.com/hashicorp/raft"
	raftboltdb "github.com/hashicorp/raft-boltdb"
)

type kvFsm struct {
	db sync.Map
}

type setPayload struct {
	Key   string
	Value string
}

// 对follower节点来说，leader会通知它来commit log entry，被commit的log entry需要调用应用层提供的Apply方法来执行日志，这里就是从logEntry拿到具体的数据，然后写入缓存里面即可。
func (kf *kvFsm) Apply(log *raft.Log) interface{} {
	fmt.Println("****************-----", log.Type)
	switch log.Type {
	case raft.LogCommand:
		var sp setPayload
		err := json.Unmarshal(log.Data, &sp)
		if err != nil {
			return fmt.Errorf("could not parse payload: %s", err)
		}

		kf.db.Store(sp.Key, sp.Value)
	case raft.LogAddPeerDeprecated:
		fmt.Println("***********LogAddPeerDeprecated")
	case raft.LogRemovePeerDeprecated:
		fmt.Println("***********LogRemovePeerDeprecated")
	case raft.LogConfiguration:
		fmt.Println("***********LogConfiguration")
	default:
		return fmt.Errorf("unknown raft log type: %#v", log.Type)
	}

	return nil
}

type snapshotNoop struct{}

func (sn snapshotNoop) Persist(_ raft.SnapshotSink) error { return nil }
func (sn snapshotNoop) Release()                          {}

func (kf *kvFsm) Snapshot() (raft.FSMSnapshot, error) {
	return snapshotNoop{}, nil
}

// 服务重启的时候，会先读取本地的快照来恢复数据，在FSM里面定义的Restore函数会被调用，这里我们就简单的对数据解析json反序列化然后写入内存即可。至此，我们已经能够正常的保存快照，也能在重启的时候从文件恢复快照数据。
func (kf *kvFsm) Restore(rc io.ReadCloser) error {
	// deleting first isn't really necessary since there's no exposed DELETE operation anyway.
	// so any changes over time will just get naturally overwritten

	decoder := json.NewDecoder(rc)

	for decoder.More() {
		var sp setPayload
		err := decoder.Decode(&sp)
		if err != nil {
			return fmt.Errorf("could not decode payload: %s", err)
		}

		kf.db.Store(sp.Key, sp.Value)
	}

	return rc.Close()
}

func setupRaft(dir, nodeId, raftAddress string, servers map[string]string) (*raft.Raft, error) {
	kf := &kvFsm{}
	err := os.MkdirAll(dir, os.ModePerm)
	if err != nil {
		return nil, fmt.Errorf("could not create data directory: %s", err)
	}

	store, err := raftboltdb.NewBoltStore(path.Join(dir, "bolt"))
	if err != nil {
		return nil, fmt.Errorf("could not create bolt store: %s", err)
	}

	snapshots, err := raft.NewFileSnapshotStore(path.Join(dir, "snapshot"), 2, os.Stderr)
	if err != nil {
		return nil, fmt.Errorf("could not create snapshot store: %s", err)
	}

	tcpAddr, err := net.ResolveTCPAddr("tcp", raftAddress)
	if err != nil {
		return nil, fmt.Errorf("could not resolve address: %s", err)
	}

	transport, err := raft.NewTCPTransport(raftAddress, tcpAddr, 10, time.Second*10, os.Stderr)
	if err != nil {
		return nil, fmt.Errorf("could not create tcp transport: %s", err)
	}

	raftCfg := raft.DefaultConfig()
	raftCfg.LocalID = raft.ServerID(nodeId)

	r, err := raft.NewRaft(raftCfg, kf, store, store, snapshots, transport)
	if err != nil {
		return nil, fmt.Errorf("could not create raft instance: %s", err)
	}
	fmt.Println("--------localAddr=", transport.LocalAddr())
	if len(servers) > 0 {
		tmp := make([]raft.Server, 0)
		for k, v := range servers {
			tmp = append(tmp, raft.Server{
				ID:      raft.ServerID(k),
				Address: raft.ServerAddress(v),
			})
		}
		r.BootstrapCluster(raft.Configuration{
			Servers: tmp})
	} else {
		// Cluster consists of unjoined leaders. Picking a leader and
		// creating a real cluster is done manually after startup.
		r.BootstrapCluster(raft.Configuration{
			Servers: []raft.Server{
				{
					ID:      raft.ServerID(nodeId),
					Address: transport.LocalAddr(),
				},
			},
		})
	}

	return r, nil
}

type NsqCluster struct {
	id          string
	r           *raft.Raft
	raftAddress string
	servers     map[string]string
	dataDir     string
}

func NewNsqCluster(id, datadir, addr string, servers []string) (*NsqCluster, error) {
	if addr == "" {
		return nil, errors.New("addr must be provided")
	}
	cli := &NsqCluster{
		id:          id,
		dataDir:     datadir,
		raftAddress: addr,
	}
	if len(servers) > 0 {
		for _, v := range servers {
			tmplist := strings.Split(v, ",")
			if cli.servers == nil {
				cli.servers = make(map[string]string)
			}
			if len(tmplist) != 2 || tmplist[0] == "" || tmplist[1] == "" {
				return nil, errors.New("each servers must have the format nodename,nodeaddr")
			}
			if _, ok := cli.servers[tmplist[0]]; ok {
				return nil, fmt.Errorf("servers eliment(%s) repeated", v)
			}
			cli.servers[tmplist[0]] = cli.servers[tmplist[1]]
		}
	}
	r, err := setupRaft(cli.dataDir, cli.id, cli.raftAddress, cli.servers)
	if err != nil || r == nil {
		return nil, fmt.Errorf("setupRaft failed:%v", err)
	}
	cli.r = r
	return cli, nil
}

func (c *NsqCluster) Join(nodeid, addr string) error {
	if nodeid == "" || addr == "" {
		log.Printf("[E]NsqCluster Join(%s, %s) failed:invalid arg\n", nodeid, addr)
		return errors.New("invalid arg")
	}
	if nodeid == c.id {
		log.Printf("[W]NsqCluster Join(%s, %s) failed:id is current node id\n", nodeid, addr)
		return nil
	}

	if addr == c.raftAddress {
		log.Printf("[W]NsqCluster Join(%s, %s) failed:addr is current node addr\n", nodeid, addr)
		return nil
	}
	_, leaderID := c.r.LeaderWithID()

	// log.Printf("[D]-----------raft state=%s\n", c.r.State().String())
	if c.r.State() != raft.Leader {
		if leaderID != "" {
			return nil
		}
		log.Printf("[E]NsqCluster Join(%s, %s) failed:Not the leader\n", nodeid, addr)
		return errors.New("not the leader")
	}
	stats := c.r.Stats()
	if _, ok := stats["latest_configuration"]; !ok || stats["latest_configuration"] == "" {
		log.Printf("[E]NsqCluster Join(%s, %s) failed:raft Stats function return invalid format\n", nodeid, addr)
		return errors.New("raft Stats function return invalid format")
	}
	if !strings.HasPrefix(stats["latest_configuration"], "[") || strings.HasPrefix(stats["latest_configuration"], "]") {
		log.Printf("[E]NsqCluster Join(%s, %s) failed:raft Stats function return latest_configuration(%s) field is invalid\n", nodeid, addr, stats["latest_configuration"])
		return errors.New("raft Stats function return latest_configuration field is invalid")
	}

	idAddressReg := regexp.MustCompile(`ID:[\S]+ Address:[\S]+\}`)
	if idAddressReg == nil {
		log.Printf("[E]NsqCluster Join(%s, %s) failed:regexp.MustCompile(`ID:[\\S]+ Address:[\\S]+\\}`) failed\n", nodeid, addr)
		return errors.New("regexp.MustCompile(`ID:[\\S]+ Address:[\\S]+\\}`) failed")
	}
	idAddresses := idAddressReg.FindAllString(stats["latest_configuration"], -1)
	if len(idAddresses) == 0 {
		log.Printf("[E]NsqCluster Join(%s, %s) failed:idAddressReg.FindAllString=%vis not invalid\n", nodeid, addr, idAddresses)
		return fmt.Errorf("idAddressReg.FindAllString=%v is not invalid", idAddresses)
	}

	for _, v := range idAddresses {
		tmplist := strings.Split(v, " ")
		tmpid := strings.TrimPrefix(tmplist[0], "ID:")
		tmpaddr := strings.TrimPrefix(tmplist[1], "Address:")
		if tmpid == "" || tmpaddr == "" {
			log.Printf("[E]NsqCluster Join(%s, %s) failed:raft Stats function return latest_configuration(%s) field is invalid, not id or address\n", nodeid, addr, stats["latest_configuration"])
			return errors.New("raft Stats function return latest_configuration field is invalid, not id or address")
		}
		if tmpid != nodeid {
			err := c.r.AddVoter(raft.ServerID(nodeid), raft.ServerAddress(addr), 0, 0).Error()
			if err != nil {
				log.Printf("[E]NsqCluster Join(%s, %s) failed:Failed to add follower because of %s\n", nodeid, addr, err)
				return fmt.Errorf("failed to add follower because of %s", err)
			}
		}
	}

	return nil
}

func (n *NSQD) clusterLoop() {
	var joinServerMap sync.Map
	// for announcements, lookupd determines the host automatically
	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			joinServerMap.Range(func(k, v interface{}) bool {
				if n.cluster.Join(k.(string), v.(string)) == nil {
					joinServerMap.Delete(k)
				}
				return true
			})

		case val := <-n.joinNotifyChan:
			joinServerMap.Store(val.NodeID, val.Addr)
		case <-n.exitChan:
			goto exit
		}
	}

exit:
	n.logf(LOG_INFO, "LOOKUP: closing")
}
