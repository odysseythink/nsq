package protocol

import (
	"encoding/binary"
	"io"
	"net"
)

type Client interface {
	Close() error
}

// Protocol describes the basic behavior of any protocol in the system
type Protocol interface {
	NewClient(net.Conn) Client
	IOLoop(Client) error
}

// SendResponse is a server side utility function to prefix data with a length header
// and write to the supplied Writer
func SendResponse(w io.Writer, data []byte) (int, error) {
	err := binary.Write(w, binary.BigEndian, int32(len(data)))
	if err != nil {
		return 0, err
	}

	n, err := w.Write(data)
	if err != nil {
		return 0, err
	}

	return (n + 4), nil
}

// SendFramedResponse is a server side utility function to prefix data with a length header
// and frame header and write to the supplied Writer
func SendFramedResponse(w io.Writer, frameType int32, data []byte) (int, error) {
	beBuf := make([]byte, 4)
	size := uint32(len(data)) + 4

	binary.BigEndian.PutUint32(beBuf, size)
	n, err := w.Write(beBuf)
	if err != nil {
		return n, err
	}

	binary.BigEndian.PutUint32(beBuf, uint32(frameType))
	n, err = w.Write(beBuf)
	if err != nil {
		return n + 4, err
	}

	n, err = w.Write(data)
	return n + 8, err
}

type NsqdPeerInfo struct {
	NodeID      string   `json:"node_id"`
	RaftAddress string   `json:"raft_address"`
	Tombstones  []bool   `json:"tombstones"`
	Topics      []string `json:"topics"`
}

type PeerInfo struct {
	LastUpdate       int64
	ID               string
	RemoteAddress    string        `json:"remote_address"`
	Hostname         string        `json:"hostname"`
	BroadcastAddress string        `json:"broadcast_address"`
	TCPPort          int           `json:"tcp_port"`
	HTTPPort         int           `json:"http_port"`
	Version          string        `json:"version"`
	NsqdPeer         *NsqdPeerInfo `json:"nsqd_peer"`
}
