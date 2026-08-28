package server

import (
	"encoding/binary"
	"fmt"
)

// Commands are the payloads of Raft log entries: the replicated write
// operations every node applies to its storage engine in log order.
//
// Encoding: [1B op][4B klen][key][4B vlen][value] (value only for puts).

type OpType byte

const (
	OpPut    OpType = 1
	OpDelete OpType = 2
)

type Command struct {
	Op    OpType
	Key   []byte
	Value []byte
}

func EncodeCommand(c Command) []byte {
	size := 1 + 4 + len(c.Key)
	if c.Op == OpPut {
		size += 4 + len(c.Value)
	}
	buf := make([]byte, 0, size)
	buf = append(buf, byte(c.Op))
	buf = binary.BigEndian.AppendUint32(buf, uint32(len(c.Key)))
	buf = append(buf, c.Key...)
	if c.Op == OpPut {
		buf = binary.BigEndian.AppendUint32(buf, uint32(len(c.Value)))
		buf = append(buf, c.Value...)
	}
	return buf
}

// EncodeCommands concatenates several commands into one Raft entry payload.
// Batching concurrent client writes into a single entry means one log
// append + fsync + replication round covers the whole group.
func EncodeCommands(cmds []Command) []byte {
	var buf []byte
	for _, c := range cmds {
		buf = append(buf, EncodeCommand(c)...)
	}
	return buf
}

// DecodeCommands parses a (possibly batched) entry payload.
func DecodeCommands(data []byte) ([]Command, error) {
	var cmds []Command
	for len(data) > 0 {
		c, n, err := decodeOne(data)
		if err != nil {
			return nil, err
		}
		cmds = append(cmds, c)
		data = data[n:]
	}
	return cmds, nil
}

func DecodeCommand(data []byte) (Command, error) {
	c, _, err := decodeOne(data)
	return c, err
}

// decodeOne parses one command and reports how many bytes it consumed.
func decodeOne(data []byte) (Command, int, error) {
	var c Command
	if len(data) < 5 {
		return c, 0, fmt.Errorf("command too short: %d bytes", len(data))
	}
	c.Op = OpType(data[0])
	klen := binary.BigEndian.Uint32(data[1:5])
	consumed := 5
	rest := data[5:]
	if uint32(len(rest)) < klen {
		return c, 0, fmt.Errorf("command truncated in key")
	}
	c.Key = rest[:klen]
	rest = rest[klen:]
	consumed += int(klen)

	switch c.Op {
	case OpDelete:
		return c, consumed, nil
	case OpPut:
		if len(rest) < 4 {
			return c, 0, fmt.Errorf("command truncated before value length")
		}
		vlen := binary.BigEndian.Uint32(rest[:4])
		rest = rest[4:]
		consumed += 4
		if uint32(len(rest)) < vlen {
			return c, 0, fmt.Errorf("command truncated in value")
		}
		c.Value = rest[:vlen]
		consumed += int(vlen)
		return c, consumed, nil
	default:
		return c, 0, fmt.Errorf("unknown op type %d", c.Op)
	}
}
