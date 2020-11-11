package data

import (
	"crypto/sha512"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"github.com/joe-zxh/pbft/internal/proto"
	"sync"
)

// Command is the client data that is processed by HotStuff
type Command string

// BlockHash represents a SHA256 hashsum of a Block
type EntryHash [64]byte

func (d EntryHash) String() string {
	return hex.EncodeToString(d[:])
}

func (d EntryHash) ToSlice() []byte {
	return d[:]
}

type EntryID struct {
	V uint32
	N uint32
}

type Entry struct {
	Lock sync.Mutex

	// preprepareArgs
	View     uint32
	Seq      uint32
	Commands []Command

	P          []*proto.PrepareArgs
	SendCommit bool
	C          []*proto.CommitArgs
	Committed  bool
	SendReply  bool
	hash       *EntryHash
}

func (e Entry) String() string {
	return fmt.Sprintf("Entry{View: %d, Seq: %d, sendReply: %v}",
		e.View, e.Seq, e.SendReply)
}

// Hash returns a hash digest of the block.
func (e Entry) Hash() EntryHash {
	// return cached hash if available
	if e.hash != nil {
		return *e.hash
	}

	s512 := sha512.New()

	byte4 := make([]byte, 4)
	binary.LittleEndian.PutUint32(byte4, uint32(e.View))
	s512.Write(byte4[:])

	binary.LittleEndian.PutUint32(byte4, uint32(e.Seq))
	s512.Write(byte4[:])

	for _, cmd := range e.Commands {
		s512.Write([]byte(cmd))
	}

	e.hash = new(EntryHash)
	sum := s512.Sum(nil)
	copy(e.hash[:], sum)

	return *e.hash
}
