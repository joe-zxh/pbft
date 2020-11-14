package data

import (
	"crypto/sha512"
	"encoding/binary"
	"encoding/hex"
	"fmt"
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
	Mut        sync.Mutex
	PP         *PrePrepareArgs
	P          []*PrepareArgs
	SendCommit bool
	C          []*CommitArgs
	Committed  bool
	Digest     *EntryHash
}

func (e *Entry) String() string {
	return fmt.Sprintf("Entry{View: %d, Seq: %d, Committed: %v}",
		e.PP.View, e.PP.Seq, e.Committed)
}

// Hash returns a hash digest of the block.
func (e *Entry) Hash() EntryHash {
	// return cached hash if available
	if e.Digest != nil {
		return *e.Digest
	}

	s512 := sha512.New()

	byte4 := make([]byte, 4)
	binary.LittleEndian.PutUint32(byte4, uint32(e.PP.View))
	s512.Write(byte4[:])

	binary.LittleEndian.PutUint32(byte4, uint32(e.PP.Seq))
	s512.Write(byte4[:])

	for _, cmd := range e.PP.Commands {
		s512.Write([]byte(cmd))
	}

	e.Digest = new(EntryHash)
	sum := s512.Sum(nil)
	copy(e.Digest[:], sum)

	return *e.Digest
}
