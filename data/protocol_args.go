package data

type PrePrepareArgs struct {
	View     uint32
	Seq      uint32
	Commands []Command
	Sender   uint32
}

type PrepareArgs struct {
	View   uint32
	Seq    uint32
	Digest EntryHash
	Sender uint32
}

type CommitArgs struct {
	View   uint32
	Seq    uint32
	Digest EntryHash
	Sender uint32
}
