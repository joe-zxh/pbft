// proto的类型是用于rpc时的传输，它的字段会更精简；而普通数据结构是存在 本地的，它会记录更多的字段。这个文件是用于转换的。
package proto

import (
	"github.com/joe-zxh/pbft/data"
)

func PP2Proto(dpp *data.PrePrepareArgs) *PrePrepareArgs {
	commands := make([]*Command, 0, len(dpp.Commands))
	for _, cmd := range dpp.Commands {
		commands = append(commands, CommandToProto(cmd))
	}
	return &PrePrepareArgs{
		View:     dpp.View,
		Seq:      dpp.Seq,
		Commands: commands,
	}
}

func (pp *PrePrepareArgs) Proto2PP() *data.PrePrepareArgs {
	commands := make([]data.Command, 0, len(pp.GetCommands()))
	for _, cmd := range pp.GetCommands() {
		commands = append(commands, cmd.Proto2Command())
	}
	dpp := &data.PrePrepareArgs{
		View:     pp.View,
		Seq:      pp.Seq,
		Commands: commands,
	}
	return dpp
}

func P2Proto(dp *data.PrepareArgs) *PrepareArgs {
	return &PrepareArgs{
		View:   dp.View,
		Seq:    dp.Seq,
		Digest: dp.Digest.ToSlice(),
	}
}

func (p *PrepareArgs) Proto2P() *data.PrepareArgs {
	dpp := &data.PrepareArgs{
		View: p.View,
		Seq:  p.Seq,
	}
	copy(dpp.Digest[:], p.Digest[:len(dpp.Digest)])
	return dpp
}

func C2Proto(dc *data.CommitArgs) *CommitArgs {
	return &CommitArgs{
		View:   dc.View,
		Seq:    dc.Seq,
		Digest: dc.Digest.ToSlice(),
	}
}

func (c *CommitArgs) Proto2C() *data.CommitArgs {
	dc := &data.CommitArgs{
		View: c.View,
		Seq:  c.Seq,
	}
	copy(dc.Digest[:], c.Digest[:len(dc.Digest)])
	return dc
}

func CommandToProto(cmd data.Command) *Command {
	return &Command{Data: []byte(cmd)}
}

func (cmd *Command) Proto2Command() data.Command {
	return data.Command(cmd.GetData())
}
