// proto的类型是用于rpc时的传输，它的字段会更精简；而普通数据结构是存在 本地的，它会记录更多的字段。这个文件是用于转换的。
package proto

import (
	"github.com/joe-zxh/pbft/data"
)

func EntryToPPProto(e *data.Entry) *PrePrepareArgs {
	commands := make([]*Command, 0, len(e.Commands))
	for _, cmd := range e.Commands {
		commands = append(commands, CommandToProto(cmd))
	}
	return &PrePrepareArgs{
		View:     e.View,
		Seq:      e.Seq,
		Commands: commands,
	}
}

func (pp *PrePrepareArgs) PPProto2Entry() *data.Entry {
	commands := make([]data.Command, 0, len(pp.GetCommands()))
	for _, cmd := range pp.GetCommands() {
		commands = append(commands, cmd.FromProto())
	}
	e := &data.Entry{
		View:     pp.View,
		Seq:      pp.Seq,
		Commands: commands,
	}
	e.Hash()
	return e
}

func CommandToProto(cmd data.Command) *Command {
	return &Command{Data: []byte(cmd)}
}

func (cmd *Command) FromProto() data.Command {
	return data.Command(cmd.GetData())
}
