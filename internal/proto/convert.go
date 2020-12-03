// proto的类型是用于rpc时的传输，它的字段会更精简；而普通数据结构是存在 本地的，它会记录更多的字段。这个文件是用于转换的。
package proto

import (
	"github.com/joe-zxh/pbft/data"
	"github.com/joe-zxh/pbft/util"
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
		Sender: dp.Sender,
		View:   dp.View,
		Seq:    dp.Seq,
		Digest: dp.Digest.ToSlice(),
	}
}

func (p *PrepareArgs) Proto2P() *data.PrepareArgs {
	dpp := &data.PrepareArgs{
		Sender: p.Sender,
		View:   p.View,
		Seq:    p.Seq,
	}
	copy(dpp.Digest[:], p.Digest[:len(dpp.Digest)])
	return dpp
}

func C2Proto(dc *data.CommitArgs) *CommitArgs {
	return &CommitArgs{
		Sender: dc.Sender,
		View:   dc.View,
		Seq:    dc.Seq,
		Digest: dc.Digest.ToSlice(),
	}
}

func (c *CommitArgs) Proto2C() *data.CommitArgs {
	dc := &data.CommitArgs{
		Sender: c.Sender,
		View:   c.View,
		Seq:    c.Seq,
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

// view change...
func Pm2Proto(dPm *data.Pm) *Pm {

	pprepares := make([]*PrepareArgs, 0)
	for _, dp := range dPm.P {
		pprepares = append(pprepares, P2Proto(dp))
	}

	return &Pm{
		PP: PP2Proto(dPm.PP),
		P:  pprepares,
	}
}

func (pPm *Pm) Proto2Pm() *data.Pm {

	dprepares := make([]*data.PrepareArgs, 0)
	for _, pp := range pPm.P {
		dprepares = append(dprepares, pp.Proto2P())
	}

	return &data.Pm{
		PP: pPm.PP.Proto2PP(),
		P:  dprepares,
	}
}

func CPA2Proto(dCPA *data.CheckPointArgs) *CheckPointArgs {
	return &CheckPointArgs{
		Seq:    dCPA.Seq,
		Digest: []byte(dCPA.Digest),
		Rid:    dCPA.Rid,
	}
}

func (pCPA *CheckPointArgs) Proto2CPA() *data.CheckPointArgs {
	return &data.CheckPointArgs{
		Seq:    pCPA.Seq,
		Digest: string(pCPA.Digest),
		Rid:    pCPA.Rid,
	}
}

func CP2Proto(dCP *data.CheckPoint) *CheckPoint {

	pproof := make([]*CheckPointArgs, 0)
	for _, p := range dCP.Proof {
		pproof = append(pproof, CPA2Proto(p))
	}

	state, _ := util.GetBytes(dCP.State)

	return &CheckPoint{
		Seq:    dCP.Seq,
		Stable: dCP.Stable,
		State:  state,
		Proof:  pproof,
	}
}

func (pCP *CheckPoint) Proto2CP() *data.CheckPoint {

	dproof := make([]*data.CheckPointArgs, 0)
	for _, p := range pCP.Proof {
		dproof = append(dproof, p.Proto2CPA())
	}

	return &data.CheckPoint{
		Seq:    pCP.Seq,
		Stable: pCP.Stable,
		State:  pCP.State,
		Proof:  dproof,
	}
}

func VC2Proto(dVC *data.ViewChangeArgs) *ViewChangeArgs {

	dpms := make([]*Pm, 0)
	for _, p := range dVC.P {
		dpms = append(dpms, Pm2Proto(p))
	}

	return &ViewChangeArgs{
		View: dVC.View,
		CP:   CP2Proto(dVC.CP),
		P:    dpms,
		Rid:  dVC.Rid,
	}
}

func (pVC *ViewChangeArgs) Proto2VC() *data.ViewChangeArgs {

	ppms := make([]*data.Pm, 0)
	for _, p := range pVC.P {
		ppms = append(ppms, p.Proto2Pm())
	}

	return &data.ViewChangeArgs{
		View: pVC.View,
		CP:   pVC.CP.Proto2CP(),
		P:    ppms,
		Rid:  pVC.Rid,
	}
}

func NV2Proto(dNV *data.NewViewArgs) *NewViewArgs {

	pVs := make([]*ViewChangeArgs, 0)

	for _, dv := range dNV.V {
		pVs = append(pVs, VC2Proto(dv))
	}

	pPPs := make([]*PrePrepareArgs, 0)
	for _, dpp := range dNV.O {
		pPPs = append(pPPs, PP2Proto(dpp))
	}

	return &NewViewArgs{
		View: dNV.View,
		V:    pVs,
		O:    pPPs,
	}
}

func (pNV *NewViewArgs) Proto2NV() *data.NewViewArgs {

	dVs := make([]*data.ViewChangeArgs, 0)

	for _, pvc := range pNV.V {
		dVs = append(dVs, pvc.Proto2VC())
	}

	dPPs := make([]*data.PrePrepareArgs, 0)
	for _, ppp := range pNV.O {
		dPPs = append(dPPs, ppp.Proto2PP())
	}

	return &data.NewViewArgs{
		View: pNV.View,
		V:    dVs,
		O:    dPPs,
	}
}
