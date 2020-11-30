package consensus

import (
	"github.com/joe-zxh/pbft/data"
)

// Should lock s.lock before call this function
func (pbft *PBFTCore) getCheckPoint(seq uint32) *data.CheckPoint {
	_, ok := pbft.CPs[seq]
	if !ok {
		for k, v := range pbft.CPs {
			if k > seq && v.Stable {
				return nil
			}
		}

		pbft.CPs[seq] = &data.CheckPoint{
			Seq:    uint32(seq),
			Stable: false,
			State:  nil,
			Proof:  make([]*data.CheckPointArgs, 0),
		}
	}

	if seq == 0 { // 为了view change的实验，第一个checkpoint，我们给他构造2f+1proof的数据。
		var proof data.CheckPointArgs
		for i := 0; i < 2*int(pbft.F)+1; i++ {
			pbft.CPs[seq].Proof = append(pbft.CPs[seq].Proof, &proof)
		}
	}

	return pbft.CPs[seq]
}

func (s *PBFTCore) GetStableCheckPoint() *data.CheckPoint {
	for _, v := range s.CPs {
		if v.Stable {
			return v
		}
	}
	panic("No stable checkpoint")
}

// Should lock s.lock before call this function
func (s *PBFTCore) GenerateViewChange() *data.ViewChangeArgs {
	s.Changing = true
	cp := s.GetStableCheckPoint()

	vcArgs := data.ViewChangeArgs{
		View: s.View + 1,
		Rid:  s.ID,
		CP:   cp,
	}

	log := s.Log

	for k, v := range log {
		if k.N > cp.Seq && v.PP != nil && (v.SendCommit || s.Prepared(v)) {
			pm := data.Pm{
				PP: v.PP,
				P:  v.P,
			}
			vcArgs.P = append(vcArgs.P, &pm)
		}
	}

	return &vcArgs
}
