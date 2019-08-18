package client

import (
    "fmt"
    "github.com/plan-systems/plan-core/plan"
)

// SeatDesc returns a string containing useful info about this MemberSeat
func (seat *MemberSeat) SeatDesc() string {
    commID := plan.BinEncode(seat.GenesisSeed.CommunityEpoch.CommunityID)
    return fmt.Sprintf("seat:%s, member:%v(%d), community:%v", 
        plan.BinEncode(seat.SeatID), seat.MemberEpoch.Alias, seat.MemberEpoch.MemberID, commID)
}
