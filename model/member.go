package model

import (
	"fmt"
	"time"
)

type Member struct {
	ID        int64
	Name      string
	CreatedAt time.Time
}

func (m Member) String() string {
	return fmt.Sprintf("%d~%q (%s)", m.ID, m.Name, m.CreatedAt.Format(time.RFC3339))
}

type MemberFeed struct {
	MemberID  int64
	LastTxID  int64
	LastLSN   string
	UpdatedAt time.Time
}

func (m MemberFeed) String() string {
	return fmt.Sprintf("%d @ [%d, %s] (%s)", m.MemberID, m.LastTxID, m.LastLSN, m.UpdatedAt.Format(time.RFC3339))
}

type MemberWithFeed struct {
	Member *Member
	Feed   *MemberFeed
}

func (m MemberWithFeed) String() string {
	if m.Feed != nil {
		return fmt.Sprintf("%s ^ %s", m.Member.String(), m.Feed.String())
	}
	return fmt.Sprintf("%s", m.Member.String())
}
