package model

import (
	"database/sql"
	"fmt"
	"time"
)

type Member struct {
	ID        int64
	Name      string
	CreatedAt time.Time
}

func (m Member) String() string {
	return fmt.Sprintf("%05d~%q (%s)", m.ID, m.Name, m.CreatedAt.Format(time.RFC3339Nano))
}

type MemberFeed struct {
	MemberID int64

	LastTxID      int64
	LastCommitted sql.NullTime

	LastLSN   string
	UpdatedAt time.Time
}

func (m MemberFeed) String() string {
	if m.LastCommitted.Valid {
		return fmt.Sprintf("%05d @ [%d, %s, %s] (%s)", m.MemberID, m.LastTxID, m.LastCommitted.Time.Format(time.RFC3339Nano), m.LastLSN, m.UpdatedAt.Format(time.RFC3339Nano))
	}
	return fmt.Sprintf("%05d @ [%d, %s] (%s)", m.MemberID, m.LastTxID, m.LastLSN, m.UpdatedAt.Format(time.RFC3339Nano))
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
