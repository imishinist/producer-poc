package producer

import (
	"context"
	"database/sql"
	"log"
	"time"

	"github.com/imishinist/producer/model"
)

type FetchNewerCondition struct {
	LastMemberID  int64
	LastCommitted sql.NullTime
	Limit         int
}

func FetchNewerMember(ctx context.Context, db *sql.DB, cond FetchNewerCondition) ([]*model.MemberWithFeed, error) {
	lastTs := time.Time{}
	if cond.LastCommitted.Valid {
		lastTs = cond.LastCommitted.Time
	}

	query := `
WITH feeds AS (
	SELECT 
	    member_id,
	    last_txid,
	    pg_xact_commit_timestamp(last_txid::xid) AS last_committed,
	    last_lsn,
	    updated_at
	FROM member_feed
)
SELECT members.id, members.name, members.created_at, feeds.member_id, feeds.last_txid, feeds.last_committed, feeds.last_lsn, feeds.updated_at FROM feeds
INNER JOIN members ON member_id = members.id
WHERE last_committed IS NOT NULL
	AND (last_committed > $1 OR (last_committed = $1 AND id > $2))
	ORDER  BY last_committed, id
	LIMIT  $3
`

	rows, err := db.QueryContext(ctx, query, lastTs, cond.LastMemberID, cond.Limit)
	if err != nil {
		log.Fatalf("Error querying members: %v", err)
	}
	defer rows.Close()

	members := make([]*model.MemberWithFeed, 0)
	for rows.Next() {
		var member model.Member
		var feed model.MemberFeed
		if err := rows.Scan(&member.ID, &member.Name, &member.CreatedAt, &feed.MemberID, &feed.LastTxID, &feed.LastCommitted, &feed.LastLSN, &feed.UpdatedAt); err != nil {
			log.Fatalf("Error scanning row: %v", err)
		}

		members = append(members, &model.MemberWithFeed{Member: &member, Feed: &feed})
	}
	return members, nil
}
