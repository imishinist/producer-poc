package main

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"math/rand"

	_ "github.com/lib/pq"

	"github.com/imishinist/producer"
	"github.com/imishinist/producer/model"
)

func randString(n int) string {
	const letters = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ01234567890"
	b := make([]byte, n)
	for i := range b {
		b[i] = letters[rand.Intn(len(letters))]
	}
	return string(b)
}

func fetchMembers(ctx context.Context, db *sql.DB) ([]*model.MemberWithFeed, error) {
	rows, err := db.QueryContext(ctx, "SELECT id, name, created_at, member_id, last_txid, last_lsn, updated_at FROM members LEFT JOIN member_feed ON members.id = member_feed.member_id")
	if err != nil {
		log.Fatalf("Error querying members: %v", err)
	}
	defer rows.Close()

	members := make([]*model.MemberWithFeed, 0)
	for rows.Next() {
		var ret model.MemberWithFeed
		var member model.Member
		var (
			memberID  sql.NullInt64
			lastTxID  sql.NullInt64
			lastLSN   sql.NullString
			updatedAt sql.NullTime
		)
		if err := rows.Scan(&member.ID, &member.Name, &member.CreatedAt, &memberID, &lastTxID, &lastLSN, &updatedAt); err != nil {
			log.Fatalf("Error scanning row: %v", err)
		}
		ret.Member = &member
		if memberID.Valid && lastTxID.Valid && lastLSN.Valid && updatedAt.Valid {
			ret.Feed = &model.MemberFeed{
				MemberID:  memberID.Int64,
				LastTxID:  lastTxID.Int64,
				LastLSN:   lastLSN.String,
				UpdatedAt: updatedAt.Time,
			}
		}

		members = append(members, &ret)
	}
	return members, nil
}

func fetchMemberFeeds(ctx context.Context, db *sql.DB) ([]*model.MemberFeed, error) {
	rows, err := db.QueryContext(ctx, "SELECT member_id, last_txid, last_lsn, updated_at FROM member_feed")
	if err != nil {
		log.Fatalf("Error querying members: %v", err)
	}
	defer rows.Close()

	feeds := make([]*model.MemberFeed, 0)
	for rows.Next() {
		var feed model.MemberFeed
		if err := rows.Scan(&feed.MemberID, &feed.LastTxID, &feed.LastLSN, &feed.UpdatedAt); err != nil {
			log.Fatalf("Error scanning row: %v", err)
		}
		feeds = append(feeds, &feed)
	}
	return feeds, nil
}

func main() {
	db, err := producer.Connect()
	if err != nil {
		log.Fatalf("Error connecting to producer: %v", err)
	}
	defer db.Close()

	ctx := context.Background()

	fmt.Println("------------------------------------- before -----------------------------------------")
	members, err := fetchMembers(ctx, db)
	if err != nil {
		log.Fatalf("Error fetching member feeds: %v", err)
	}
	for _, member := range members {
		fmt.Println(member)
	}
	fmt.Println("--------------------------------------------------------------------------------------")
	fmt.Println()

	m := &model.MemberWithFeed{
		Member: &model.Member{ID: 1, Name: randString(16)},
	}
	if err := producer.UpdateMember(ctx, db, m); err != nil {
		log.Fatalf("Error modifying member: %v", err)
	}

	fmt.Println("------------------------------------- after ------------------------------------------")
	members, err = fetchMembers(ctx, db)
	if err != nil {
		log.Fatalf("Error fetching member feeds: %v", err)
	}
	for _, member := range members {
		fmt.Println(member)
	}
	fmt.Println("--------------------------------------------------------------------------------------")
	fmt.Println()
}
