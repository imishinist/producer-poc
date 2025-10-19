package main

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"math/rand"
	"os"

	_ "github.com/lib/pq"

	"github.com/imishinist/producer/model"
)

var (
	PG_USER     string
	PG_PASSWORD string
	PG_DATABASE string
	PG_HOST     string
	PG_PORT     = 5432
)

func init() {
	PG_USER = os.Getenv("PGUSER")
	PG_PASSWORD = os.Getenv("PGPASSWORD")
	PG_DATABASE = os.Getenv("PGDATABASE")
	PG_HOST = os.Getenv("PGHOST")
	if port := os.Getenv("PGPORT"); port != "" {
		fmt.Sscanf(port, "%d", &PG_PORT)
	}
}

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

func modifyMember(ctx context.Context, db *sql.DB, m *model.Member) error {
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}

	_, err = tx.ExecContext(ctx, "UPDATE members SET name=$1 WHERE id=$2", m.Name, m.ID)
	if err != nil {
		if rerr := tx.Rollback(); rerr != nil {
			log.Printf("Error rolling back transaction: %v", rerr)
		}

		return err
	}

	_, err = tx.ExecContext(ctx, `
INSERT INTO member_feed (member_id, last_txid, last_lsn, updated_at) VALUES ($1, pg_current_xact_id(), pg_current_wal_lsn(), now()) 
ON CONFLICT (member_id) DO UPDATE 
SET last_txid = GREATEST(member_feed.last_txid, EXCLUDED.last_txid),
    last_lsn = GREATEST(member_feed.last_lsn, EXCLUDED.last_lsn),
    updated_at = GREATEST(member_feed.updated_at, EXCLUDED.updated_at)`, m.ID)
	if err != nil {
		if rerr := tx.Rollback(); rerr != nil {
			log.Printf("Error rolling back transaction: %v", rerr)
		}
		return err
	}

	err = tx.Commit()
	if err != nil {
		return tx.Rollback()
	}
	return nil
}

func main() {
	connStr := fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s sslmode=disable",
		PG_HOST, PG_PORT, PG_USER, PG_PASSWORD, PG_DATABASE)

	db, err := sql.Open("postgres", connStr)
	if err != nil {
		log.Fatalf("Error opening database connection: %v", err)
	}
	defer db.Close()

	if err := db.Ping(); err != nil {
		log.Fatalf("Error connecting to the database: %v", err)
	}

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

	if err := modifyMember(ctx, db, &model.Member{ID: 1, Name: randString(16)}); err != nil {
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
