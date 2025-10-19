package producer

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"math/rand"
	"os"
	"time"

	"github.com/imishinist/producer/model"
)

var (
	PgUser     string
	PgPassword string
	PgDatabase string
	PgHost     string
	PgPort     = 5432
)

func init() {
	PgUser = os.Getenv("PGUSER")
	PgPassword = os.Getenv("PGPASSWORD")
	PgDatabase = os.Getenv("PGDATABASE")
	PgHost = os.Getenv("PGHOST")
	if port := os.Getenv("PGPORT"); port != "" {
		fmt.Sscanf(port, "%d", &PgPort)
	}
}

func Connect() (*sql.DB, error) {
	connStr := fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s sslmode=disable",
		PgHost, PgPort, PgUser, PgPassword, PgDatabase)

	db, err := sql.Open("postgres", connStr)
	if err != nil {
		return nil, fmt.Errorf("error opening database connection: %w", err)
	}
	return db, nil
}

func AddMember(ctx context.Context, db *sql.DB, m *model.MemberWithFeed) error {
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("error starting transaction: %w", err)
	}

	var (
		id        int64
		createdAt time.Time
	)
	query := `INSERT INTO members (name, created_at) VALUES ($1, now()) RETURNING id, created_at`
	if err := tx.QueryRowContext(ctx, query, m.Member.Name).Scan(&id, &createdAt); err != nil {
		if rerr := tx.Rollback(); rerr != nil {
			slog.Error("failed to rollback transaction", "error", rerr)
		}
		return fmt.Errorf("error inserting member: %w", err)
	}
	m.Member.ID = id
	m.Member.CreatedAt = createdAt

	feed, err := upsertFeed(ctx, tx, id)
	if err != nil {
		if rerr := tx.Rollback(); rerr != nil {
			slog.Error("failed to rollback transaction", "error", rerr)
		}
		return fmt.Errorf("error upserting member feed: %w", err)
	}
	m.Feed = feed

	RandSleep(rand.Float64())

	err = tx.Commit()
	if err != nil {
		if rerr := tx.Rollback(); rerr != nil {
			slog.Error("failed to rollback transaction", "error", rerr)
		}
		return fmt.Errorf("error commit: %w", err)
	}
	return nil
}

func UpdateMember(ctx context.Context, db *sql.DB, m *model.MemberWithFeed) error {
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}

	member := m.Member
	_, err = tx.ExecContext(ctx, "UPDATE members SET name=$1 WHERE id=$2", member.Name, member.ID)
	if err != nil {
		if rerr := tx.Rollback(); rerr != nil {
			slog.Error("failed to rollback transaction", "error", rerr)
		}
		return fmt.Errorf("failed to update member: %w", err)
	}

	feed, err := upsertFeed(ctx, tx, member.ID)
	if err != nil {
		if rerr := tx.Rollback(); rerr != nil {
			slog.Error("failed to rollback transaction", "error", rerr)
		}
		return fmt.Errorf("error upserting member feed: %w", err)
	}
	m.Feed = feed

	RandSleep(rand.Float64())

	err = tx.Commit()
	if err != nil {
		if rerr := tx.Rollback(); rerr != nil {
			slog.Error("failed to rollback transaction", "error", rerr)
		}
		return fmt.Errorf("error commit: %w", err)
	}
	return nil
}

func upsertFeed(ctx context.Context, tx *sql.Tx, id int64) (*model.MemberFeed, error) {
	var (
		memberID  int64
		lastTxID  int64
		lastLSN   string
		updatedAt time.Time
	)
	if err := tx.QueryRowContext(ctx, `
INSERT INTO member_feed (member_id, last_txid, last_lsn, updated_at) VALUES ($1, pg_current_xact_id(), pg_current_wal_lsn(), now()) 
ON CONFLICT (member_id) DO UPDATE 
SET last_txid = GREATEST(member_feed.last_txid, EXCLUDED.last_txid),
    last_lsn = GREATEST(member_feed.last_lsn, EXCLUDED.last_lsn),
    updated_at = GREATEST(member_feed.updated_at, EXCLUDED.updated_at)
RETURNING member_feed.*`, id).Scan(&memberID, &lastTxID, &lastLSN, &updatedAt); err != nil {
		return nil, fmt.Errorf("error upsert member feed: %w", err)
	}
	return &model.MemberFeed{
		MemberID:  memberID,
		LastTxID:  lastTxID,
		LastLSN:   lastLSN,
		UpdatedAt: updatedAt,
	}, nil
}
