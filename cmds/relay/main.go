package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"time"

	_ "github.com/lib/pq"

	"github.com/imishinist/producer"
)

var InitState = State{
	MemberID:      0,
	LastCommitted: time.Time{},
	LastLSN:       "0/0",
}

type State struct {
	MemberID      int64     `json:"member_id"`
	LastCommitted time.Time `json:"committed"`
	LastLSN       string    `json:"lsn"`
}

func (s State) String() string {
	return fmt.Sprintf("%d @ [%s, %s]", s.MemberID, s.LastCommitted.Format(time.RFC3339Nano), s.LastLSN)
}

func SaveState(file string, s *State) error {
	f, err := os.OpenFile(file, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0600)
	if err != nil {
		return fmt.Errorf("failed to open file: %w", err)
	}
	defer f.Close()

	if err := json.NewEncoder(f).Encode(s); err != nil {
		return fmt.Errorf("failed to encode state: %w", err)
	}
	return nil
}

func GetState(file string) (*State, error) {
	f, err := os.Open(file)
	if err != nil {
		return nil, fmt.Errorf("failed to open file: %w", err)
	}
	defer f.Close()

	var s State
	if err := json.NewDecoder(f).Decode(&s); err != nil {
		return nil, fmt.Errorf("failed to decode state: %w", err)
	}
	return &s, nil
}

func main() {
	var (
		loop     = false
		count    = 10
		interval = 5 * time.Second
	)
	flag.IntVar(&count, "c", count, "number of loop count")
	flag.BoolVar(&loop, "l", loop, "loop count")
	flag.DurationVar(&interval, "i", interval, "interval")
	flag.Parse()

	db, err := producer.Connect()
	if err != nil {
		slog.Error("Error connecting to database", "error", err)
	}
	defer db.Close()

	state, err := GetState("data/state.json")
	if err != nil {
		slog.Warn("failed to load state", "error", err)
		state = &InitState
	}
	defer func() {
		if err := SaveState("data/state.json", state); err != nil {
			slog.Warn("failed to save state", "error", err)
		}
	}()

	slog.Info("initial state", "state", state)
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()

	ticker := time.NewTicker(interval)
	for loop || count > 0 {
		count -= 1

		members, err := producer.FetchNewerMember(ctx, db, producer.FetchNewerCondition{
			LastMemberID: state.MemberID,
			LastCommitted: sql.NullTime{
				Time:  state.LastCommitted,
				Valid: true,
			},
			Limit: 1000,
		})
		if err != nil {
			slog.Error("fetch newer members", "err", err)
			return
		}
		for _, member := range members {
			slog.Info("member", "member", member)
			if member.Feed.LastCommitted.Valid {
				state.LastCommitted = member.Feed.LastCommitted.Time
			}
			state.LastLSN = member.Feed.LastLSN
			state.MemberID = member.Feed.MemberID
		}

		if len(members) > 0 {
			slog.Info("updated state", "state", state)
		}

		select {
		case <-ticker.C:
		case <-ctx.Done():
			return
		}
	}
}
