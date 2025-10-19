package main

import (
	"context"
	"database/sql"
	"flag"
	"log/slog"
	"os"
	"os/signal"
	"time"

	_ "github.com/lib/pq"

	"github.com/imishinist/producer"
)

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

	state, err := producer.GetState("data/state.json")
	if err != nil {
		slog.Warn("failed to load state", "error", err)
		state = &producer.InitState
	}
	defer func() {
		if err := producer.SaveState("data/state.json", state); err != nil {
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
			slog.Info("fetch members", "count", len(members))
			slog.Info("updated state", "state", state)
		}

		select {
		case <-ticker.C:
		case <-ctx.Done():
			return
		}
	}
}
