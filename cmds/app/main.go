package main

import (
	"context"
	"database/sql"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"math/rand"
	"os"
	"os/signal"
	"sync"
	"time"

	_ "github.com/lib/pq"

	"github.com/imishinist/producer"
	"github.com/imishinist/producer/model"
)

var (
	once sync.Once

	ErrEmpty = errors.New("data is empty")
)

type AppContext struct {
	data []*model.MemberWithFeed

	mu  *sync.Mutex
	mud []*sync.Mutex
}

func (a *AppContext) init() {
	once.Do(func() {
		a.data = make([]*model.MemberWithFeed, 0)
		a.mud = make([]*sync.Mutex, 0)
		a.mu = new(sync.Mutex)
	})
}

func (a *AppContext) Sample() (*model.MemberWithFeed, func(), error) {
	a.init()

	a.mu.Lock()
	length := len(a.data)
	if length == 0 {
		a.mu.Unlock()
		return nil, nil, ErrEmpty
	}
	a.mu.Unlock()

	for {
		idx := rand.Intn(length)
		if !a.mud[idx].TryLock() {
			continue
		}

		return a.data[idx], func() {
			a.mud[idx].Unlock()
		}, nil
	}
}

func (a *AppContext) Add(items ...*model.MemberWithFeed) {
	a.init()

	a.mu.Lock()
	defer a.mu.Unlock()
	for _, item := range items {
		a.data = append(a.data, item)
		a.mud = append(a.mud, new(sync.Mutex))
	}
}

type ScenarioRunner struct {
	ctx *AppContext
	db  *sql.DB

	AddRatio float64
}

func NewScenarioRunner(ratio float64) *ScenarioRunner {
	db, err := producer.Connect()
	if err != nil {
		slog.Error("failed to connect to db", "err", err)
		panic(err)
	}

	return &ScenarioRunner{
		ctx:      &AppContext{},
		db:       db,
		AddRatio: ratio,
	}
}

func (s *ScenarioRunner) Init(members ...*model.MemberWithFeed) {
	s.ctx.Add(members...)
}

func (s *ScenarioRunner) Run(ctx context.Context) error {
	ratio := rand.Float64()
	slog.Debug("ratio", "ratio", ratio)
	if ratio < s.AddRatio {
		return s.add(ctx)
	} else {
		return s.update(ctx)
	}
}

func (s *ScenarioRunner) add(ctx context.Context) error {
	m := &model.MemberWithFeed{
		Member: &model.Member{
			Name:      producer.RandomString(10),
			CreatedAt: time.Now(),
		},
	}

	if err := producer.AddMember(ctx, s.db, m); err != nil {
		return fmt.Errorf("update error: %w", err)
	}
	slog.Info("  added", "member", m)
	s.ctx.Add(m)

	return nil
}

func (s *ScenarioRunner) update(ctx context.Context) error {
	item, ret, err := s.ctx.Sample()
	if err != nil {
		slog.Debug("sampling error", "err", err)
		if errors.Is(err, ErrEmpty) {
			return nil
		}
		return fmt.Errorf("update error: %w", err)
	}
	defer ret()

	item.Member.Name = producer.RandomString(10)
	if err := producer.UpdateMember(ctx, s.db, item); err != nil {
		return fmt.Errorf("update error: %w", err)
	}

	slog.Info("updated", "member", item)
	return nil
}

func main() {
	var (
		interval time.Duration
	)
	flag.DurationVar(&interval, "i", 500*time.Millisecond, "interval")
	flag.Parse()

	runner := NewScenarioRunner(0.3)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()

	db, err := producer.Connect()
	if err != nil {
		slog.Error("Error connecting to database", "error", err)
	}
	defer db.Close()

	feeds, err := producer.FetchNewerMember(ctx, db, producer.FetchNewerCondition{
		LastMemberID:  0,
		LastCommitted: sql.NullTime{},
		Limit:         1000,
	})
	if err != nil {
		slog.Error("Error fetching feeds", "error", err)
		return
	}
	runner.Init(feeds...)

	ticker := time.NewTicker(interval)
	for {
		if err := runner.Run(ctx); err != nil {
			slog.Error("scenario run error", "error", err)
		}

		select {
		case <-ticker.C:
		case <-ctx.Done():
			return
		}
	}
}
