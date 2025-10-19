package producer

import (
	"encoding/json"
	"fmt"
	"os"
	"time"
)

var InitState = State{
	MemberID:      0,
	LastCommitted: time.Time{},
	LastLSN:       "0/0",
}

type State struct {
	LastCommitted time.Time `json:"committed"`
	LastLSN       string    `json:"lsn"`
	MemberID      int64     `json:"member_id"`
}

func (s State) String() string {
	return fmt.Sprintf("[%s, %s] @ %d", s.LastCommitted.Format(time.RFC3339Nano), s.LastLSN, s.MemberID)
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
