package db

import (
	"time"
)

// Stat representation
type Stat struct {
	Average int
}

// StatsReader interface
type StatsReader interface {
	Run(host string, start time.Time, end time.Time) (Stat, error)
}

// DB Database representation
type DB struct {
}

// Run it
func (db *DB) Run(host string, start time.Time, end time.Time) (Stat, error) {
	// TODO: db logic
	return Stat{Average: 1}, nil
}
