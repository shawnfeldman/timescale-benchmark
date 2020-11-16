package benchmark_test

import (
	"testing"
	"time"

	"github.com/shawnfeldman/timescale-benchmark/internal/benchmark"
	"github.com/shawnfeldman/timescale-benchmark/internal/db"

	"github.com/stretchr/testify/assert"
)

func TestIntegration(t *testing.T) {
	b := benchmark.Benchmark{StatsReader: &MockDB{}}
	stats := b.Run("../../db/query_params.csv", 10, 10)
	assert.True(t, stats.TotalTime > 2000)
}

// DB Database representation
type MockDB struct {
}

// Run it
func (d *MockDB) Run(host string, start time.Time, end time.Time) (db.Stat, error) {
	return db.Stat{Average: 100}, nil
}
