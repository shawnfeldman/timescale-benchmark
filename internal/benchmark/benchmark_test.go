package benchmark_test

import (
	"fmt"
	"testing"
	"time"

	"github.com/shawnfeldman/timescale-benchmark/internal/benchmark"
	"github.com/shawnfeldman/timescale-benchmark/internal/db"

	"github.com/stretchr/testify/assert"
)

func TestBenchmarkOutput(t *testing.T) {
	b := benchmark.Benchmark{StatsReader: &MockDB{}}
	stats, err := b.Run("ok.csv", 10, 10)
	assert.Nil(t, err)
	assert.Equal(t, int64(0), stats.TotalTime)
}

func TestErrorIntegration(t *testing.T) {
	b := benchmark.Benchmark{StatsReader: &MockErrDB{}}
	_, err := b.Run("ok.csv", 10, 10)
	assert.NotNil(t, err)
	assert.Equal(t, "Failed during workers: Failed to get to db", err.Error())
}

// DB Database representation
type MockDB struct {
}

// Run it
func (d *MockDB) Run(host string, start time.Time, end time.Time) (db.Stat, error) {
	return db.Stat{Host: "test", ExecutionTime: 1}, nil
}

// DB Database err representation
type MockErrDB struct {
}

// Run it
func (d *MockErrDB) Run(host string, start time.Time, end time.Time) (db.Stat, error) {
	return db.Stat{Host: "test", ExecutionTime: 1}, fmt.Errorf("Failed to get to db")
}
