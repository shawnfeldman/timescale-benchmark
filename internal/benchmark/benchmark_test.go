package benchmark

import (
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/shawnfeldman/timescale-benchmark/internal/db"
	log "github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
)

func TestBenchmarkOutput(t *testing.T) {
	b := Benchmark{statsReader: &MockDB{}}
	stats, err := b.Run("ok.csv", 10, 10)
	assert.Nil(t, err)
	assert.Equal(t, int64(0), stats.TotalTime)
}

func TestErrorOutput(t *testing.T) {
	b := Benchmark{statsReader: &MockErrDB{}}
	_, err := b.Run("ok.csv", 10, 10)
	assert.NotNil(t, err)
	assert.Equal(t, "Failed during workers: Failed to get to db", err.Error())
}

func TestSetMedian(t *testing.T) {
	stats := []db.Stat{db.Stat{ExecutionTime: 1 * time.Millisecond}, db.Stat{ExecutionTime: 3 * time.Millisecond}, db.Stat{ExecutionTime: 4 * time.Millisecond}}
	median := GetMedian(stats)
	assert.Equal(t, float64(3), median)
}
func TestSetMedianWithEven(t *testing.T) {
	stats := []db.Stat{db.Stat{ExecutionTime: 1 * time.Millisecond}, db.Stat{ExecutionTime: 2 * time.Millisecond}, db.Stat{ExecutionTime: 3 * time.Millisecond}, db.Stat{ExecutionTime: 4 * time.Millisecond}}
	median := GetMedian(stats)
	assert.Equal(t, 2.5, median)
}
func TestSimpleEndToEnd(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}
	log.SetOutput(os.Stdout)
	log.SetFormatter(&log.JSONFormatter{PrettyPrint: true})
	db := &db.DB{}
	err := db.Open("localhost", 5432, "homework", "postgres", "")
	b := Benchmark{statsReader: db}
	stats, err := b.Run("ok.csv", 10, 10)
	assert.Nil(t, err)
	log.WithField("stats", stats).Info()
	assert.True(t, stats.MeanQueryTime > 0)
}

func TestManyIntegrations(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}
	log.SetOutput(os.Stdout)
	log.SetFormatter(&log.JSONFormatter{PrettyPrint: true})
	db := &db.DB{}
	db.Open("localhost", 5432, "homework", "postgres", "")
	trailingCount := 200
	for i := 0; i < 10; i++ {
		b := Benchmark{statsReader: db}
		stats, err := b.Run("ok.csv", 10, 10)
		assert.Nil(t, err)
		assert.True(t, stats.MeanQueryTime > 0)
		assert.Equal(t, trailingCount, stats.Count)
	}
}

// DB Database representation
type MockDB struct {
}

// Run it
func (d *MockDB) Run(host string, start time.Time, end time.Time) (db.Stat, error) {
	return db.Stat{Host: "test", ExecutionTime: 1, Start: time.Now(), End: time.Now(), UsageStats: []db.UsageStats{db.UsageStats{Bucket: time.Now(), Max: 10.0, Min: 1.0}}}, nil
}

// DB Database err representation
type MockErrDB struct {
}

// Run it
func (d *MockErrDB) Run(host string, start time.Time, end time.Time) (db.Stat, error) {
	return db.Stat{Host: "test", ExecutionTime: 1}, fmt.Errorf("Failed to get to db")
}
