package workers_test

import (
	"fmt"
	"testing"
	"time"

	"github.com/shawnfeldman/timescale-benchmark/internal/db"
	"github.com/shawnfeldman/timescale-benchmark/internal/input"
	"github.com/shawnfeldman/timescale-benchmark/internal/workers"

	"github.com/stretchr/testify/assert"
)

func TestSingleInsert(t *testing.T) {
	w := workers.WorkerProcessor{Workers: 10, StatsReader: &MockDB{}}

	streamer := make(chan input.QueryParams)
	statsChan, errChan := w.Process(streamer)
	streamer <- input.QueryParams{Host: "a", Start: time.Now(), End: time.Now()}
	stat := <-statsChan
	assert.Equal(t, time.Duration(1), stat.ExecutionTime)
	close(streamer)
	err := <-errChan
	assert.Nil(t, err)
}

func TestSingleError(t *testing.T) {
	w := workers.WorkerProcessor{Workers: 10, StatsReader: &MockErrorDB{}}

	streamer := make(chan input.QueryParams)
	statsChan, errChan := w.Process(streamer)
	streamer <- input.QueryParams{Host: "a", Start: time.Now(), End: time.Now()}
	stat := <-statsChan
	assert.Equal(t, time.Duration(0), stat.ExecutionTime)
	err := <-errChan
	assert.NotNil(t, err)
	close(streamer)

}
func Test10Inserts(t *testing.T) {
	w := workers.WorkerProcessor{Workers: 10, StatsReader: &MockDB{}}
	streamer := make(chan input.QueryParams)

	go func() {
		defer close(streamer)
		for i := 0; i < 100; i++ {
			streamer <- input.QueryParams{Host: fmt.Sprintf("a%d", i), Start: time.Now(), End: time.Now()}
		}
	}()
	statsChan, errChan := w.Process(streamer)

	counter := 0
	for stat := range statsChan {
		assert.Equal(t, time.Duration(1), stat.ExecutionTime)
		counter++
	}
	assert.Equal(t, 100, counter)
	err := <-errChan
	assert.Nil(t, err)
}

// DB Database representation
type MockDB struct {
}

// Run it
func (d *MockDB) Run(host string, start time.Time, end time.Time) (db.Stat, error) {
	return db.Stat{ExecutionTime: time.Duration(1)}, nil
}

// DB Database representation
type MockErrorDB struct {
}

// Run it
func (d *MockErrorDB) Run(host string, start time.Time, end time.Time) (db.Stat, error) {
	return db.Stat{}, fmt.Errorf("Bad Error")
}
