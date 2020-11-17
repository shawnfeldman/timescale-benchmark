package benchmark

import (
	"log"
	"time"

	"github.com/shawnfeldman/timescale-benchmark/internal/db"
	"github.com/shawnfeldman/timescale-benchmark/internal/input"
	"github.com/shawnfeldman/timescale-benchmark/internal/workers"
)

// Benchmark runner
type Benchmark struct {
	StatsReader db.StatsReader
	Aggregation Stats
}

/*
particular, we are looking for the # of queries run, the total
processing time across all queries, the minimum query time (for a single query), the median
query time, the average query time, and the maximum query time.
*/

// Stats stores the total stats across the benchmark run
type Stats struct {
	TotalTime        time.Duration
	MinQueryTime     time.Duration
	MinQuery         input.QueryParams
	MedianQueryTime  time.Duration
	MedianQuery      input.QueryParams
	AverageQueryTime time.Duration
	MaximumQueryTime time.Duration
	MaximumQuery     input.QueryParams
}

// Run the csv stream based on file path, buffer and workerThreads limit capactiy
func (b *Benchmark) Run(filePath string, workerThreads, buffer int) Stats {
	streamer := &input.CSVStreamer{Buffer: buffer}
	streamerChan, streamerErrChan := streamer.Stream(filePath)
	w := workers.WorkerProcessor{StatsReader: b.StatsReader, Workers: workerThreads, StatsBuffer: buffer}
	statsChan, workerErrChan := w.Process(streamerChan)

	for {
		select {
		case err := <-streamerErrChan: // stream to main err chan
			if err != nil {
				log.Fatalf("Failed during streaming %+v", err)
			}

			break
		case err := <-workerErrChan: // stream to main err chan
			if err != nil {
				log.Fatalf("Failed during workers %+v", err)
			}
			break
		case stats := <-statsChan:
			if stats.Host != "" {
				b.ProcessStats(&stats)
				// collect stats
			} else {
				// signal to be done
				return b.Aggregation
			}
			break
		default:
			time.Sleep(100 * time.Millisecond)
		}

	}
}

// ProcessStats process new stats from db and add to aggregation
func (b *Benchmark) ProcessStats(stat *db.Stat) {
	if stat.ExecutionTime > b.Aggregation.MaximumQueryTime {
		b.Aggregation.MaximumQueryTime = stat.ExecutionTime
	}
	if stat.ExecutionTime < b.Aggregation.MinQueryTime {
		b.Aggregation.MinQueryTime = stat.ExecutionTime
	}
}
