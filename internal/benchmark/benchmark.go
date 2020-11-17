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
}

/*
particular, we are looking for the # of queries run, the total
processing time across all queries, the minimum query time (for a single query), the median
query time, the average query time, and the maximum query time.
*/

// Stats stores the total stats across the benchmark run
type Stats struct {
	TotalTime        int64
	MinQueryTime     int64
	MinQuery         input.QueryParams
	MedianQueryTime  int64
	MedianQuery      input.QueryParams
	AverageQueryTime int64
	MaximumQueryTime int64
	MaximumQuery     input.QueryParams
}

// Run the csv stream based on file path, buffer and workerThreads limit capactiy
func (b *Benchmark) Run(filePath string, workerThreads, buffer int) Stats {
	streamer := &input.CSVStreamer{Buffer: buffer}
	streamerChan, streamerErrChan := streamer.Stream(filePath)
	w := workers.WorkerProcessor{StatsReader: b.StatsReader, Workers: workerThreads, StatsBuffer: buffer}
	statsChan, workerErrChan := w.Process(streamerChan)

	sum := 0
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
				sum += stats.Average
				// collect stats
			} else {
				// TODO: ProcessStats
				// signal to be done
				log.Printf("stats done %d", sum)
				return Stats{TotalTime: int64(sum)}
			}
			break
		default:
			time.Sleep(100 * time.Millisecond)
		}

	}
}
