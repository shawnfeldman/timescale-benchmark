package benchmark

import (
	"fmt"
	"sort"
	"time"

	"github.com/shawnfeldman/timescale-benchmark/internal/db"
	"github.com/shawnfeldman/timescale-benchmark/internal/input"
	"github.com/shawnfeldman/timescale-benchmark/internal/workers"
)

// Benchmark runner
type Benchmark struct {
	Identifier    string
	statsReader   db.StatsReader
	Aggregation   AggregatedStats
	streamedStats []db.Stat
}

// NewBenchmark instantiation for benchmark
func NewBenchmark(id string, db db.StatsReader) Benchmark {
	return Benchmark{statsReader: db, Identifier: id}
}

/*
particular, we are looking for the # of queries run, the total
processing time across all queries, the minimum query time (for a single query), the median
query time, the average query time, and the maximum query time.
*/

// AggregatedStats stores the total stats across the benchmark run
type AggregatedStats struct {
	TotalTime        int64
	MinQueryTime     int64
	MedianQueryTime  float64
	MeanQueryTime    float64
	MaximumQueryTime int64
	Count            int
}

// Run the csv stream based on file path, buffer and workerThreads limit capactiy
func (b *Benchmark) Run(filePath string, workerThreads, buffer int) (AggregatedStats, error) {
	streamer := &input.CSVStreamer{Buffer: buffer}
	streamerChan, streamerErrChan := streamer.Stream(filePath)
	w := workers.WorkerProcessor{StatsReader: b.statsReader, Workers: workerThreads, StatsBuffer: buffer}
	statsChan, workerErrChan := w.Process(streamerChan)

	for {
		select {
		case err := <-streamerErrChan: // stream to main err chan
			if err != nil {
				return b.Aggregation, fmt.Errorf("Failed during streaming: %+v", err)
			}

			break
		case err := <-workerErrChan: // stream to main err chan
			if err != nil {
				return b.Aggregation, fmt.Errorf("Failed during workers: %+v", err)
			}
			break
		case stats := <-statsChan:
			if stats.Host != "" {
				b.ProcessStats(&stats)
				// collect stats
			} else {
				b.Aggregation.MedianQueryTime = GetMedian(b.streamedStats)
				b.Aggregation.MeanQueryTime = float64(b.Aggregation.TotalTime) / float64(b.Aggregation.Count)
				return b.Aggregation, nil
			}
			break
		default:
			time.Sleep(100 * time.Millisecond)
		}

	}
}

// ProcessStats process new stats from db and add to aggregation, use pointer to prevent new mem allocation
func (b *Benchmark) ProcessStats(stat *db.Stat) {
	executionTime := stat.ExecutionTime.Milliseconds()
	if stat.ExecutionTime.Milliseconds() > b.Aggregation.MaximumQueryTime {
		b.Aggregation.MaximumQueryTime = executionTime
	}
	if stat.ExecutionTime.Milliseconds() < b.Aggregation.MinQueryTime || b.Aggregation.Count == 0 { // account for first
		b.Aggregation.MinQueryTime = executionTime
	}
	b.Aggregation.TotalTime = b.Aggregation.TotalTime + executionTime
	b.Aggregation.Count++
	// dereference the point because slices are really just pointers
	b.streamedStats = append(b.streamedStats, *stat)
}

// StatToParam convert Stat point to Query Param
func StatToParam(stat *db.Stat) input.QueryParams {
	return input.QueryParams{Host: stat.Host, Start: stat.Start, End: stat.End}
}

// GetMedian Set the Median
func GetMedian(stats []db.Stat) float64 {
	// sort the stats
	sort.SliceStable(stats, func(i, j int) bool {
		return stats[i].ExecutionTime < stats[j].ExecutionTime
	})
	mid := len(stats) / 2
	// if odd return the clear median
	if len(stats)%2 == 1 {
		return float64(stats[mid].ExecutionTime.Milliseconds())
	}
	// else average the two medians
	middle := (stats[mid].ExecutionTime.Milliseconds() + stats[mid-1].ExecutionTime.Milliseconds())
	return float64(middle) / float64(2)
}
