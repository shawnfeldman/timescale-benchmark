package main

import (
	"flag"
	"fmt"
	"os"

	"github.com/shawnfeldman/timescale-benchmark/internal/benchmark"
	"github.com/shawnfeldman/timescale-benchmark/internal/db"
	log "github.com/sirupsen/logrus"
)

var (
	filePath      string
	buffer        = 100
	workerThreads = 10
	host          string
	port          int
	user          string
	password      string
	dbname        = "homework"
	format        = "terminal"
	debug         = false
	pretty        = true
	runs          = 1
)

func init() {
	flag.StringVar(&filePath, "file", "db/query_params.csv", "path to csv file for query param input")
	flag.StringVar(&host, "host", "localhost", "postgres host")
	flag.StringVar(&user, "user", "postgres", "postgres user")
	flag.StringVar(&password, "password", "", "postgres password")
	flag.StringVar(&dbname, "db", "homework", "postgres db name")
	flag.BoolVar(&debug, "debug", false, "set debug: true or false")
	flag.BoolVar(&pretty, "pretty_print", true, "set pretty_print: true or false, true will print across multiple lines")

	flag.IntVar(&port, "port", 5432, "postgres port")
	flag.IntVar(&runs, "runs", 1, "number of runs to display")
	flag.IntVar(&workerThreads, "workers", 10, "number of workers processing file work")
	flag.IntVar(&buffer, "buffer", 20, "file buffer to limit concurrency on files")

	log.SetOutput(os.Stdout)
}

// MultiRunStats add additional stats over aggregation
type MultiRunStats struct {
	benchmark.AggregatedStats
	MeanRunTotalTime float64
}

func main() {
	flag.Parse()
	if debug {
		log.SetLevel(log.DebugLevel)
	}
	log.SetFormatter(&log.JSONFormatter{PrettyPrint: pretty})

	if filePath == "" || filePath == "./mycsv.csv" {
		log.WithField("file", filePath).Fatal("File Path must not be empty or default")
	}
	db := &db.DB{}
	err := db.Open(host, port, dbname, user, password)
	if err != nil {
		log.WithError(err).Fatal("Failed to connect to db")
	}

	multiRunStats := MultiRunStats{}
	for i := 0; i < runs; i++ {
		b := benchmark.NewBenchmark(fmt.Sprintf("%d", i+1), db)
		stats, err := b.Run(filePath, workerThreads, buffer)
		if err != nil {
			log.WithError(err).Fatal("Failed to run benchmark")
		}

		log.WithFields(log.Fields{
			"run_id":                   b.Identifier,
			"total_execution_time_ms":  stats.TotalTime,
			"min_execution_time_ms":    stats.MinQueryTime,
			"median_execution_time_ms": stats.MedianQueryTime,
			"mean_execution_time_ms":   stats.MeanQueryTime,
			"max_execution_time_ms":    stats.MaximumQueryTime,
			"number_queries":           stats.Count,
			"threads_run":              workerThreads,
		}).Info("Benchmark Run")
		multiRunStats.Count += stats.Count
		multiRunStats.TotalTime += stats.TotalTime
		if stats.MaximumQueryTime > multiRunStats.MaximumQueryTime {
			multiRunStats.MaximumQueryTime = stats.MaximumQueryTime
		}
		if i == 0 || stats.MinQueryTime < multiRunStats.MinQueryTime {
			multiRunStats.MinQueryTime = stats.MinQueryTime
		}
	}
	if runs > 1 {
		multiRunStats.MeanQueryTime = float64(multiRunStats.TotalTime) / float64(multiRunStats.Count)
		log.WithFields(log.Fields{
			"run_id":                  "all",
			"total_execution_time_ms": multiRunStats.TotalTime,
			"min_execution_time_ms":   multiRunStats.MinQueryTime,
			"mean_execution_time_ms":  multiRunStats.MeanQueryTime,
			"max_execution_time_ms":   multiRunStats.MaximumQueryTime,
			"number_queries":          multiRunStats.Count,
			//"median_execution_time_ms": multiRunStats.MedianQueryTime, tbd
		}).Info("All Benchmark Runs Complete")
	}
}
