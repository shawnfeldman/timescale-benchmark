package main

import (
	"flag"
	"log"
	"os"
	"time"

	"github.com/shawnfeldman/timescale-benchmark/internal/db"
	"github.com/shawnfeldman/timescale-benchmark/internal/input"
	"github.com/shawnfeldman/timescale-benchmark/internal/workers"
)

var filePath string
var buffer = 100
var workerThreads = 10

func init() {
	flag.StringVar(&filePath, "file", "./mycsv.csv", "path to csv file for query param input")
	flag.IntVar(&workerThreads, "workers", 10, "number of workers processing file work")
	flag.IntVar(&buffer, "buffer", 20, "file buffer to limit concurrency on files")

	log.SetOutput(os.Stdout)
}

func main() {
	flag.Parse()

	if filePath == "" || filePath == "./mycsv.csv" {
		log.Fatalf("%s must not be empty or default", filePath)
	}

	streamer := &input.CSVStreamer{Buffer: buffer}
	streamerChan, streamerErrChan := streamer.Stream(filePath)
	w := workers.WorkerProcessor{StatsReader: &db.DB{}, Workers: workerThreads, StatsBuffer: buffer}
	statsChan, workerErrChan := w.Process(streamerChan)

	emptyStat := db.Stat{}
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
			if stats != emptyStat {
				sum += stats.Average
				// collect stats
			} else {
				// TODO: ProcessStats
				// signal to be done
				log.Printf("stats done %d", sum)
				return
			}
			break
		default:
			time.Sleep(100 * time.Millisecond)
		}

	}

}
