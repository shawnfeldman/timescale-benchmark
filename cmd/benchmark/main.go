package main

import (
	"flag"
	"log"
	"os"

	"github.com/shawnfeldman/timescale-benchmark/internal/benchmark"
	"github.com/shawnfeldman/timescale-benchmark/internal/db"
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
	b := benchmark.Benchmark{StatsReader: &db.DB{}}
	stats := b.Run(filePath, workerThreads, buffer)
	log.Printf("Here is the stats dump %+v", stats)
}
