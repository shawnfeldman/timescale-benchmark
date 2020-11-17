package main

import (
	"flag"
	"log"
	"os"

	"github.com/shawnfeldman/timescale-benchmark/internal/benchmark"
	"github.com/shawnfeldman/timescale-benchmark/internal/db"
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
)

func init() {
	flag.StringVar(&filePath, "file", "./mycsv.csv", "path to csv file for query param input")
	flag.StringVar(&host, "host", "localhost", "postgres host")
	flag.StringVar(&user, "user", "postgres", "postgres user")
	flag.StringVar(&password, "password", "", "postgres password")
	flag.StringVar(&dbname, "db", "homework", "postgres db name")
	flag.IntVar(&port, "port", 5432, "postgres port")

	flag.IntVar(&workerThreads, "workers", 10, "number of workers processing file work")
	flag.IntVar(&buffer, "buffer", 20, "file buffer to limit concurrency on files")

	log.SetOutput(os.Stdout)
}

func main() {

	flag.Parse()

	if filePath == "" || filePath == "./mycsv.csv" {
		log.Fatalf("%s must not be empty or default", filePath)
	}
	db := &db.DB{}
	err := db.Open(host, port, dbname, user, password)
	if err != nil {
		log.Fatalf("Failed to connect to db %+v", err)
	}
	b := benchmark.Benchmark{StatsReader: db}
	stats := b.Run(filePath, workerThreads, buffer)
	log.Printf("Here is the stats dump %+v", stats)
}
