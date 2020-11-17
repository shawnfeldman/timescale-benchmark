package workers

import (
	"hash/fnv"
	"sync"
	"sync/atomic"

	"github.com/shawnfeldman/timescale-benchmark/internal/db"
	"github.com/shawnfeldman/timescale-benchmark/internal/input"
	log "github.com/sirupsen/logrus"
)

// WorkerProcessor processes work funneled by streamer
type WorkerProcessor struct {
	StatsReader db.StatsReader
	Workers     int
	StatsBuffer int
}

// Process the input stream and fan out the work
func (w *WorkerProcessor) Process(streamParams chan input.QueryParams) (chan db.Stat, chan error) {
	transactionCount := int32(0)
	queryChannels := make([]chan input.QueryParams, 0)
	statsChan := make(chan db.Stat, w.StatsBuffer)
	errChan := make(chan error)
	wg := sync.WaitGroup{}

	// fan out work to many workers
	for worker := 0; worker < w.Workers; worker++ {
		queryChannels = append(queryChannels, make(chan input.QueryParams))
		wg.Add(1)
		// spawn worker
		go func(c chan input.QueryParams, worker int) {
			defer wg.Done()
			// do work on query input
			for q := range c {
				if q.Host != "" { // check for drain on closed channel
					stat, err := w.StatsReader.Run(q.Host, q.Start, q.End)
					atomic.AddInt32(&transactionCount, 1)

					if err != nil {
						statsChan <- stat
						errChan <- err
						continue
					}
					statsChan <- stat
				}
			}
		}(queryChannels[worker], worker)
	}
	// take stream and send to workers
	go func() {
		defer close(statsChan)
		defer close(errChan)
		for queryParam := range streamParams {
			if queryParam.Host != "" { // check for drain on closed channel
				slot := HashSlot(queryParam.Host, w.Workers)
				queryChannels[slot] <- queryParam
			}
		}
		// no more input so close the other channels
		for _, c := range queryChannels {
			// c <- input.QueryParams{} // send empty to indicate done
			close(c)
		}

		// wait for completion on all workers
		wg.Wait()
		log.WithField("transactions", transactionCount).Debug("Done Reading Processing  Workers and All Connections closed transactions")
	}()

	return statsChan, errChan
}

// HashSlot find slot for any string
func HashSlot(host string, size int) int {
	h := fnv.New32a()
	h.Write([]byte(host))
	return int(h.Sum32()) % size
}
