package input

import (
	"encoding/csv"
	"fmt"
	"io"
	"os"
	"time"

	"github.com/shawnfeldman/timescale-benchmark/internal/errors"
	log "github.com/sirupsen/logrus"
)

// Streamer interface
type Streamer interface {
	Stream(filePath string, queryChannels []chan QueryParams) chan error
}

// CSVStreamer container
type CSVStreamer struct {
	Buffer int
}

// QueryParams query paramters
type QueryParams struct {
	Start time.Time
	End   time.Time
	Host  string
}

const (
	// TIMEFORMAT for formatting time from csv
	TIMEFORMAT = "2006-01-02 15:04:05"
)

// Stream streams out based on file path, takes the file path and a set of query channels, the streamer will stream to the hashslot by host name
func (s *CSVStreamer) Stream(filePath string) (chan QueryParams, chan error) {
	outChan := make(chan QueryParams, s.Buffer)
	errChan := make(chan error)
	line := 1
	go func() {
		defer close(errChan)
		defer close(outChan)
		csvFile, err := os.Open(filePath)
		if err != nil {
			errChan <- err
			return
		}
		r := csv.NewReader(csvFile)
		for ; ; line++ {
			record, err := r.Read()
			if err == io.EOF {
				break
			}
			if err != nil {
				errChan <- err
			}
			if line == 1 {
				isHeader := len(record) == 3 && record[0] == "hostname"
				if isHeader {
					continue
				}
				e := &errors.CSVLineError{LineNumber: line, ParseError: fmt.Errorf("header is not present as expected")}
				errChan <- e
				return
			}
			if len(record) != 3 {
				e := &errors.CSVLineError{LineNumber: line, ParseError: fmt.Errorf("not enough columns in csv")}
				errChan <- e
				return
			}
			host := record[0]
			start, err := time.Parse(TIMEFORMAT, record[1])
			if err != nil {
				e := &errors.CSVLineError{LineNumber: line, ParseError: err}
				errChan <- e
				return
			}
			end, err := time.Parse(TIMEFORMAT, record[2])
			if err != nil {
				e := &errors.CSVLineError{LineNumber: line, ParseError: err}
				errChan <- e
				return
			}
			q := QueryParams{
				Start: start,
				End:   end,
				Host:  host,
			}
			// send the param to the hash slot
			outChan <- q
		}
		log.WithField("file", filePath).WithField("rows", line).Debug("Done streaming file")

	}()
	return outChan, errChan
}
