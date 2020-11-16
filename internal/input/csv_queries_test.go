package input_test

import (
	"github.com/shawnfeldman/timescale-benchmark/internal/errors"
	"github.com/shawnfeldman/timescale-benchmark/internal/input"

	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

func TestNoInputErr(t *testing.T) {
	streamer := input.CSVStreamer{1}

	path := ""
	_, errChan := streamer.Stream(path)
	err := <-errChan
	assert.Equal(t, "open : no such file or directory", err.Error())
	_, ok := err.(*errors.CSVLineError)
	assert.True(t, !ok)
}

func TestBadInputErr(t *testing.T) {
	streamer := input.CSVStreamer{1}

	path := "./test.csv"
	_, errChan := streamer.Stream(path)
	err := <-errChan
	assert.Equal(t, "open ./test.csv: no such file or directory", err.Error())
	_, ok := err.(*errors.CSVLineError)
	assert.True(t, !ok)
}

func TestOkInput(t *testing.T) {
	streamer := input.CSVStreamer{1}

	path := "ok.csv"
	workChan, errChan := streamer.Stream(path)
	w := <-workChan
	assert.Equal(t, "mytesthost", w.Host)
	testTime, _ := time.Parse(input.TIMEFORMAT, "2017-01-01 08:59:22")
	assert.Equal(t, testTime.Unix(), w.Start.Unix())
	testTime, _ = time.Parse(input.TIMEFORMAT, "2017-01-01 09:59:22")
	assert.Equal(t, testTime.Unix(), w.End.Unix())
	err := <-errChan
	assert.Nil(t, err)

}
func TestNotOkInput(t *testing.T) {
	streamer := input.CSVStreamer{1}

	path := "not_ok.csv"
	_, errChan := streamer.Stream(path)
	err := <-errChan
	_, ok := err.(*errors.CSVLineError)
	assert.True(t, ok)

}

func TestNotOkInputColumns(t *testing.T) {
	streamer := input.CSVStreamer{1}

	path := "not_ok_header.csv"
	_, errChan := streamer.Stream(path)
	err := <-errChan
	assert.Equal(t, "Error Parsing CSV at line 1 header is not present as expected", err.Error())
	_, ok := err.(*errors.CSVLineError)
	assert.True(t, ok)
}
