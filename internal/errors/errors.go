package errors

import (
	"fmt"
)

// CSVLineError csv error at line
type CSVLineError struct {
	LineNumber int
	ParseError error
}

func (e *CSVLineError) Error() string {
	return fmt.Sprintf("Error Parsing CSV at line %d %+v", e.LineNumber, e.ParseError)
}
