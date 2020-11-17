package db

import (
	"database/sql"
	"fmt"
	"time"

	_ "github.com/lib/pq" // importing postgres driver
)

const (
	query = `SELECT time_bucket('1 minutes', ts) AS one_min,
	MAX(usage) AS max_usage,
	MIN(usage) AS min_usage
FROM cpu_usage 
WHERE host = $1 and ts >= $2 and ts < $3
GROUP BY one_min, host
ORDER BY one_min DESC`
	format = "2006-01-02 15:04:05+00"
)

// UsageStats internal query stats from timescale
type UsageStats struct {
	Bucket time.Time
	Max    float64
	Min    float64
}

// Stat representation
type Stat struct {
	Host          string
	Start         time.Time
	End           time.Time
	UsageStats    []UsageStats
	ExecutionTime time.Duration
}

// StatsReader interface
type StatsReader interface {
	Run(host string, start time.Time, end time.Time) (Stat, error)
}

// DB Database representation
type DB struct {
	db *sql.DB
}

// Open the connection
func (d *DB) Open(host string, port int, dbName, user, password string) error {
	psqlconn := fmt.Sprintf("postgres://%s:%s@%s:%d/%s?sslmode=disable", user, password, host, port, dbName)
	db, err := sql.Open("postgres", psqlconn)
	d.db = db
	return err
}

// Run and aggregate results
func (d *DB) Run(host string, start time.Time, end time.Time) (Stat, error) {
	now := time.Now()
	rows, err := d.db.Query(query, host, start, end)

	if err != nil {
		return Stat{}, err
	}
	defer rows.Close()

	usageStats := make([]UsageStats, 0)
	for rows.Next() {
		var bucket time.Time
		var max float64
		var min float64

		err = rows.Scan(&bucket, &max, &min)
		if err != nil {
			return Stat{}, err
		}
		usageStat := UsageStats{Bucket: bucket, Max: max, Min: min}
		usageStats = append(usageStats, usageStat)
	}
	executionTime := time.Since(now)

	return Stat{
		Host:          host,
		Start:         start,
		End:           end,
		UsageStats:    usageStats,
		ExecutionTime: executionTime,
	}, nil
}
