# timescale-benchmark
 
benchmark timescaledb using a csv as input for the queries

[See Architecture](docs/ARCHITECTURE.md)

# Running timescale and benchmark via docker
1. Run `docker-compose build`
2. To start timescale run ` docker-compose up timescaledb`
3. To load the time series data, In a new terminal run `docker-compose exec timescaledb psql -U postgres -h localhost -f /tmp/db/cpu_usage.sql`
4. to see the process run automatically  `docker-compose up`
5. run `docker-compose up | grep benchmark_1`
6. leave timescale running

# Running the Tests
To run the tests we can use the make file, assuming you have go installed
- run `make unit_test` to run the unit tests only
- run `make integration` to run the full integration and view the output
- run `make run` or `make debug` to run the benchmark in debug or non debug mode
- run `make build` then `./benchmark` for default or `./benchmark --help` for args
