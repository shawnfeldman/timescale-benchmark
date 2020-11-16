To implement a benchmark tool we'll take a csv file as input for hosts and time ranges.  The CSV format will be strict and the benchmark will fatally crash if bad input is received.  The utility will report what the problem was however.

We'll break the Benchmark Util into several logical entities 
1. CSV Streamer (Input)
2. Worker Fan out (Map Phase) + DB Query (Managed by workers) 
3. Aggregate Results (Reduce Phase)

The process will instantiate all 4 entities as distinct worker sets.  Each will have its own set of threads and be managed by a worker cap and a fixed buffer to limit memory and control fan out.  We will utilize go channels to pass messages and manage control of each step.  Go Channels are async message passing implementations that allow state to be passed across threads via buffered streams.  Those buffered streams can be capped so we can limit processing and limit 

The CSV Streamer create a buffered stream to pass Query Parameters read from the CSV  The buffered stream will be capped at a limit passed in from the command line.  The CSV Streamer will run on its own thread.

The Worker Fan out will read the buffered stream and assign each Query Param to a worker by host name.  The Host Name is hashed and modulod across the max worker count to ensure we consistently deliver worker to the same worker.  Each worker is started as a go func and will read from a set of channels its been assigned.  Assignment is on startup.  The worker will maintain the same slot in the "hash ring".  The worker will connect to the db via a dependency passed in from the main process.  We will query from postgres/timescale using each query param.  This will ensure the connection pool is shared and all workers use the same resource.  Once the worker is done it will publish stats on a buffered channel for the stats aggregation.  

Stats Aggregation running in the main thread will continually pull off the buffered channel and aggregate all stats.  It will then print the results to the terminal and potentially export to CSV.