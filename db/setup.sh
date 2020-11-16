psql -U postgres -h localhost < cpu_usage.sql
psql -U postgres -h localhost -d homework -c "\COPY cpu_usage FROM cpu_usage.csv CSV HEADER"