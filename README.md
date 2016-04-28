# pgx Benchmark

This tests the performance of [pgx](https://github.com/jackc/pgx), [pgx
through database/sql](https://github.com/jackc/pgx/tree/master/stdlib),
[pq](https://github.com/lib/pq) through database/sql, [go-pg](github.com/go-
pg/pg),  and theoretical maximum PostgreSQL performance. It always uses stored
procedures.

## Configuration

go_db_bench reads its configuration from the environment:

    PGHOST - defaults to localhost
    PGUSER - default to OS user
    PGPASSWORD - defaults to empty string
    PGDATABASE - defaults to go_db_bench

## Core Benchmarks

go_db_bench includes tests selecting one value, one row, and multiple rows.

Example execution:

    PGHOST=/private/tmp go test -test.bench=. -test.benchmem
