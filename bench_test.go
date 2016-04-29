package main

import (
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx"
)

var (
	pgxPool *pgx.ConnPool
)

func TestMain(m *testing.M) {
	flag.Parse()
	err := setup()
	if err != nil {
		fmt.Fprintf(os.Stderr, "Unable to setup test: %v\n", err)
		os.Exit(1)
	}

	os.Exit(m.Run())
}

func setup() error {
	config, err := extractConfig()
	if err != nil {
		return err
	}

	pgxPool, err = pgx.NewConnPool(config)
	if err != nil {
		return err
	}
}

const selectUserSQL = `
select id, first_name, last_name, sex, birth_date, weight, height, update_time
from person
where id=$1`

var selectPersonSQLQuestionMark = `
select id, first_name, last_name, sex, birth_date, weight, height, update_time
from person
where id=?`

var selectMultiplePeopleSQL = `
select id, first_name, last_name, sex, birth_date, weight, height, update_time
from person
where id between $1 and $1 + 24`
var selectMultiplePeopleSQLQuestionMark = `
select id, first_name, last_name, sex, birth_date, weight, height, update_time
from person
where id between ? and ? + 24`

type person struct {
	Id         int32
	FirstName  string
	LastName   string
	Sex        string
	BirthDate  time.Time
	Weight     int32
	Height     int32
	UpdateTime time.Time
}

func BenchmarkPgxNativeSelectSingleValue(b *testing.B) {
	setup(b)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		id := randPersonIDs[i%len(randPersonIDs)]
		var firstName string
		err := pgxPool.QueryRow("selectPersonName", id).Scan(&firstName)
		if err != nil {
			b.Fatalf("pgxPool.QueryRow Scan failed: %v", err)
		}
		if len(firstName) == 0 {
			b.Fatal("FirstName was empty")
		}
	}
}

func BenchmarkPgxStdlibSelectSingleValue(b *testing.B) {
	setup(b)
	stmt, err := pgxStdlib.Prepare(selectPersonNameSQL)
	if err != nil {
		b.Fatalf("Prepare failed: %v", err)
	}
	defer stmt.Close()

	benchmarkSelectSingleValue(b, stmt)
}

func BenchmarkPgSelectSingleValue(b *testing.B) {
	setup(b)

	stmt, err := pg.Prepare(selectPersonNameSQL)
	if err != nil {
		b.Fatalf("Prepare failed: %v", err)
	}
	defer stmt.Close()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		var firstName string
		id := randPersonIDs[i%len(randPersonIDs)]
		_, err := stmt.QueryOne(gopg.LoadInto(&firstName), id)
		if err != nil {
			b.Fatalf("stmt.QueryOne failed: %v", err)
		}
		if len(firstName) == 0 {
			b.Fatal("FirstName was empty")
		}
	}
}

func BenchmarkPqSelectSingleValue(b *testing.B) {
	setup(b)
	stmt, err := pq.Prepare(selectPersonNameSQL)
	if err != nil {
		b.Fatalf("Prepare failed: %v", err)
	}
	defer stmt.Close()

	benchmarkSelectSingleValue(b, stmt)
}

func benchmarkSelectSingleValue(b *testing.B, stmt *sql.Stmt) {
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		id := randPersonIDs[i%len(randPersonIDs)]
		row := stmt.QueryRow(id)
		var firstName string
		err := row.Scan(&firstName)
		if err != nil {
			b.Fatalf("row.Scan failed: %v", err)
		}
		if len(firstName) == 0 {
			b.Fatal("FirstName was empty")
		}
	}
}

func BenchmarkRawSelectSingleValue(b *testing.B) {
	setup(b)

	b.ResetTimer()

	txBufs := make([][]byte, len(randPersonIDs))
	for i, personID := range randPersonIDs {
		var err error
		txBufs[i], err = rawConn.BuildPreparedQueryBuf(rawSelectPersonNameStmt, personID)
		if err != nil {
			b.Fatalf("rawConn.BuildQueryBuf failed: %v", err)
		}
	}

	for i := 0; i < b.N; i++ {
		txBuf := txBufs[i%len(txBufs)]
		_, err := rawConn.Conn().Write(txBuf)
		if err != nil {
			b.Fatalf("rawConn.Conn.Write failed: %v", err)
		}

		rxRawUntilReady(b)
	}
}

func checkPersonWasFilled(b *testing.B, p person) {
	if p.Id == 0 {
		b.Fatal("id was 0")
	}
	if len(p.FirstName) == 0 {
		b.Fatal("FirstName was empty")
	}
	if len(p.LastName) == 0 {
		b.Fatal("LastName was empty")
	}
	if len(p.Sex) == 0 {
		b.Fatal("Sex was empty")
	}
	var zeroTime time.Time
	if p.BirthDate == zeroTime {
		b.Fatal("BirthDate was zero time")
	}
	if p.Weight == 0 {
		b.Fatal("Weight was 0")
	}
	if p.Height == 0 {
		b.Fatal("Height was 0")
	}
	if p.UpdateTime == zeroTime {
		b.Fatal("UpdateTime was zero time")
	}
}

func BenchmarkPgxNativeSelectSingleRow(b *testing.B) {
	setup(b)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		var p person
		id := randPersonIDs[i%len(randPersonIDs)]

		rows, _ := pgxPool.Query("selectPerson", id)
		for rows.Next() {
			rows.Scan(&p.Id, &p.FirstName, &p.LastName, &p.Sex, &p.BirthDate, &p.Weight, &p.Height, &p.UpdateTime)
		}
		if rows.Err() != nil {
			b.Fatalf("pgxPool.Query failed: %v", rows.Err())
		}

		checkPersonWasFilled(b, p)
	}
}

func BenchmarkPgxStdlibSelectSingleRow(b *testing.B) {
	setup(b)
	stmt, err := pgxStdlib.Prepare(selectPersonSQL)
	if err != nil {
		b.Fatalf("Prepare failed: %v", err)
	}
	defer stmt.Close()

	benchmarkSelectSingleRow(b, stmt)
}

func BenchmarkPgSelectSingleRow(b *testing.B) {
	setup(b)

	stmt, err := pg.Prepare(selectPersonSQL)
	if err != nil {
		b.Fatalf("Prepare failed: %v", err)
	}
	defer stmt.Close()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		var p person
		id := randPersonIDs[i%len(randPersonIDs)]
		_, err := stmt.QueryOne(&p, id)
		if err != nil {
			b.Fatalf("stmt.QueryOne failed: %v", err)
		}

		checkPersonWasFilled(b, p)
	}
}

func BenchmarkPqSelectSingleRow(b *testing.B) {
	setup(b)
	stmt, err := pq.Prepare(selectPersonSQL)
	if err != nil {
		b.Fatalf("Prepare failed: %v", err)
	}
	defer stmt.Close()

	benchmarkSelectSingleRow(b, stmt)
}

func benchmarkSelectSingleRow(b *testing.B, stmt *sql.Stmt) {
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		id := randPersonIDs[i%len(randPersonIDs)]
		row := stmt.QueryRow(id)
		var p person
		err := row.Scan(&p.Id, &p.FirstName, &p.LastName, &p.Sex, &p.BirthDate, &p.Weight, &p.Height, &p.UpdateTime)
		if err != nil {
			b.Fatalf("row.Scan failed: %v", err)
		}

		checkPersonWasFilled(b, p)
	}
}

func BenchmarkRawSelectSingleRow(b *testing.B) {
	setup(b)
	b.ResetTimer()

	txBufs := make([][]byte, len(randPersonIDs))
	for i, personID := range randPersonIDs {
		var err error
		txBufs[i], err = rawConn.BuildPreparedQueryBuf(rawSelectPersonStmt, personID)
		if err != nil {
			b.Fatalf("rawConn.BuildPreparedQueryBuf failed: %v", err)
		}
	}

	for i := 0; i < b.N; i++ {
		txBuf := txBufs[i%len(txBufs)]
		_, err := rawConn.Conn().Write(txBuf)
		if err != nil {
			b.Fatalf("rawConn.Conn.Write failed: %v", err)
		}

		rxRawUntilReady(b)
	}
}

func BenchmarkPgxNativeSelectMultipleRows(b *testing.B) {
	setup(b)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		var people []person
		id := randPersonIDs[i%len(randPersonIDs)]

		rows, _ := pgxPool.Query("selectMultiplePeople", id)
		for rows.Next() {
			var p person
			rows.Scan(&p.Id, &p.FirstName, &p.LastName, &p.Sex, &p.BirthDate, &p.Weight, &p.Height, &p.UpdateTime)
			people = append(people, p)
		}
		if rows.Err() != nil {
			b.Fatalf("pgxPool.Query failed: %v", rows.Err())
		}

		for _, p := range people {
			checkPersonWasFilled(b, p)
		}
	}
}

func BenchmarkPgxStdlibSelectMultipleRows(b *testing.B) {
	setup(b)

	stmt, err := pgxStdlib.Prepare(selectMultiplePeopleSQL)
	if err != nil {
		b.Fatalf("Prepare failed: %v", err)
	}
	defer stmt.Close()

	benchmarkSelectMultipleRows(b, stmt)
}

func BenchmarkPgSelectMultipleRows(b *testing.B) {
	setup(b)

	stmt, err := pg.Prepare(selectMultiplePeopleSQL)
	if err != nil {
		b.Fatalf("Prepare failed: %v", err)
	}
	defer stmt.Close()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		var people People
		id := randPersonIDs[i%len(randPersonIDs)]
		_, err := stmt.Query(&people, id)
		if err != nil {
			b.Fatalf("stmt.Query failed: %v", err)
		}

		for i, _ := range people.C {
			checkPersonWasFilled(b, people.C[i])
		}
	}
}

func BenchmarkPgSelectMultipleRowsAndDiscard(b *testing.B) {
	setup(b)

	stmt, err := pg.Prepare(selectMultiplePeopleSQL)
	if err != nil {
		b.Fatalf("Prepare failed: %v", err)
	}
	defer stmt.Close()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		id := randPersonIDs[i%len(randPersonIDs)]
		_, err := stmt.Query(gopg.Discard, id)
		if err != nil {
			b.Fatalf("stmt.Query failed: %v", err)
		}
	}
}

func BenchmarkPqSelectMultipleRows(b *testing.B) {
	setup(b)

	stmt, err := pq.Prepare(selectMultiplePeopleSQL)
	if err != nil {
		b.Fatalf("Prepare failed: %v", err)
	}
	defer stmt.Close()

	benchmarkSelectMultipleRows(b, stmt)
}

func benchmarkSelectMultipleRows(b *testing.B, stmt *sql.Stmt) {
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		var people []person
		id := randPersonIDs[i%len(randPersonIDs)]
		rows, err := stmt.Query(id)
		if err != nil {
			b.Fatalf("db.Query failed: %v", err)
		}

		for rows.Next() {
			var p person
			err := rows.Scan(&p.Id, &p.FirstName, &p.LastName, &p.Sex, &p.BirthDate, &p.Weight, &p.Height, &p.UpdateTime)
			if err != nil {
				b.Fatalf("rows.Scan failed: %v", err)
			}
			people = append(people, p)
		}

		if rows.Err() != nil {
			b.Fatalf("rows.Err() returned an error: %v", err)
		}

		for _, p := range people {
			checkPersonWasFilled(b, p)
		}
	}
}

func BenchmarkRawSelectMultipleRows(b *testing.B) {
	setup(b)

	b.ResetTimer()

	txBufs := make([][]byte, len(randPersonIDs))
	for i, personID := range randPersonIDs {
		var err error
		txBufs[i], err = rawConn.BuildPreparedQueryBuf(rawSelectMultiplePeopleStmt, personID)
		if err != nil {
			b.Fatalf("rawConn.BuildPreparedQueryBuf failed: %v", err)
		}
	}

	for i := 0; i < b.N; i++ {
		txBuf := txBufs[i%len(txBufs)]
		_, err := rawConn.Conn().Write(txBuf)
		if err != nil {
			b.Fatalf("rawConn.Conn.Write failed: %v", err)
		}

		rxRawUntilReady(b)
	}
}

func rxRawUntilReady(b *testing.B) {
	for {
		n, err := rawConn.Conn().Read(rxBuf)
		if err != nil {
			b.Fatalf("rawConn.Conn.Read failed: %v", err)
		}
		if rxBuf[n-6] == 'Z' && rxBuf[n-2] == 5 && rxBuf[n-1] == 'I' {
			return
		}
	}
}
