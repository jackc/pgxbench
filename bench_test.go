package pgxbench

import (
	"flag"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx"
)

var (
	pgxPool     *pgx.ConnPool
	randUserIDs []int64
	manyCount   int64
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

	rows, err := pgxPool.Query(`select id from pgxbench_user order by random()`)
	if err != nil {
		return err
	}

	for rows.Next() {
		var id int64
		rows.Scan(&id)
		randUserIDs = append(randUserIDs, id)
	}

	if rows.Err() != nil {
		return err
	}

	manyCount = 25

	return nil
}

type user struct {
	id               int64
	active           bool
	admin            bool
	name             string
	email            string
	firstName        string
	lastName         string
	birthDate        time.Time
	passwordDigest   []byte
	loginCount       int32
	failedLoginCount int32
	passwordStrength int32
	creationTime     time.Time
	lastLoginTime    time.Time
}

func BenchmarkPgxSelectOneRow(b *testing.B) {
	conn, err := pgxPool.Acquire()
	if err != nil {
		b.Fatalf("unable to acquire connection: %v", err)
	}
	defer pgxPool.Release(conn)

	psName := "selectOneUser"
	_, err = pgxPool.Prepare(psName, `select id, active, admin, name, email, first_name, last_name, birth_date, password_digest, login_count, failed_login_count, password_strength, creation_time, last_login_time
from pgxbench_user
where id=$1`)
	if err != nil {
		b.Fatalf("unable to prepare query: %v", err)
	}
	defer pgxPool.Deallocate(psName)

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		id := randUserIDs[i%len(randUserIDs)]
		var u user
		err := pgxPool.QueryRow(psName, id).Scan(&u.id, &u.active, &u.admin, &u.name, &u.email, &u.firstName, &u.lastName, &u.birthDate, &u.passwordDigest, &u.loginCount, &u.failedLoginCount, &u.passwordStrength, &u.creationTime, &u.lastLoginTime)
		if err != nil {
			b.Fatalf("pgxPool.QueryRow Scan failed: %v", err)
		}

		// Simple check to ensure data was actually read
		if u.id == 0 {
			b.Fatal("id was 0")
		}
	}
}

func BenchmarkPgxSelectOneString(b *testing.B) {
	conn, err := pgxPool.Acquire()
	if err != nil {
		b.Fatalf("unable to acquire connection: %v", err)
	}
	defer pgxPool.Release(conn)

	psName := "selectOneString"
	_, err = pgxPool.Prepare(psName, `select name from pgxbench_user where id=$1`)
	if err != nil {
		b.Fatalf("unable to prepare query: %v", err)
	}
	defer pgxPool.Deallocate(psName)

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		id := randUserIDs[i%len(randUserIDs)]
		var name string
		err := pgxPool.QueryRow(psName, id).Scan(&name)
		if err != nil {
			b.Fatalf("pgxPool.QueryRow Scan failed: %v", err)
		}

		// Simple check to ensure data was actually read
		if name == "" {
			b.Fatal("name was blank")
		}
	}
}

func BenchmarkPgxSelectOneInt32(b *testing.B) {
	conn, err := pgxPool.Acquire()
	if err != nil {
		b.Fatalf("unable to acquire connection: %v", err)
	}
	defer pgxPool.Release(conn)

	psName := "selectOneInt32"
	_, err = pgxPool.Prepare(psName, `select password_strength from pgxbench_user where id=$1`)
	if err != nil {
		b.Fatalf("unable to prepare query: %v", err)
	}
	defer pgxPool.Deallocate(psName)

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		id := randUserIDs[i%len(randUserIDs)]
		var passwordStrength int32
		err := pgxPool.QueryRow(psName, id).Scan(&passwordStrength)
		if err != nil {
			b.Fatalf("pgxPool.QueryRow Scan failed: %v", err)
		}
	}
}

func BenchmarkPgxSelectManyInt32(b *testing.B) {
	conn, err := pgxPool.Acquire()
	if err != nil {
		b.Fatalf("unable to acquire connection: %v", err)
	}
	defer pgxPool.Release(conn)

	psName := "selectManyInt32"
	_, err = pgxPool.Prepare(psName, `select password_strength from pgxbench_user where id between $1 and $2`)
	if err != nil {
		b.Fatalf("unable to prepare query: %v", err)
	}
	defer pgxPool.Deallocate(psName)

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		id := randUserIDs[i%len(randUserIDs)]
		var passwordStrength int32
		rows, err := pgxPool.Query(psName, id, id+manyCount)
		if err != nil {
			b.Fatalf("pgxPool.Query failed: %v", err)
		}

		for rows.Next() {
			rows.Scan(&passwordStrength)
		}

		if rows.Err() != nil {
			b.Fatalf("rows.Err(): %v", rows.Err())
		}
	}
}

func BenchmarkPgxSelectOneByteSlice(b *testing.B) {
	conn, err := pgxPool.Acquire()
	if err != nil {
		b.Fatalf("unable to acquire connection: %v", err)
	}
	defer pgxPool.Release(conn)

	psName := "selectOneByteSlice"
	_, err = pgxPool.Prepare(psName, `select password_digest from pgxbench_user where id=$1`)
	if err != nil {
		b.Fatalf("unable to prepare query: %v", err)
	}
	defer pgxPool.Deallocate(psName)

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		id := randUserIDs[i%len(randUserIDs)]
		var passwordDigest []byte
		err := pgxPool.QueryRow(psName, id).Scan(&passwordDigest)
		if err != nil {
			b.Fatalf("pgxPool.QueryRow Scan failed: %v", err)
		}

		// Simple check to ensure data was actually read
		if len(passwordDigest) == 0 {
			b.Fatal("passwordDigest was blank")
		}
	}
}
