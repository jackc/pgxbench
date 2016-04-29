package pgxbench

import (
	"github.com/jackc/pgx"
)

func extractConfig() (config pgx.ConnPoolConfig, err error) {
	config.ConnConfig, err = pgx.ParseEnvLibpq()
	if err != nil {
		return config, err
	}

	if config.Host == "" {
		config.Host = "localhost"
	}

	if config.User == "" {
		config.User = os.Getenv("USER")
	}

	if config.Database == "" {
		config.Database = "pgxbench"
	}

	config.TLSConfig = nil
	config.UseFallbackTLS = false

	config.MaxConnections = 10

	return config, nil
}
