package producer

import (
	"database/sql"
	"fmt"
	"os"
)

var (
	PgUser     string
	PgPassword string
	PgDatabase string
	PgHost     string
	PgPort     = 5432
)

func init() {
	PgUser = os.Getenv("PGUSER")
	PgPassword = os.Getenv("PGPASSWORD")
	PgDatabase = os.Getenv("PGDATABASE")
	PgHost = os.Getenv("PGHOST")
	if port := os.Getenv("PGPORT"); port != "" {
		fmt.Sscanf(port, "%d", &PgPort)
	}
}

func Connect() (*sql.DB, error) {
	connStr := fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s sslmode=disable",
		PgHost, PgPort, PgUser, PgPassword, PgDatabase)

	db, err := sql.Open("postgres", connStr)
	if err != nil {
		return nil, fmt.Errorf("error opening database connection: %w", err)
	}
	return db, nil
}
