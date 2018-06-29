package db

import (
	"database/sql"
	"fmt"
	"strings"
	"time"

	// import the postgres database driver
	_ "github.com/lib/pq"
)

type MosmixDB struct {
	db                  *sql.DB
	ProcessingTimestamp time.Time
	RunIdentifier       string
}

func NewMosmixDB(connectionString string) (*MosmixDB, error) {
	fmt.Println("Connecting to database ... ")
	db, err := sql.Open("postgres", connectionString)
	if err != nil {
		return &MosmixDB{}, err
	}
	now := time.Now()
	m := &MosmixDB{db, now, now.Format("20060102150405")}
	fmt.Print("Preparing tables ... ")
	start := time.Now()
	err = m.createTables()
	if err != nil {
		return &MosmixDB{}, err
	}
	fmt.Printf("done in %s\n", time.Now().Sub(start))

	return m, nil
}

func (m *MosmixDB) Finalize() error {
	fmt.Print("Creating indexes ... ")
	start := time.Now()
	err := m.createIndexes()
	if err != nil {
		return err
	}
	fmt.Printf("done in %s\n", time.Now().Sub(start))

	return nil
}

func (m *MosmixDB) Close() error {
	return m.db.Close()
}

// SELECT table_name
//   FROM information_schema.tables
//  WHERE table_schema='public'
//    AND table_type='BASE TABLE' and (table_name LIKE 'forecasts_%' or table_name LIKE 'forecast_places_%');

func (m *MosmixDB) createIndexes() error {
	var err error
	// query the table names to drop..
	sqlStmt := fmt.Sprintf(`SELECT table_name
	FROM information_schema.tables
	WHERE table_schema='public'
	AND table_type='BASE TABLE'
	AND (table_name LIKE 'forecasts_%%' OR table_name LIKE 'forecast_places_%%')
	AND table_name != 'forecasts_%s' AND table_name != 'forecast_places_%s';`,
		m.RunIdentifier, m.RunIdentifier)
	rows, err := m.db.Query(sqlStmt)
	if err != nil {
		return err
	}
	defer rows.Close()

	var dropStmt strings.Builder

	for rows.Next() {
		var tableName string
		if err := rows.Scan(&tableName); err != nil {
			return err
		}
		dropStmt.WriteString(" DROP TABLE ")
		dropStmt.WriteString(tableName)
		dropStmt.WriteRune(';')
	}

	sqlStmt = fmt.Sprintf(`BEGIN;

	ANALYZE forecast_places_%s;
	ANALYZE forecasts_%s;

	ALTER TABLE forecast_places_%s ADD CONSTRAINT y%s
		CHECK ( processing_timestamp >= '%s' AND processing_timestamp < '%s' );
	--COPY forecast_places_%s FROM 'forecast_places_%s';

	CREATE INDEX IF NOT EXISTS idx_the_geom_forecast_places_%s ON forecast_places_%s USING GIST (the_geom);

	ALTER TABLE forecast_places_%s INHERIT forecast_places;

	ALTER TABLE forecasts_%s ADD CONSTRAINT y%s
		CHECK ( processing_timestamp >= '%s' AND processing_timestamp < '%s' );
	--COPY forecasts_%s FROM 'forecasts_%s';

	CREATE INDEX IF NOT EXISTS idx_forecasts_place_id_name_%s ON forecasts_%s (place_id, name);
	CREATE INDEX IF NOT EXISTS idx_forecasts_name_timestep_place_id_%s on forecasts_%s (name, timestep, place_id);

	ALTER TABLE forecasts_%s INHERIT forecasts;

	%s

	COMMIT;`,
		m.RunIdentifier,
		m.RunIdentifier,
		m.RunIdentifier, m.RunIdentifier,
		m.ProcessingTimestamp.Add(-1*time.Second).Format(time.RFC3339), m.ProcessingTimestamp.Add(1*time.Second).Format(time.RFC3339),
		m.RunIdentifier, m.RunIdentifier,
		m.RunIdentifier, m.RunIdentifier,
		m.RunIdentifier,
		m.RunIdentifier, m.RunIdentifier,
		m.ProcessingTimestamp.Add(-1*time.Second).Format(time.RFC3339), m.ProcessingTimestamp.Add(1*time.Second).Format(time.RFC3339),
		m.RunIdentifier, m.RunIdentifier,
		m.RunIdentifier, m.RunIdentifier,
		m.RunIdentifier, m.RunIdentifier,
		m.RunIdentifier,
		dropStmt.String(),
	)
	_, err = m.db.Exec(sqlStmt)
	if err != nil {
		return err
	}

	// err = m.CreateForecastsAllView()
	// if err != nil {
	// 	return err
	// }
	// sqlStmt = `BEGIN;

	// DROP TABLE IF EXISTS forecast_places_old;
	// DROP TABLE IF EXISTS forecasts_old;
	// DROP TABLE IF EXISTS dwd_referenced_models_old;
	// DROP TABLE IF EXISTS dwd_available_timesteps_old;
	// DROP TABLE IF EXISTS dwd_available_forecast_variables_old;
	// DROP TABLE IF EXISTS metadata_old;

	// COMMIT;`
	// _, err = m.db.Exec(sqlStmt)
	// if err != nil {
	// 	return err
	// }

	return nil
}

func (m *MosmixDB) createTables() error {
	var err error
	sqlStmt := `BEGIN;

	DO $$
	BEGIN
		IF NOT EXISTS (SELECT FROM pg_type WHERE typname = 'dwd_referenced_model') THEN
			CREATE TYPE dwd_referenced_model AS (
				name TEXT,
				reference_time TIMESTAMP WITH TIME ZONE
			);
		END IF;
	END$$;

	CREATE TABLE IF NOT EXISTS metadata(
		source_url TEXT NOT NULL,
		processing_timestamp TIMESTAMP WITH TIME ZONE NOT NULL,
		download_duration REAL NOT NULL,
		parsing_duration REAL NOT NULL,
		parser TEXT NOT NULL,
		dwd_issuer TEXT NOT NULL,
		dwd_product_id TEXT NOT NULL,
		dwd_generating_process TEXT NOT NULL,
		dwd_available_forecast_variables TEXT[] NOT NULL,
		dwd_available_timesteps TIMESTAMP WITH TIME ZONE[] NOT NULL,
		dwd_referenced_models dwd_referenced_model[] NOT NULL
	);

	CREATE TABLE IF NOT EXISTS forecast_places(
		id TEXT NOT NULL,
		name TEXT NOT NULL,
		the_geom geometry(PointZ,4326) NOT NULL,
		processing_timestamp TIMESTAMP WITH TIME ZONE NOT NULL
	);

	CREATE TABLE IF NOT EXISTS forecasts(
		place_id TEXT NOT NULL,
		name TEXT NOT NULL,
		timestep TIMESTAMP WITH TIME ZONE NOT NULL,
		value REAL NOT NULL,
		processing_timestamp TIMESTAMP WITH TIME ZONE NOT NULL
	);

	SET synchronous_commit TO off;

	DO $$
	BEGIN
		IF NOT EXISTS (SELECT FROM pg_views WHERE viewname = 'places') THEN
			CREATE VIEW places AS
				SELECT id, name, ST_X(the_geom) AS lng, ST_Y(the_geom) AS lat, ST_Z(the_geom) AS alt, the_geom from forecast_places;
		END IF;
	END$$;


	COMMIT;
	`
	_, err = m.db.Exec(sqlStmt)
	if err != nil {
		return err
	}

	sqlStmt = fmt.Sprintf(`BEGIN;

	CREATE TABLE forecast_places_%s
		(LIKE forecast_places INCLUDING DEFAULTS INCLUDING CONSTRAINTS);
	ALTER TABLE forecast_places_%s SET UNLOGGED;
	CREATE TABLE forecasts_%s
		(LIKE forecasts INCLUDING DEFAULTS INCLUDING CONSTRAINTS);
	ALTER TABLE forecasts_%s SET UNLOGGED;

	COMMIT;
	`, m.RunIdentifier, m.RunIdentifier, m.RunIdentifier, m.RunIdentifier)
	_, err = m.db.Exec(sqlStmt)
	if err != nil {
		return err
	}

	return nil
}
