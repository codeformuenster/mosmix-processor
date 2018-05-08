package db

import (
	"database/sql"
	"fmt"
	"time"

	_ "github.com/lib/pq"
)

type MosmixDB struct {
	db *sql.DB
}

func NewMosmixDB(connectionString string) (*MosmixDB, error) {
	fmt.Println("Connecting to database ... ")
	db, err := sql.Open("postgres", connectionString)
	if err != nil {
		return &MosmixDB{}, err
	}

	m := &MosmixDB{db}
	fmt.Print("Preparing tables ... ")
	start := time.Now()
	err = m.createTables()
	duration := time.Now().Sub(start)
	fmt.Printf("done in %s\n", duration)
	if err != nil {
		return &MosmixDB{}, err
	}

	return m, nil
}

func (m *MosmixDB) Close() error {
	fmt.Print("Creating indexes ... ")
	start := time.Now()
	err := m.createIndexes()
	if err != nil {
		return err
	}
	duration := time.Now().Sub(start)
	fmt.Printf("done in %s\n", duration)
	return m.db.Close()
}

func (m *MosmixDB) createIndexes() error {
	var err error
	sqlStmt := `BEGIN;

	ANALYZE forecast_places;
	ANALYZE forecasts;

	CREATE INDEX IF NOT EXISTS idx_the_geom_forecast_places ON forecast_places USING GIST (the_geom);
	CREATE OR REPLACE VIEW places AS SELECT id, name, ST_X(the_geom) AS lng, ST_Y(the_geom) AS lat, ST_Z(the_geom) AS alt, the_geom from forecast_places;

	CREATE INDEX IF NOT EXISTS idx_forecasts_place_id_name ON forecasts (place_id, name);
	CREATE INDEX IF NOT EXISTS idx_forecasts_name_timestep_place_id on forecasts(name, timestep, place_id);

	ALTER TABLE forecast_places SET LOGGED;
	ALTER TABLE forecasts SET LOGGED;

	COMMIT;`
	_, err = m.db.Exec(sqlStmt)
	if err != nil {
		return err
	}

	return nil
}

func (m *MosmixDB) createTables() error {
	var err error
	sqlStmt := `BEGIN;
	CREATE TABLE IF NOT EXISTS dwd_referenced_models(
		name TEXT NOT NULL,
		reference_time TIMESTAMP WITH TIME ZONE NOT NULL
	);

	CREATE TABLE IF NOT EXISTS dwd_available_timesteps(
		timestep TIMESTAMP WITH TIME ZONE NOT NULL
	);

	CREATE TABLE IF NOT EXISTS dwd_available_forecast_variables(
		name TEXT NOT NULL
	);

	CREATE TABLE IF NOT EXISTS metadata(
		source_url TEXT NOT NULL,
		processing_time TIMESTAMP WITH TIME ZONE NOT NULL,
		download_duration REAL NOT NULL,
		parsing_duration REAL NOT NULL,
		parser TEXT NOT NULL,
		dwd_issuer TEXT NOT NULL,
		dwd_product_id TEXT NOT NULL,
		dwd_generating_process TEXT NOT NULL
	);

	CREATE TABLE IF NOT EXISTS forecast_places(
		id TEXT PRIMARY KEY,
		name TEXT NOT NULL,
		the_geom geometry(PointZ,4326) NOT NULL
	);
	ALTER TABLE forecast_places SET UNLOGGED;

	CREATE TABLE IF NOT EXISTS forecasts(
		place_id TEXT NOT NULL,
		name TEXT NOT NULL,
		timestep TIMESTAMP WITH TIME ZONE NOT NULL,
		value REAL NOT NULL
	);
	ALTER TABLE forecasts SET UNLOGGED;

	SET synchronous_commit TO off;

	COMMIT;
	`
	_, err = m.db.Exec(sqlStmt)
	if err != nil {
		return err
	}

	return nil
}
