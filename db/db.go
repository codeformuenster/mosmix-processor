package db

import (
	"database/sql"
	"fmt"
	"time"

	// import the postgres database driver
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

func (m *MosmixDB) Finalize() error {
	fmt.Print("Creating indexes ... ")
	start := time.Now()
	err := m.createIndexes()
	if err != nil {
		return err
	}
	duration := time.Now().Sub(start)
	fmt.Printf("done in %s\n", duration)

	return nil
}

func (m *MosmixDB) Close() error {
	return m.db.Close()
}

func (m *MosmixDB) createIndexes() error {
	var err error
	sqlStmt := `BEGIN;

	ANALYZE forecast_places_temp;
	ANALYZE forecasts_temp;

	CREATE INDEX IF NOT EXISTS idx_the_geom_forecast_places_temp ON forecast_places_temp USING GIST (the_geom);

	CREATE INDEX IF NOT EXISTS idx_forecasts_place_id_name_temp ON forecasts_temp (place_id, name);
	CREATE INDEX IF NOT EXISTS idx_forecasts_name_timestep_place_id_temp on forecasts_temp (name, timestep, place_id);

	ALTER TABLE IF EXISTS forecast_places RENAME TO forecast_places_old;
	ALTER TABLE IF EXISTS forecasts RENAME TO forecasts_old;
	ALTER TABLE IF EXISTS dwd_referenced_models RENAME TO dwd_referenced_models_old;
	ALTER TABLE IF EXISTS dwd_available_timesteps RENAME TO dwd_available_timesteps_old;
	ALTER TABLE IF EXISTS dwd_available_forecast_variables RENAME TO dwd_available_forecast_variables_old;
	ALTER TABLE IF EXISTS metadata RENAME TO metadata_old;

	ALTER TABLE forecast_places_temp RENAME TO forecast_places;
	ALTER TABLE forecasts_temp RENAME TO forecasts;
	ALTER TABLE dwd_referenced_models_temp RENAME TO dwd_referenced_models;
	ALTER TABLE dwd_available_timesteps_temp RENAME TO dwd_available_timesteps;
	ALTER TABLE dwd_available_forecast_variables_temp RENAME TO dwd_available_forecast_variables;
	ALTER TABLE metadata_temp RENAME TO metadata;

	CREATE OR REPLACE VIEW places AS SELECT id, name, ST_X(the_geom) AS lng, ST_Y(the_geom) AS lat, ST_Z(the_geom) AS alt, the_geom from forecast_places;

	COMMIT;`
	_, err = m.db.Exec(sqlStmt)
	if err != nil {
		return err
	}

	err = m.CreateForecastsAllView()
	if err != nil {
		return err
	}
	sqlStmt = `BEGIN;

	DROP TABLE IF EXISTS forecast_places_old;
	DROP TABLE IF EXISTS forecasts_old;
	DROP TABLE IF EXISTS dwd_referenced_models_old;
	DROP TABLE IF EXISTS dwd_available_timesteps_old;
	DROP TABLE IF EXISTS dwd_available_forecast_variables_old;
	DROP TABLE IF EXISTS metadata_old;

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

	DROP TABLE IF EXISTS forecast_places_temp;
	DROP TABLE IF EXISTS forecasts_temp;
	DROP TABLE IF EXISTS dwd_referenced_models_temp;
	DROP TABLE IF EXISTS dwd_available_timesteps_temp;
	DROP TABLE IF EXISTS dwd_available_forecast_variables_temp;
	DROP TABLE IF EXISTS metadata_temp;
	DROP TABLE IF EXISTS forecast_places_old;
	DROP TABLE IF EXISTS forecasts_old;
	DROP TABLE IF EXISTS dwd_referenced_models_old;
	DROP TABLE IF EXISTS dwd_available_timesteps_old;
	DROP TABLE IF EXISTS dwd_available_forecast_variables_old;
	DROP TABLE IF EXISTS metadata_old;

	CREATE TABLE dwd_referenced_models_temp(
		name TEXT NOT NULL,
		reference_time TIMESTAMP WITH TIME ZONE NOT NULL
	);

	CREATE TABLE dwd_available_timesteps_temp(
		timestep TIMESTAMP WITH TIME ZONE NOT NULL
	);

	CREATE TABLE dwd_available_forecast_variables_temp(
		name TEXT NOT NULL
	);

	CREATE TABLE metadata_temp(
		source_url TEXT NOT NULL,
		processing_time TIMESTAMP WITH TIME ZONE NOT NULL,
		download_duration REAL NOT NULL,
		parsing_duration REAL NOT NULL,
		parser TEXT NOT NULL,
		dwd_issuer TEXT NOT NULL,
		dwd_product_id TEXT NOT NULL,
		dwd_generating_process TEXT NOT NULL
	);

	CREATE TABLE forecast_places_temp(
		id TEXT PRIMARY KEY,
		name TEXT NOT NULL,
		the_geom geometry(PointZ,4326) NOT NULL
	);
	ALTER TABLE forecast_places_temp SET UNLOGGED;

	CREATE TABLE forecasts_temp(
		place_id TEXT NOT NULL,
		name TEXT NOT NULL,
		timestep TIMESTAMP WITH TIME ZONE NOT NULL,
		value REAL NOT NULL
	);
	ALTER TABLE forecasts_temp SET UNLOGGED;

	SET synchronous_commit TO off;

	COMMIT;
	`
	_, err = m.db.Exec(sqlStmt)
	if err != nil {
		return err
	}

	return nil
}
