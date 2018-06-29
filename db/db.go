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

func (m *MosmixDB) createIndexes() error {
	var err error
	sqlStmt := `BEGIN;

--	CREATE INDEX IF NOT EXISTS idx_the_geom_forecast_places ON forecast_places_temp USING GIST (the_geom);
--
--	CREATE INDEX IF NOT EXISTS idx_forecasts_place_id_name ON forecasts (place_id, name);
--	CREATE INDEX IF NOT EXISTS idx_forecasts_name_timestep_place_id on forecasts (name, timestep, place_id);
--
--	ALTER TABLE forecast_places RENAME TO forecast_places;
--	ALTER TABLE forecasts RENAME TO forecasts;
--	ALTER TABLE dwd_referenced_models RENAME TO dwd_referenced_models;
--	ALTER TABLE dwd_available_timesteps RENAME TO dwd_available_timesteps;
--	ALTER TABLE dwd_available_forecast_variables RENAME TO dwd_available_forecast_variables;
--	ALTER TABLE metadata RENAME TO metadata;
--
--	DROP VIEW IF EXISTS places;
--	CREATE VIEW places AS SELECT id, name, ST_X(the_geom) AS lng, ST_Y(the_geom) AS lat, ST_Z(the_geom) AS alt, the_geom from forecast_places;

	COMMIT;`
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
		processing_time TIMESTAMP WITH TIME ZONE NOT NULL,
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
