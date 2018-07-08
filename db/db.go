package db

import (
	"database/sql"
	"fmt"
	"strings"
	"time"

	// import the postgres database driver
	_ "github.com/lib/pq"
)

const MosmixSSchemaName = "mosmix_s"
const MosmixLSchemaName = "mosmix_l"

type MosmixDB struct {
	db                  *sql.DB
	ProcessingTimestamp time.Time
	runIdentifier       string
	metadata            *Metadata
	schema              string
}

func NewMosmixDB(connectionString, schema string) (*MosmixDB, error) {
	fmt.Println("Connecting to database ... ")
	db, err := sql.Open("postgres", connectionString)
	if err != nil {
		return &MosmixDB{}, err
	}
	now := time.Now()
	m := &MosmixDB{db, now, now.Format("20060102150405"), &Metadata{}, schema}
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

func (m *MosmixDB) buildDropOldTablesQuery() (string, error) {
	var err error
	// query the table names to drop..
	sqlStmt := fmt.Sprintf(`SELECT table_name
	FROM information_schema.tables
	WHERE table_schema='%s'
	AND table_type='BASE TABLE'
	AND table_name ~ E'_\\d{14}$'
	AND table_name !~ '.*_%s$';`,
		m.schema,
		m.runIdentifier)
	rows, err := m.db.Query(sqlStmt)
	if err != nil {
		return "", err
	}
	defer rows.Close()

	var dropStmt strings.Builder

	for rows.Next() {
		var tableName string
		if err := rows.Scan(&tableName); err != nil {
			return "", err
		}
		dropStmt.WriteString(" DROP TABLE ")
		dropStmt.WriteString(tableName)
		dropStmt.WriteRune(';')
	}

	return dropStmt.String(), nil
}

func (m *MosmixDB) buildCrosstabFunctionQuery() (string, error) {
	// generate the list of forecast variables available
	var forecastVariables []string
	for _, fcVar := range m.metadata.AvailableVariables {
		forecastVariables = append(forecastVariables, fmt.Sprintf("%s NUMERIC(8, 2)", fcVar))
	}
	forecastVariablesArgumentsString := strings.Join(forecastVariables, ", ")

	functionSrc := fmt.Sprintf("SELECT * FROM crosstab("+
		"'SELECT timestep, place_id, name, value FROM %[1]s.forecasts "+
		"WHERE place_id = ' || quote_literal(place_id) || ' "+
		"ORDER BY timestep, place_id', "+
		"'SELECT DISTINCT(UNNEST(dwd_available_forecast_variables)) FROM %[1]s.metadata') "+
		"AS ct (timestep TIMESTAMP WITH TIME ZONE, place_id TEXT, %[2]s);",
		m.schema, forecastVariablesArgumentsString)

	replaceFunction := false
	var fnSrcInDB string
	err := m.db.QueryRow(
		fmt.Sprintf(`SELECT p.prosrc FROM pg_catalog.pg_proc p
		LEFT JOIN pg_catalog.pg_namespace n ON n.oid = p.pronamespace
		WHERE p.proname = 'forecasts_for_place_id'
		AND n.nspname OPERATOR(pg_catalog.~) '^(%s)$';`,
			m.schema,
		),
	).Scan(&fnSrcInDB)

	switch {
	case err == sql.ErrNoRows:
		// the function does not exist, just create it
		replaceFunction = true
	case err != nil:
		return "", err
	default:
		// compare the new function with the one in the database
		if strings.TrimSpace(functionSrc) != strings.TrimSpace(fnSrcInDB) {
			fmt.Printf("\nForecasts functions differ!!!\nin DB:\n%s\nnew:\n%s\n",
				strings.TrimSpace(fnSrcInDB), strings.TrimSpace(functionSrc))
			replaceFunction = true
		}
	}

	if replaceFunction == true {
		return fmt.Sprintf("DROP FUNCTION IF EXISTS forecasts_for_place_id;"+
			"CREATE FUNCTION forecasts_for_place_id(place_id TEXT) "+
			"RETURNS TABLE (timestep TIMESTAMP WITH TIME ZONE, place_id TEXT, %s) AS $$"+
			"%s "+
			"$$ LANGUAGE SQL;",
			forecastVariablesArgumentsString, functionSrc), nil
	}

	return "", nil
}

func (m *MosmixDB) createIndexes() error {
	var err error

	dropStmt, err := m.buildDropOldTablesQuery()
	if err != nil {
		return err
	}

	constraintFrom := m.ProcessingTimestamp.Add(-1 * time.Second).Format(time.RFC3339)
	constraintTo := m.ProcessingTimestamp.Add(1 * time.Second).Format(time.RFC3339)

	sqlStmt := fmt.Sprintf(`BEGIN;

	ANALYZE forecast_places_%[1]s;
	ANALYZE forecasts_%[1]s;

	ALTER TABLE forecast_places_%[1]s ADD CONSTRAINT y%[1]s
		CHECK ( processing_timestamp >= '%[2]s' AND processing_timestamp < '%[3]s' );

	CREATE INDEX IF NOT EXISTS idx_the_geom_forecast_places_%[1]s ON forecast_places_%[1]s USING GIST (the_geom);

	ALTER TABLE forecast_places_%[1]s INHERIT forecast_places;

	ALTER TABLE forecasts_%[1]s ADD CONSTRAINT y%[1]s
		CHECK ( processing_timestamp >= '%[2]s' AND processing_timestamp < '%[3]s' );

	CREATE INDEX IF NOT EXISTS idx_forecasts_place_id_name_%[1]s ON forecasts_%[1]s (place_id, name);
	CREATE INDEX IF NOT EXISTS idx_forecasts_place_id_%[1]s on forecasts_%[1]s (place_id);

	ALTER TABLE forecasts_%[1]s INHERIT forecasts;

	ALTER TABLE metadata_%[1]s ADD CONSTRAINT y%[1]s
		CHECK ( processing_timestamp >= '%[2]s' AND processing_timestamp < '%[3]s' );

	ALTER TABLE metadata_%[1]s INHERIT metadata;

	ALTER TABLE met_element_definitions_%[1]s ADD CONSTRAINT y%[1]s
		CHECK ( processing_timestamp >= '%[2]s' AND processing_timestamp < '%[3]s' );

	ALTER TABLE met_element_definitions_%[1]s INHERIT met_element_definitions;

	%[4]s

	COMMIT;`,
		m.runIdentifier, constraintFrom, constraintTo, dropStmt,
	)
	_, err = m.db.Exec(sqlStmt)
	if err != nil {
		return err
	}

	crosstabStmt, err := m.buildCrosstabFunctionQuery()
	if err != nil {
		return err
	}
	// create the function after metadata partion has been switched..
	_, err = m.db.Exec(crosstabStmt)
	if err != nil {
		return err
	}

	return nil
}

func (m *MosmixDB) createTables() error {
	// create schema and switch to it
	_, err := m.db.Exec(fmt.Sprintf(`BEGIN;

	DO $$
	BEGIN
		IF NOT EXISTS (SELECT FROM pg_type WHERE typname = 'dwd_referenced_model') THEN
			CREATE TYPE dwd_referenced_model AS (
				name TEXT,
				reference_time TIMESTAMP WITH TIME ZONE
			);
		END IF;
	END$$;

	CREATE EXTENSION IF NOT EXISTS tablefunc;

	CREATE SCHEMA IF NOT EXISTS %[1]s;

	SET search_path TO %[1]s, public;

	COMMIT`, m.schema))
	if err != nil {
		return err
	}

	_, err = m.db.Exec(`BEGIN;

	CREATE UNLOGGED TABLE IF NOT EXISTS metadata(
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

	CREATE UNLOGGED TABLE IF NOT EXISTS forecast_places(
		id TEXT NOT NULL,
		name TEXT NOT NULL,
		the_geom geometry(PointZ,4326) NOT NULL,
		processing_timestamp TIMESTAMP WITH TIME ZONE NOT NULL
	);

	CREATE UNLOGGED TABLE IF NOT EXISTS forecasts(
		place_id TEXT NOT NULL,
		name TEXT NOT NULL,
		timestep TIMESTAMP WITH TIME ZONE NOT NULL,
		value NUMERIC(8, 2) NOT NULL,
		processing_timestamp TIMESTAMP WITH TIME ZONE NOT NULL
	);

	CREATE UNLOGGED TABLE IF NOT EXISTS met_element_definitions(
		description TEXT NOT NULL,
		unit_of_measurement TEXT NOT NULL,
		short_name TEXT NOT NULL,
		processing_timestamp TIMESTAMP WITH TIME ZONE NOT NULL
	);

	SET synchronous_commit TO off;

	COMMIT;
	`)
	if err != nil {
		return err
	}

	_, err = m.db.Exec(fmt.Sprintf(`BEGIN;

	CREATE UNLOGGED TABLE forecast_places_%[1]s
		(LIKE forecast_places INCLUDING DEFAULTS INCLUDING CONSTRAINTS);
	CREATE UNLOGGED TABLE forecasts_%[1]s
		(LIKE forecasts INCLUDING DEFAULTS INCLUDING CONSTRAINTS);
	CREATE UNLOGGED TABLE metadata_%[1]s
		(LIKE metadata INCLUDING DEFAULTS INCLUDING CONSTRAINTS);
	CREATE UNLOGGED TABLE met_element_definitions_%[1]s
		(LIKE met_element_definitions INCLUDING DEFAULTS INCLUDING CONSTRAINTS);

	COMMIT;
	`, m.runIdentifier))
	if err != nil {
		return err
	}

	return nil
}
