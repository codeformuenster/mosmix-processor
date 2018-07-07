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
	runIdentifier       string
	metadata            *Metadata
}

func NewMosmixDB(connectionString string) (*MosmixDB, error) {
	fmt.Println("Connecting to database ... ")
	db, err := sql.Open("postgres", connectionString)
	if err != nil {
		return &MosmixDB{}, err
	}
	now := time.Now()
	m := &MosmixDB{db, now, now.Format("20060102150405"), &Metadata{}}
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
	WHERE table_schema='public'
	AND table_type='BASE TABLE'
	AND table_name ~ E'_\\d{14}$'
	AND table_name !~ '.*_%s$';`,
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

	functionSrc := fmt.Sprintf("SELECT * FROM crosstab('SELECT timestep, place_id, name, value FROM forecasts WHERE place_id = ' || quote_literal(place_id) || ' ORDER BY timestep, place_id', 'SELECT DISTINCT(UNNEST(dwd_available_forecast_variables)) FROM metadata') AS ct (timestep TIMESTAMP WITH TIME ZONE, place_id TEXT, %s);", forecastVariablesArgumentsString)

	replaceFunction := false
	var fnSrcInDB string
	err := m.db.QueryRow("SELECT prosrc FROM pg_proc WHERE proname = 'forecasts_for_place_id';").Scan(&fnSrcInDB)

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
				strings.TrimSpace(functionSrc), strings.TrimSpace(fnSrcInDB))
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

	ANALYZE forecast_places_%s;
	ANALYZE forecasts_%s;

	ALTER TABLE forecast_places_%s ADD CONSTRAINT y%s
		CHECK ( processing_timestamp >= '%s' AND processing_timestamp < '%s' );

	CREATE INDEX IF NOT EXISTS idx_the_geom_forecast_places_%s ON forecast_places_%s USING GIST (the_geom);

	ALTER TABLE forecast_places_%s INHERIT forecast_places;

	ALTER TABLE forecasts_%s ADD CONSTRAINT y%s
		CHECK ( processing_timestamp >= '%s' AND processing_timestamp < '%s' );

	CREATE INDEX IF NOT EXISTS idx_forecasts_place_id_name_%s ON forecasts_%s (place_id, name);
	CREATE INDEX IF NOT EXISTS idx_forecasts_place_id_%s on forecasts_%s (place_id);

	ALTER TABLE forecasts_%s INHERIT forecasts;

	ALTER TABLE metadata_%s ADD CONSTRAINT y%s
		CHECK ( processing_timestamp >= '%s' AND processing_timestamp < '%s' );

	ALTER TABLE metadata_%s INHERIT metadata;

	ALTER TABLE met_element_definitions_%s ADD CONSTRAINT y%s
		CHECK ( processing_timestamp >= '%s' AND processing_timestamp < '%s' );

	ALTER TABLE met_element_definitions_%s INHERIT met_element_definitions;

	%s

	COMMIT;`,
		m.runIdentifier,
		m.runIdentifier,

		m.runIdentifier, m.runIdentifier,
		constraintFrom, constraintTo,

		m.runIdentifier, m.runIdentifier,

		m.runIdentifier,

		m.runIdentifier, m.runIdentifier,
		constraintFrom, constraintTo,

		m.runIdentifier, m.runIdentifier,
		m.runIdentifier, m.runIdentifier,

		m.runIdentifier,

		m.runIdentifier, m.runIdentifier,
		constraintFrom, constraintTo,

		m.runIdentifier,

		m.runIdentifier, m.runIdentifier,
		constraintFrom, constraintTo,

		m.runIdentifier,

		dropStmt,
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
	var err error
	sqlStmt := `BEGIN;

	CREATE EXTENSION IF NOT EXISTS tablefunc;

	DO $$
	BEGIN
		IF NOT EXISTS (SELECT FROM pg_type WHERE typname = 'dwd_referenced_model') THEN
			CREATE TYPE dwd_referenced_model AS (
				name TEXT,
				reference_time TIMESTAMP WITH TIME ZONE
			);
		END IF;
	END$$;

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
	`
	_, err = m.db.Exec(sqlStmt)
	if err != nil {
		return err
	}

	sqlStmt = fmt.Sprintf(`BEGIN;

	CREATE UNLOGGED TABLE forecast_places_%s
		(LIKE forecast_places INCLUDING DEFAULTS INCLUDING CONSTRAINTS);
	CREATE UNLOGGED TABLE forecasts_%s
		(LIKE forecasts INCLUDING DEFAULTS INCLUDING CONSTRAINTS);
	CREATE UNLOGGED TABLE metadata_%s
		(LIKE metadata INCLUDING DEFAULTS INCLUDING CONSTRAINTS);
	CREATE UNLOGGED TABLE met_element_definitions_%s
		(LIKE met_element_definitions INCLUDING DEFAULTS INCLUDING CONSTRAINTS);

	COMMIT;
	`, m.runIdentifier, m.runIdentifier, m.runIdentifier, m.runIdentifier)
	_, err = m.db.Exec(sqlStmt)
	if err != nil {
		return err
	}

	return nil
}
