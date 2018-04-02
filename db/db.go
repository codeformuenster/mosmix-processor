package db

import (
	"database/sql"
	"os"

	_ "github.com/shaxbee/go-spatialite"
)

type MosmixDB struct {
	db *sql.DB
}

func NewMosmixDB(filename string) (*MosmixDB, error) {
	os.Remove(filename)

	db, err := sql.Open("spatialite", filename)
	if err != nil {
		return &MosmixDB{}, err
	}

	m := &MosmixDB{db: db}
	err = m.createTables()
	if err != nil {
		return &MosmixDB{}, err
	}

	return m, nil
}

func (m *MosmixDB) Close() error {
	return m.db.Close()
}

func (m *MosmixDB) createTables() error {
	_, err := m.db.Exec("SELECT InitSpatialMetadata(1, 'WGS84')")
	if err != nil {
		return err
	}
	sqlStmt := `BEGIN;
	CREATE TABLE metadata(
		json TEXT NOT NULL
	);
	COMMIT;
	BEGIN;
	CREATE TABLE forecast_places(
		id TEXT PRIMARY KEY,
		name TEXT NOT NULL
	);
	SELECT AddGeometryColumn('forecast_places', 'the_geom', 4326, 'POINTZ', 'XYZ', 1);
	SELECT CreateSpatialIndex('forecast_places', 'the_geom');
	COMMIT;
	BEGIN;
	CREATE TABLE forecasts(
		place_id TEXT NOT NULL,
		name TEXT NOT NULL,
		timestep TEXT NOT NULL,
		value REAL NOT NULL
	);
	CREATE INDEX idx_forecasts_place_id_name ON forecasts (place_id, name);
	COMMIT;`
	_, err = m.db.Exec(sqlStmt)
	if err != nil {
		return err
	}
	_, err = m.db.Exec("PRAGMA synchronous=OFF;")
	if err != nil {
		return err
	}

	_, err = m.db.Exec("PRAGMA journal_mode=OFF;")
	if err != nil {
		return err
	}

	return nil
}
