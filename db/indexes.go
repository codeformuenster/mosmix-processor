package db

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/tidwall/buntdb"
)

const (
	Locations        = "l"
	LocationsPattern = "f:%v:loc"
	Names            = "n"
	NamesPattern     = "f:%v:name"

	ForecastVariableTimesteps        = "fv_%v_%v"
	ForecastVariableTimestepsPattern = "f:%v:%v:%v" // "forecasts:001:PPPP:1522521520"
)

var indexOpts = buntdb.IndexOptions{CaseInsensitiveKeyMatching: false}

func (m *MosmixDB) createIndexes() error {
	return m.db.Update(func(tx *buntdb.Tx) error {
		// Geo index
		err := tx.CreateSpatialIndexOptions(Locations, fmt.Sprintf(LocationsPattern, "*"), &indexOpts, indexKMLCoordinates)
		if err != nil {
			return err
		}
		// Index Placenames
		err = tx.CreateIndexOptions(Names, fmt.Sprintf(NamesPattern, "*"), &indexOpts, buntdb.IndexString)
		if err != nil {
			return err
		}

		return nil
	})
}

func (m *MosmixDB) createForecastIndex(tx *buntdb.Tx, placeID, forecastVariable string) error {
	// fmt.Printf("Creating index \"%s\": %s (%s)\n", fmt.Sprintf(ForecastVariableTimesteps, placeID, forecastVariable), fmt.Sprintf(ForecastVariableTimestepsPattern, placeID, forecastVariable, "*"), ForecastVariableTimestepsPattern)
	return tx.CreateIndexOptions(fmt.Sprintf(ForecastVariableTimesteps, placeID, forecastVariable), fmt.Sprintf(ForecastVariableTimestepsPattern, placeID, forecastVariable, "*"), &indexOpts, buntdb.IndexString)
}

// convert kml coordinates (lon,lat) to buntdb coordinates (lat,lon)
func indexKMLCoordinates(str string) (min, max []float64) {
	parts := strings.Split(str, ",")
	if len(parts) == 2 || len(parts) == 3 {

		var lon, lat float64

		lon, err := strconv.ParseFloat(parts[0], 64)
		if err != nil {
			fmt.Printf("Coordinate %v of coordinates %v cannot be parsed as float", parts[0], parts)
		}
		lat, err = strconv.ParseFloat(parts[1], 64)
		if err != nil {
			fmt.Printf("Coordinate %v of coordinates %v cannot be parsed as float", parts[1], parts)
		}

		if len(parts) != 2 {
			altitude, err := strconv.ParseFloat(parts[2], 64)
			if err != nil {
				fmt.Printf("Coordinate %v of coordinates %v cannot be parsed as float", parts[2], parts)
			}
			return []float64{lat, lon, altitude}, []float64{lat, lon, altitude}
		}

		return []float64{lat, lon}, []float64{lat, lon}
	}

	return []float64{}, []float64{}
}
