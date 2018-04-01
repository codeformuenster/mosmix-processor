package db

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/tidwall/buntdb"
)

type Metadata struct {
	ForecastTimeSteps []string `xml:"https://opendata.dwd.de/weather/lib/pointforecast_dwd_extension_V1_0.xsd ForecastTimeSteps>TimeStep" json:"-"`
	DefaultUndefSign  string   `xml:"https://opendata.dwd.de/weather/lib/pointforecast_dwd_extension_V1_0.xsd FormatCfg>DefaultUndefSign,omitempty" json:"-"`
	GeneratingProcess string   `xml:"https://opendata.dwd.de/weather/lib/pointforecast_dwd_extension_V1_0.xsd GeneratingProcess,omitempty" json:"generatingProcess"`
	Issuer            string   `xml:"https://opendata.dwd.de/weather/lib/pointforecast_dwd_extension_V1_0.xsd Issuer,omitempty" json:"issuer,omitempty"`
	ProductID         string   `xml:"https://opendata.dwd.de/weather/lib/pointforecast_dwd_extension_V1_0.xsd ProductID,omitempty" json:"productID,omitempty"`
	ReferencedModels  []struct {
		Name          string    `xml:"https://opendata.dwd.de/weather/lib/pointforecast_dwd_extension_V1_0.xsd name,attr" json:"name"`
		ReferenceTime time.Time `xml:"https://opendata.dwd.de/weather/lib/pointforecast_dwd_extension_V1_0.xsd referenceTime,attr" json:"referenceTime"`
	} `xml:"https://opendata.dwd.de/weather/lib/pointforecast_dwd_extension_V1_0.xsd ReferencedModel>Model" json:"referencedModels"`
	ProcessingTime   time.Time     `json:"processingTime"`
	DownloadDuration time.Duration `json:"downloadDuration"`
	ParsingDuration  time.Duration `json:"processingDuration"`
	SourceURL        string        `json:"sourceURL"`
	// IssueTime is empty?!
	// IssueTime       *time.Time `xml:"https://opendata.dwd.de/weather/lib/pointforecast_dwd_extension_V1_0.xsd IssueTime,omitempty"`   // ZZmaxLength=0
}

type ForecastPlace struct {
	ForecastVariables []struct {
		Name  string `xml:"https://opendata.dwd.de/weather/lib/pointforecast_dwd_extension_V1_0.xsd elementName,attr"`
		Value string `xml:"https://opendata.dwd.de/weather/lib/pointforecast_dwd_extension_V1_0.xsd value"`
	} `xml:"ExtendedData>Forecast" json:"-"` // Ignore namespace, because I don't know how to write this with namespace
	ProcessedForecastVariables []*ProcessedForecastVariable `json:"forecastVariables"`
	Coordinates                string                       `xml:"http://www.opengis.net/kml/2.2 Point>coordinates" json:"-"`
	Name                       string                       `xml:"http://www.opengis.net/kml/2.2 description"`
	ID                         string                       `xml:"http://www.opengis.net/kml/2.2 name"`
	Geometry                   struct {
		Type        string    `json:"type"`
		Coordinates []float64 `json:"coordinates"`
	} `json:"geometry"`
}

type ProcessedForecastVariable struct {
	Name      string                      `json:"name"`
	Timesteps []ProcessedForecastTimestep `json:"timesteps"`
}

type ProcessedForecastTimestep struct {
	Timestep string `json:"timestep"`
	Value    string `json:"value"`
}

// InsertMetadata creates a JSON document in the database under the metadata key
func (m *MosmixDB) InsertMetadata(metadata *Metadata) error {
	return m.db.Update(func(tx *buntdb.Tx) error {
		jsonStr, err := json.Marshal(metadata)
		if err != nil {
			return err
		}

		_, _, err = tx.Set("metadata", string(jsonStr), nil)
		if err != nil {
			return err
		}

		return nil
	})
}

func (m *MosmixDB) GetMetadata() (string, error) {
	var jsonStr string
	err := m.db.View(func(tx *buntdb.Tx) error {
		res, err := tx.Get("metadata")
		if err != nil {
			return err
		}
		jsonStr = res

		return nil
	})
	if err != nil {
		return "", err
	}
	return jsonStr, err
}

func (m *MosmixDB) InsertForecast(forecast *ForecastPlace) error {
	return m.db.Update(func(tx *buntdb.Tx) error {
		_, _, err := tx.Set(fmt.Sprintf(LocationsPattern, forecast.ID), forecast.Coordinates, nil)
		if err != nil {
			return err
		}
		_, _, err = tx.Set(fmt.Sprintf(NamesPattern, forecast.ID), forecast.Name, nil)
		if err != nil {
			return err
		}

		// for each variable
		for _, forecastVariable := range forecast.ProcessedForecastVariables {
			// create an index
			err := m.createForecastIndex(tx, forecast.ID, forecastVariable.Name)
			if err != nil {
				return err
			}
			// insert the timesteps
			for _, timestep := range forecastVariable.Timesteps {
				_, _, err = tx.Set(fmt.Sprintf(ForecastVariableTimestepsPattern, forecast.ID, forecastVariable.Name, timestep.Timestep), timestep.Value, nil)
				if err != nil {
					return err
				}
			}
		}

		return nil
	})
}

func (m *MosmixDB) GetForecastsAround(lonLat string, maxDistMeters float64) error {
	inputCoordinate, _ := indexKMLCoordinates(lonLat)
	return m.db.View(func(tx *buntdb.Tx) error {
		var ids []string
		err := tx.Nearby(Locations, lonLat, func(key, val string, dist float64) bool {
			pointCoordinate, _ := indexKMLCoordinates(val)
			ids = append(ids, extractID(key))
			fmt.Println(distance(inputCoordinate, pointCoordinate))
			return distance(inputCoordinate, pointCoordinate) <= maxDistMeters
		})
		if err != nil {
			return err
		}

		for _, id := range ids {
			s, err := tx.Get(fmt.Sprintf(NamesPattern, id))
			if err != nil {
				return err
			}
			fmt.Println(s)
		}

		return nil
	})
}
