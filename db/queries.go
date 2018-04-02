package db

import (
	"encoding/json"
	"time"
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
	DownloadDuration time.Duration `json:"downloadDuration,string"`
	ParsingDuration  time.Duration `json:"processingDuration,string"`
	SourceURL        string        `json:"sourceURL"`
	// IssueTime is empty?!
	// IssueTime       *time.Time `xml:"https://opendata.dwd.de/weather/lib/pointforecast_dwd_extension_V1_0.xsd IssueTime,omitempty"`   // ZZmaxLength=0
}

type ForecastPlace struct {
	ForecastVariables []struct {
		Name      string `xml:"https://opendata.dwd.de/weather/lib/pointforecast_dwd_extension_V1_0.xsd elementName,attr"`
		RawValues string `xml:"https://opendata.dwd.de/weather/lib/pointforecast_dwd_extension_V1_0.xsd value"`
		Values    []ForecastVariableTimestep
	} `xml:"ExtendedData>Forecast"` // Ignore namespace, because I don't know how to write this with namespace
	Geometry KMLPoint `xml:"http://www.opengis.net/kml/2.2 Point>coordinates"`
	Name     string   `xml:"http://www.opengis.net/kml/2.2 description"`
	ID       string   `xml:"http://www.opengis.net/kml/2.2 name"`
}

type KMLPoint struct {
	Longitude, Latitude, Altitude float64
}

type ForecastVariableTimestep struct {
	Timestep string
	Value    string // we rely on casting in sqlite
}

// InsertMetadata creates a JSON document in the database under the metadata key
func (m *MosmixDB) InsertMetadata(metadata *Metadata) error {
	jsonStr, err := json.Marshal(metadata)
	if err != nil {
		return err
	}
	tx, err := m.db.Begin()
	if err != nil {
		return err
	}
	stmt, err := tx.Prepare("INSERT INTO metadata(json) values(?)")
	if err != nil {
		return err
	}
	defer stmt.Close()
	_, err = stmt.Exec(string(jsonStr))
	if err != nil {
		return err
	}
	tx.Commit()
	if err != nil {
		return err
	}
	return nil
}

func (m *MosmixDB) GetMetadata() (string, error) {
	stmt, err := m.db.Prepare("SELECT json FROM metadata")
	if err != nil {
		return "", err
	}
	defer stmt.Close()
	var jsonStr string
	err = stmt.QueryRow().Scan(&jsonStr)
	if err != nil {
		return "", err
	}

	return jsonStr, nil
}

func (m *MosmixDB) InsertForecast(forecast *ForecastPlace) error {
	tx, err := m.db.Begin()
	if err != nil {
		return err
	}
	forecastPlaceStatement, err := tx.Prepare("INSERT INTO forecast_places(id, name, the_geom) values(?, ?, MakePointZ(?, ?, ?, 4326))")
	if err != nil {
		return err
	}
	defer forecastPlaceStatement.Close()
	_, err = forecastPlaceStatement.Exec(forecast.ID, forecast.Name, forecast.Geometry.Longitude, forecast.Geometry.Latitude, forecast.Geometry.Altitude)
	if err != nil {
		return err
	}
	forecastVariableStatement, err := tx.Prepare("INSERT INTO forecasts(place_id, name, timestep, value) values(?, ?, ?, ?)")
	if err != nil {
		return err
	}
	defer forecastVariableStatement.Close()
	for _, variable := range forecast.ForecastVariables {
		for _, value := range variable.Values {
			_, err = forecastVariableStatement.Exec(forecast.ID, variable.Name, value.Timestep, value.Value)
			if err != nil {
				return err
			}
		}
	}
	tx.Commit()
	if err != nil {
		return err
	}
	return nil
}
