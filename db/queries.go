package db

import (
	"time"
)

type Metadata struct {
	ForecastTimeSteps []string `xml:"https://opendata.dwd.de/weather/lib/pointforecast_dwd_extension_V1_0.xsd ForecastTimeSteps>TimeStep"`
	DefaultUndefSign  string   `xml:"https://opendata.dwd.de/weather/lib/pointforecast_dwd_extension_V1_0.xsd FormatCfg>DefaultUndefSign"`
	GeneratingProcess string   `xml:"https://opendata.dwd.de/weather/lib/pointforecast_dwd_extension_V1_0.xsd GeneratingProcess"`
	Issuer            string   `xml:"https://opendata.dwd.de/weather/lib/pointforecast_dwd_extension_V1_0.xsd Issuer"`
	ProductID         string   `xml:"https://opendata.dwd.de/weather/lib/pointforecast_dwd_extension_V1_0.xsd ProductID"`
	ReferencedModels  []struct {
		Name          string    `xml:"https://opendata.dwd.de/weather/lib/pointforecast_dwd_extension_V1_0.xsd name,attr"`
		ReferenceTime time.Time `xml:"https://opendata.dwd.de/weather/lib/pointforecast_dwd_extension_V1_0.xsd referenceTime,attr"`
	} `xml:"https://opendata.dwd.de/weather/lib/pointforecast_dwd_extension_V1_0.xsd ReferencedModel>Model"`
	ProcessingTime     time.Time
	DownloadDuration   time.Duration
	ParsingDuration    time.Duration
	SourceURL          string
	AvailableVariables []string
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
	tx, err := m.db.Begin()
	if err != nil {
		return err
	}

	stmt, err := tx.Prepare(`INSERT INTO metadata(
		source_url,
		processing_time,
		download_duration,
		parsing_duration,
		parser,
		dwd_issuer,
		dwd_product_id,
		dwd_generating_process
		) values(?, ?, ?, ?, ?, ?, ?, ?)`)
	if err != nil {
		return err
	}
	defer stmt.Close()

	_, err = stmt.Exec(
		metadata.SourceURL,
		metadata.ProcessingTime,
		metadata.DownloadDuration,
		metadata.ParsingDuration,
		"github.com/codeformuenster/mosmix-processor",
		metadata.Issuer,
		metadata.ProductID,
		metadata.GeneratingProcess,
	)
	if err != nil {
		return err
	}

	modelsStmt, err := tx.Prepare("INSERT INTO dwd_referenced_models(name, reference_time) values(?, ?)")
	if err != nil {
		return err
	}
	defer modelsStmt.Close()

	for _, model := range metadata.ReferencedModels {
		modelsStmt.Exec(model.Name, model.ReferenceTime)
		if err != nil {
			return err
		}
	}

	timestepsStmt, err := tx.Prepare("INSERT INTO dwd_available_timesteps(timestep) values(?)")
	if err != nil {
		return err
	}
	defer timestepsStmt.Close()

	for _, timestep := range metadata.ForecastTimeSteps {
		timestepsStmt.Exec(timestep)
		if err != nil {
			return err
		}
	}

	variablesStmt, err := tx.Prepare("INSERT INTO dwd_available_forecast_variables(name) values(?)")
	if err != nil {
		return err
	}
	defer variablesStmt.Close()

	for _, variable := range metadata.AvailableVariables {
		variablesStmt.Exec(variable)
		if err != nil {
			return err
		}
	}

	tx.Commit()
	if err != nil {
		return err
	}

	return nil
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
