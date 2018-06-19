package db

import (
	"bytes"
	"fmt"
	"log"
	"time"

	"github.com/lib/pq"
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
	_, err := m.db.Exec(`INSERT INTO metadata_temp (
		source_url,
		processing_time,
		download_duration,
		parsing_duration,
		parser,
		dwd_issuer,
		dwd_product_id,
		dwd_generating_process
		) values($1, $2, $3, $4, $5, $6, $7, $8)`,
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

	for _, model := range metadata.ReferencedModels {
		_, err := m.db.Exec("INSERT INTO dwd_referenced_models_temp (name, reference_time) values($1, $2)", model.Name, model.ReferenceTime)
		if err != nil {
			return err
		}
	}

	for _, timestep := range metadata.ForecastTimeSteps {
		_, err := m.db.Exec("INSERT INTO dwd_available_timesteps_temp (timestep) values($1)", timestep)
		if err != nil {
			return err
		}
	}

	for _, variable := range metadata.AvailableVariables {
		_, err := m.db.Exec("INSERT INTO dwd_available_forecast_variables_temp (name) VALUES ($1)", variable)
		if err != nil {
			return err
		}
	}

	return nil
}

func (m *MosmixDB) CreateForecastsAllView() error {
	var qryBytes bytes.Buffer

	_, err := m.db.Exec("DROP MATERIALIZED VIEW IF EXISTS forecasts_all")
	if err != nil {
		return err
	}

	rows, err := m.db.Query("SELECT name FROM dwd_available_forecast_variables")
	if err != nil {
		return err
	}
	defer rows.Close()
	isFirst := true
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return err
		}
		if isFirst == true {
			fmt.Fprintf(&qryBytes, "CREATE MATERIALIZED VIEW forecasts_all AS SELECT * FROM (SELECT place_id, timestep, value AS %s FROM forecasts WHERE name = '%s') AS %s", name, name, name)
			isFirst = false
			continue
		}
		fmt.Fprintf(&qryBytes, " LEFT JOIN (SELECT place_id, timestep, value as %s FROM forecasts WHERE name = '%s') AS %s USING (timestep, place_id)", name, name, name)
	}
	if err := rows.Err(); err != nil {
		return err
	}

	_, err = m.db.Exec(qryBytes.String())
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

	_, err = tx.Exec("INSERT INTO forecast_places_temp (id, name, the_geom) VALUES ($1, $2, ST_SetSRID(ST_MakePoint($3, $4, $5), 4326));",
		forecast.ID, forecast.Name, forecast.Geometry.Longitude, forecast.Geometry.Latitude, forecast.Geometry.Altitude)
	if err != nil {
		return err
	}

	stmt, err := tx.Prepare(pq.CopyIn("forecasts_temp", "place_id", "name", "timestep", "value"))
	if err != nil {
		log.Fatal(err)
	}

	for _, variable := range forecast.ForecastVariables {
		for _, value := range variable.Values {
			_, err := stmt.Exec(forecast.ID, variable.Name, value.Timestep, value.Value)
			if err != nil {
				return err
			}
		}
	}
	err = stmt.Close()
	if err != nil {
		return err
	}

	err = tx.Commit()
	if err != nil {
		return err
	}
	return nil
}
