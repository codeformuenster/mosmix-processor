package url

import (
	"errors"
	"fmt"
	"time"
)

const baseURL = "https://opendata.dwd.de/weather/local_forecasts/mos"

// Generate can be used to to generate a valid mosmix URL
func Generate(schema string) (string, error) {
	// create a timestamp like its used in the mosmix filename
	timestamp := time.Now().UTC().Format("2006010215")

	if schema == "mosmix_s" {
		return fmt.Sprintf("%s/MOSMIX_S/all_stations/kml/MOSMIX_S_%s_240.kmz", baseURL, timestamp), nil
	} else if schema == "mosmix_l" {
		return fmt.Sprintf("%s/MOSMIX_L/all_stations/kml/MOSMIX_L_%s.kmz", baseURL, timestamp), nil
	}

	return "", errors.New("Unknown schema")
}
