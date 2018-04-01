package xml

import (
	"bufio"
	"encoding/xml"
	"io/ioutil"
	"os"
	"strings"
	"time"

	mosmixDB "github.com/codeformuenster/mosmix/db"
	getter "github.com/hashicorp/go-getter"
	"golang.org/x/net/html/charset"
)

// Default Mosmix URL
const DefaultMosmixURL = "https://opendata.dwd.de/weather/local_forecasts/mos/MOSMIX_S_LATEST_240.kmz"

// DownloadAndParse tries to download and extract the given url into the given
// db instance
func DownloadAndParse(url string, db *mosmixDB.MosmixDB) error {
	startDownload := time.Now()
	// create a tmpfile
	tmpfile, err := ioutil.TempFile("", "mosmix")
	if err != nil {
		return err
	}
	tmpFilename := tmpfile.Name()
	defer os.Remove(tmpFilename)
	// download the file into the tmpfile
	err = downloadFile(url, tmpFilename)
	if err != nil {
		return err
	}

	metadata := mosmixDB.Metadata{SourceURL: url, ProcessingTime: startDownload, DownloadDuration: time.Now().Sub(startDownload)}

	startParsing := time.Now()
	err = parseDWDKMLFile(tmpFilename, db, &metadata)
	if err != nil {
		return err
	}
	metadata.ParsingDuration = time.Now().Sub(startParsing)
	err = db.InsertMetadata(&metadata)

	if err != nil {
		return err
	}

	return nil
}

func downloadFile(url, targetFilename string) error {
	client := getter.Client{
		Src:  url,
		Dst:  targetFilename,
		Mode: getter.ClientModeFile,
		Decompressors: map[string]getter.Decompressor{
			"kmz": new(getter.ZipDecompressor),
		},
	}
	return client.Get()
}

func parseDWDKMLFile(filename string, db *mosmixDB.MosmixDB, metadata *mosmixDB.Metadata) error {
	file, err := os.Open(filename)
	if err != nil {
		return err
	}
	defer file.Close()

	xmlDecoder := xml.NewDecoder(bufio.NewReader(file))
	xmlDecoder.CharsetReader = charset.NewReaderLabel

	// http://blog.davidsingleton.org/parsing-huge-xml-files-with-go/
	for {
		token, _ := xmlDecoder.Token()
		if token == nil {
			break
		}
		switch se := token.(type) {
		case xml.StartElement:
			if se.Name.Local == "Placemark" {
				err = parseAndPersistPlacemarkElement(se, xmlDecoder, db, metadata)
				if err != nil {
					return err
				}
			} else if se.Name.Local == "ProductDefinition" {
				err := xmlDecoder.DecodeElement(&metadata, &se)
				if err != nil {
					return err
				}
				err = db.InsertMetadata(metadata)
				if err != nil {
					return err
				}
			}
		}
	}

	return nil
}

func parseAndPersistPlacemarkElement(se xml.StartElement, xmlDecoder *xml.Decoder, db *mosmixDB.MosmixDB, metadata *mosmixDB.Metadata) error {
	// var place Placemark
	var place mosmixDB.ForecastPlace
	err := xmlDecoder.DecodeElement(&place, &se)
	if err != nil {
		return err
	}

	// iterate through ForecastVariables
	for _, variable := range place.ForecastVariables {
		parts := strings.Fields(variable.Value)
		// fmt.Println(variable.Name)
		processedForecastVariable := mosmixDB.ProcessedForecastVariable{Name: variable.Name}
		for i, part := range parts {
			if part == metadata.DefaultUndefSign {
				continue
			}
			// fmt.Printf("%d %v\n", i, part)
			processedForecastVariable.Timesteps = append(processedForecastVariable.Timesteps, mosmixDB.ProcessedForecastTimestep{Value: part, Timestep: metadata.ForecastTimeSteps[i]})
		}
		place.ProcessedForecastVariables = append(place.ProcessedForecastVariables, &processedForecastVariable)
	}

	err = db.InsertForecast(&place)
	if err != nil {
		return err
	}
	return nil
}
