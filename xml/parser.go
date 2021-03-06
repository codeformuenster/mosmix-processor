package xml

import (
	"bufio"
	"encoding/xml"
	"fmt"
	"io/ioutil"
	"os"
	"strings"
	"time"

	mosmixDB "github.com/codeformuenster/mosmix-processor/db"
	getter "github.com/hashicorp/go-getter"
	"golang.org/x/net/html/charset"
)

// DownloadAndParse tries to download and extract the given url into the given
// db instance
func DownloadAndParse(url string, db *mosmixDB.MosmixDB) error {
	fmt.Printf("Downloading & extracting file %v .... ", url)
	db.ProcessingTimestamp = time.Now()
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

	metadata := mosmixDB.Metadata{
		SourceURL:        url,
		ProcessingTime:   db.ProcessingTimestamp.UTC(),
		DownloadDuration: time.Now().Sub(db.ProcessingTimestamp),
	}
	fmt.Printf("done in %s\n", metadata.DownloadDuration)

	startParsingMetDefs := time.Now()
	fmt.Printf("Downloading & parsing element definitions from %v .... ", metElementDefinitionURL)
	err = downloadAndParseDefinitions(db)
	if err != nil {
		return err
	}
	fmt.Printf("done in %s\n", time.Now().Sub(startParsingMetDefs))

	startParsing := time.Now()
	fmt.Print("Parsing & inserting .... ")
	err = parseDWDKMLFile(tmpFilename, db, &metadata)
	if err != nil {
		return err
	}
	metadata.ParsingDuration = time.Now().Sub(startParsing)
	fmt.Printf("done in %s\n", metadata.ParsingDuration)

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
			}
		}
	}

	return nil
}

func contains(arr []string, str string) bool {
	for _, a := range arr {
		if a == str {
			return true
		}
	}
	return false
}

func parseAndPersistPlacemarkElement(se xml.StartElement, xmlDecoder *xml.Decoder, db *mosmixDB.MosmixDB, metadata *mosmixDB.Metadata) error {
	place := mosmixDB.ForecastPlace{}
	err := xmlDecoder.DecodeElement(&place, &se)
	if err != nil {
		return err
	}

	// iterate through ForecastVariables
	for ctVariable, variable := range place.ForecastVariables {
		parts := strings.Fields(place.ForecastVariables[ctVariable].RawValues)
		for ctTimestep, part := range parts {
			if part == metadata.DefaultUndefSign {
				continue
			}
			place.ForecastVariables[ctVariable].Values = append(place.ForecastVariables[ctVariable].Values,
				mosmixDB.ForecastVariableTimestep{Value: part, Timestep: metadata.ForecastTimeSteps[ctTimestep]})
		}
		if !contains(metadata.AvailableVariables, variable.Name) {
			metadata.AvailableVariables = append(metadata.AvailableVariables, variable.Name)
		}
	}

	err = db.InsertForecast(&place)
	if err != nil {
		return err
	}
	return nil
}
