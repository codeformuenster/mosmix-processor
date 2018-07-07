package xml

import (
	"bufio"
	"encoding/xml"
	"io/ioutil"
	"os"

	mosmixDB "github.com/codeformuenster/mosmix-processor/db"
	"golang.org/x/net/html/charset"
)

const metElementDefinitionURL = "https://opendata.dwd.de/weather/lib/MetElementDefinition.xml"

func downloadAndParseDefinitions(db *mosmixDB.MosmixDB) error {
	// create a tmpfile
	tmpfile, err := ioutil.TempFile("", "mosmix")
	if err != nil {
		return err
	}
	tmpFilename := tmpfile.Name()
	defer os.Remove(tmpFilename)
	// download the file into the tmpfile
	err = downloadFile(metElementDefinitionURL, tmpFilename)
	if err != nil {
		return err
	}

	file, err := os.Open(tmpFilename)
	if err != nil {
		return err
	}
	defer file.Close()

	xmlDecoder := xml.NewDecoder(bufio.NewReader(file))
	xmlDecoder.CharsetReader = charset.NewReaderLabel

	var metElements []mosmixDB.MetElement

	// http://blog.davidsingleton.org/parsing-huge-xml-files-with-go/
	for {
		token, _ := xmlDecoder.Token()
		if token == nil {
			break
		}
		switch se := token.(type) {
		case xml.StartElement:
			if se.Name.Local == "MetElement" {
				metElement := mosmixDB.MetElement{}
				err := xmlDecoder.DecodeElement(&metElement, &se)
				if err != nil {
					return err
				}
				metElements = append(metElements, metElement)
			}
		}
	}

	err = db.InsertMetDefinitions(&metElements)
	if err != nil {
		return err
	}

	return nil
}
