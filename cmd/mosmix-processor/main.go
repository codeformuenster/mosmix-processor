package main

import (
	"flag"
	"fmt"

	mosmixDB "github.com/codeformuenster/mosmix-processor/db"
	mosmixXML "github.com/codeformuenster/mosmix-processor/xml"
)

func main() {
	urlToDownload := flag.String("-src", mosmixXML.DefaultMosmixURL, "the url to download")
	dbPath := flag.String("-d", "data.spatialite", "the url to download")

	db, err := mosmixDB.NewMosmixDB(*dbPath)
	if err != nil {
		fmt.Println(err)
		return
	}
	defer db.Close()

	fmt.Printf("Processing %v into %v\n", *urlToDownload, *dbPath)

	err = mosmixXML.DownloadAndParse(*urlToDownload, db)
	if err != nil {
		fmt.Println(err)
		return
	}
	metadata, err := db.GetMetadata()
	if err != nil {
		fmt.Println(err)
		return
	}
	fmt.Println(metadata)

}
