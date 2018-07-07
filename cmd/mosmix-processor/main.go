package main

import (
	"flag"
	"fmt"

	mosmixDB "github.com/codeformuenster/mosmix-processor/db"
	mosmixXML "github.com/codeformuenster/mosmix-processor/xml"
)

func main() {
	urlToDownload := flag.String("src", "", "the url to download")
	dbPath := flag.String("db", "", "postgis db connection string")
	flag.Parse()
	schema := flag.Arg(0)
	if *dbPath == "" {
		fmt.Println("Error: Missing db parameter (postgres connection URI)")
		return
	}
	if *urlToDownload == "" && schema == "mosmix_s" {
		*urlToDownload = mosmixXML.DefaultMosmixSURL
	} else if *urlToDownload == "" && schema == "mosmix_l" {
		*urlToDownload = mosmixXML.DefaultMosmixLURL
	} else if *urlToDownload == "" {
		fmt.Println("Error: src flag is required on missing or invalid mosmix type argument (either \"mosmix_s\" or \"mosmix_l\")")
		return
	}

	if schema == "" {
		schema = "public"
	}

	db, err := mosmixDB.NewMosmixDB(*dbPath, schema)
	if err != nil {
		fmt.Println(err)
		return
	}
	defer db.Close()

	err = mosmixXML.DownloadAndParse(*urlToDownload, db)
	if err != nil {
		fmt.Println(err)
		return
	}

	err = db.Finalize()
	if err != nil {
		fmt.Println(err)
		return
	}
}
