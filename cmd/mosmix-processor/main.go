package main

import (
	"flag"
	"fmt"

	mosmixDB "github.com/codeformuenster/mosmix-processor/db"
	mosmixXML "github.com/codeformuenster/mosmix-processor/xml"
)

func main() {
	urlToDownload := flag.String("src", mosmixXML.DefaultMosmixURL, "the url to download")
	dbPath := flag.String("db", "postgresql://mosmix-postgis?sslmode=disable", "postgis db connection string")
	flag.Parse()

	db, err := mosmixDB.NewMosmixDB(*dbPath)
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
}
