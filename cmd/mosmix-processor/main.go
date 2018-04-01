package main

import (
	"fmt"

	mosmixDB "github.com/codeformuenster/mosmix/db"
	mosmixXML "github.com/codeformuenster/mosmix/xml"
)

func main() {
	// f, err := os.Create("profile")
	// if err != nil {
	// 	log.Fatal(err)
	// }
	// pprof.StartCPUProfile(f)
	// defer pprof.StopCPUProfile()

	db, err := mosmixDB.NewMosmixDB(":memory:")
	// db, err := mosmixDB.NewMosmixDB("data.db")
	if err != nil {
		fmt.Println(err)
		return
	}
	// err = mosmixXML.DownloadAndParse("file:///home/gerald/go/src/github.com/codeformuenster/mosmix/MOSMIX_S_2018033016_240.kml", db)
	err = mosmixXML.DownloadAndParse("file:///home/gerald/go/src/github.com/codeformuenster/mosmix/MOSMIX_S_2018033016_240.kml_original", db)
	// err = mosmixXML.DownloadAndParse(mosmixXML.DefaultMosmixURL, db)
	if err != nil {
		fmt.Println(err)
		return
	}
	err = db.PersistToDisk("data.db")
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

	// err = db.GetForecastsAround("9.39,51.66", 10000)
	// if err != nil {
	// 	fmt.Println(err)
	// 	return
	// }
}
