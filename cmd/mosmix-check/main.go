package main

import (
	"flag"
	"fmt"
	"net/http"
	"time"

	mosmixURL "github.com/codeformuenster/mosmix-processor/url"
)

func checkAvailable(url string) bool {
	// request the url
	resp, err := http.Head(url)
	if err != nil {
		return false
	}
	defer resp.Body.Close()

	// fmt.Printf("%s: %s, %s, %s\n", time.Now().Format(time.RFC3339), resp.Status, resp.Header["Last-Modified"], resp.Header["Content-Length"])

	// Check the status code
	if resp.StatusCode != 200 {
		return false
	}

	// check if the response has the Last-Modified header
	if _, ok := resp.Header["Last-Modified"]; !ok {
		return false
	}
	// check if the response has the Content-Length header
	if _, ok := resp.Header["Content-Length"]; !ok {
		return false
	}

	return true
}

func main() {
	intervalFlag := flag.String("interval", "20s", "the interval between checks. Parsed by time.ParseDuration")
	flag.Parse()
	schema := flag.Arg(0)

	sleepInterval, err := time.ParseDuration(*intervalFlag)
	if err != nil {
		fmt.Println(err)
		return
	}

	url, err := mosmixURL.Generate(schema)
	if err != nil {
		fmt.Println(err)
		return
	}

	if available := checkAvailable(url); available {
		return
	}

	// not available, start ticking
	ticker := time.NewTicker(sleepInterval)
	defer ticker.Stop()
	done := make(chan bool)
	for {
		select {
		case available := <-done:
			if available == true {
				return
			}
		case <-ticker.C:
			go func() {
				done <- checkAvailable(url)
			}()
		}
	}
}
