package main

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"net/http"
	"os"
	"path/filepath"
	"sync"
	"time"

	dac "github.com/xinsnake/go-http-digest-auth-client"
)

func main() {
	outDir := "./out"
	var wg sync.WaitGroup
	concurrencyLimit := 15
	sem := make(chan struct{}, concurrencyLimit)
	counter := 0
	counterMutex := sync.Mutex{}

	// Ticker to refresh the counter every 500ms
	ticker := time.NewTicker(500 * time.Millisecond)
	go func() {
		for range ticker.C {
			counterMutex.Lock()
			fmt.Printf("Files uploaded: %d\n", counter)
			counterMutex.Unlock()
		}
	}()

	err := filepath.Walk(outDir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			fmt.Printf("Error accessing file: %v\n", err)
			return nil
		}

		if !info.IsDir() {
			wg.Add(1)
			sem <- struct{}{} // Acquire a slot in the semaphore

			go func(path string) {
				defer wg.Done()
				defer func() { <-sem }() // Release the slot in the semaphore

				fileContents, err := ioutil.ReadFile(path)
				if err != nil {
					fmt.Printf("Error reading file %s: %v\n", path, err)
					return
				}

				url := "http://localhost:8890/sparql-graph-crud-auth/?graph-uri=https://synbiohub.org/public"
				t := dac.NewTransport("dba", "dba")
				req, err := http.NewRequest("POST", url, bytes.NewBuffer(fileContents))
				if err != nil {
					fmt.Printf("Error creating request for file %s: %v\n", path, err)
					return
				}

				req.Header.Set("Content-Type", "application/rdf+xml")

				resp, err := t.RoundTrip(req)
				if err != nil {
					fmt.Printf("Error sending request for file %s: %v\n", path, err)
					return
				}
				defer resp.Body.Close()

				if resp.StatusCode != http.StatusOK {
					if resp.StatusCode == http.StatusCreated {
						fmt.Printf("Received status code %d, retrying in 1 second...\n", resp.StatusCode)
						time.Sleep(1 * time.Second)
						resp, err = t.RoundTrip(req)
						if err != nil {
							fmt.Printf("Error sending request for file %s: %v\n", path, err)
							return
						} else {
							counterMutex.Lock()
							counter++
							counterMutex.Unlock()
						}
						defer resp.Body.Close()
					} else {
						fmt.Printf("Failed to upload file %s: received status code %d\n", path, resp.StatusCode)
						return
					}
				} else {
					counterMutex.Lock()
					counter++
					counterMutex.Unlock()
				}

			}(path)
		}

		return nil
	})

	if err != nil {
		fmt.Printf("Error walking directory: %v\n", err)
	}

	wg.Wait()
	ticker.Stop() // Stop the ticker when all uploads are done
	fmt.Printf("Final count of files uploaded: %d\n", counter)
}
