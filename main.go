// ORIG GREP: 127769
package main

import (
	"bytes"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"runtime"
	"sync"
	"time"

	"github.com/edsrzf/mmap-go"
)

func N(n int) []struct{} {
	return make([]struct{}, n)
}

func Index(s, sep []byte, start int) int {
	i := bytes.Index(s[start:], sep)
	if i == -1 {
		return -1
	}
	i += start
	return i
}

func PrevNewline(s []byte, start int) int {
	prevNL := bytes.LastIndexAny(s[:start], "\n")
	if prevNL == -1 {
		// Line start is at beginning of chunk
		prevNL = 0
	}
	return prevNL + 1
}

func NextNewline(s []byte, start int) int {
	nextNL := bytes.IndexAny(s[start:], "\n")
	if nextNL == -1 {
		return -1
	}
	nextNL += start
	return nextNL
}

func Query(mapping []byte, output io.Writer, query string, cancel chan struct{}) {

	totalSize := len(mapping)
	Nreaders := runtime.GOMAXPROCS(0)

	chunkSize := totalSize / Nreaders

	result := make(chan [][]byte)

	readChunk := func(idx int) {
		start, end := idx*chunkSize, (idx+1)*chunkSize
		if idx == Nreaders-1 {
			end = totalSize
		}

		data := mapping[start:end]

		i := -1
		var n int
		localResult := [][]byte{}

		for {
			select {
			case <-cancel:
				return
			default:
			}

			i = Index(data, []byte(query), i+1)
			if i == -1 {
				break
			}
			n++
			prevNL := PrevNewline(data, i)
			nextNL := NextNewline(data, i)
			if nextNL == -1 {
				// Line end is at end of chunk
				break
			}

			i = nextNL
			line := data[prevNL : nextNL-1]
			localResult = append(localResult, line)
		}
		result <- localResult
	}

	var wg sync.WaitGroup

	for i := range N(Nreaders) {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			readChunk(i)
		}(i)
	}

	finished := make(chan struct{})
	go func() {
		defer close(finished)

		allResults := [][]byte{}

		for r := range result {
			allResults = append(allResults, r...)
		}
		log.Println("Total matches:", len(allResults))

		for _, r := range allResults {
			select {
			case <-cancel:
				return
			default:
			}
			fmt.Fprintln(output, string(r))
		}
	}()

	start := time.Now()
	wg.Wait()
	log.Println("Elapsed:", time.Since(start))
	close(result)
	<-finished
}

func main() {

	fd, err := os.Open("/mem/output-xsd-fix-no-provenance-factuality.tql")
	if err != nil {
		panic(err)
	}
	defer fd.Close()

	mapping, err := mmap.Map(fd, mmap.RDONLY, 0)
	if err != nil {
		panic(err)
	}
	defer mapping.Unmap()

	handler := func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)

		cancel := make(chan struct{})
		go func() {
			<-w.(http.CloseNotifier).CloseNotify()
			close(cancel)
		}()

		log.Println("Query:", r.URL.RawQuery)
		Query(mapping, w, r.URL.RawQuery, cancel)
	}

	http.HandleFunc("/", handler)

	log.Println("Serving on :80")
	http.ListenAndServe(":80", nil)
}
