package main

import (
	// "bufio"
	"bytes"
	"fmt"
	"log"
	"os"
	"runtime"
	// "regexp"
	"sync"
	"time"

	"github.com/edsrzf/mmap-go"
	// "github.com/moovweb/rubex"
	// "code.google.com/p/ahocorasick"
)

const MiB = 1024 * 1024

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
	return prevNL

}

func NextNewline(s []byte, start int) int {
	nextNL := bytes.IndexAny(s[start:], "\n")
	if nextNL == -1 {
		return -1
	}
	nextNL += start
	return nextNL
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

	totalSize := len(mapping)
	Nreaders := runtime.GOMAXPROCS(0)

	chunkSize := totalSize / Nreaders

	result := make(chan [][]byte)

	// regex := rubex.MustCompile("(?i)beckham")
	// _ = regex

	readChunk := func(idx int) {
		start, end := idx*chunkSize, (idx+1)*chunkSize
		if idx == Nreaders-1 {
			end = totalSize
		}

		data := mapping[start:end]
		// wholeData := mapping[start:end]

		// var n int
		// for {
		// 	n++
		// 	i := bytes.Index(data, []byte("eckham"))
		// 	if i == -1 {
		// 		break
		// 	}
		// 	nextNL := bytes.IndexAny(data, "\n")
		// 	here := len(wholeData) - len(data) + i
		// 	dataUptoHere := wholeData[:here]
		// 	prevNL := bytes.LastIndexAny(dataUptoHere, "\n")
		// 	if prevNL == -1 {
		// 		prevNL = 0
		// 	}
		// 	// log.Println("i=", i, string(data[i:i+50]))
		// 	log.Println("start, end, len =", prevNL, here-i+nextNL, len(wholeData)-(here-i+nextNL))
		// 	line := wholeData[prevNL : here-i+nextNL]
		// 	fmt.Fprintln(os.Stdout, line)
		// 	data = data[i+1:]
		// }

		i := -1
		var n int
		localResult := [][]byte{}

		for {
			i = Index(data, []byte("eckham"), i+1)
			if i == -1 {
				break
			}
			n++
			prevNL := PrevNewline(data, i)
			nextNL := NextNewline(data, i)
			if nextNL == -1 {
				// Line end is at end of chunk
				// nextNL = len(data) - 1
				break
			}

			i = nextNL
			// prevNL := bytes.LastIndexAny(data[:i], "\n")
			// if prevNL == -1 {
			// 	// Line start is at beginning of chunk
			// 	prevNL = 0
			// }
			// nextNL := bytes.IndexAny(data[i:], "\n")
			// if nextNL == -1 {
			// 	// Line end is at end of chunk
			// 	nextNL = len(data) - 1
			// } else {
			// 	nextNL += i
			// }

			line := data[prevNL:nextNL]
			localResult = append(localResult, line)
		}
		result <- localResult
		log.Println("count =", n)

		// needle := ahocorasick.NewAhoCorasick([]string{"eckham"})

		// var n int
		// for result := range ahocorasick.MatchBytes(data, needle) {
		// 	n++
		// 	_ = result
		// }
		// result <- n
		// log.Println("Here:", len(data))
		// locs := regex.FindIndex(data[:100*1024])
		// matches := len(locs)
		// result <- matches

		// buf := bytes.NewReader(data)
		// s := bufio.NewScanner(buf)

		// lines := 0
		// matches := 0

		// for s.Scan() {
		// 	lines++
		// 	locs := regex.FindIndex(s.Bytes())
		// 	matches += len(locs)
		// }
		// log.Println("Read", lines, "lines")

		// result <- matches
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

		for r := range allResults {
			fmt.Fprintln(os.Stdout, string(r))
		}
	}()

	start := time.Now()
	wg.Wait()
	log.Println("Elapsed:", time.Since(start))
	close(result)
	<-finished
}
