package main

import (
	// "bufio"
	"bytes"
	"log"
	"os"
	"runtime"
	// "regexp"
	"sync"
	"time"

	"github.com/edsrzf/mmap-go"
	"github.com/moovweb/rubex"
)

const MiB = 1024 * 1024

func N(n int) []struct{} {
	return make([]struct{}, n)
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

	result := make(chan int)

	regex := rubex.MustCompile("(?i)beckham")
	// _ = regex
	_ = bytes.IndexFunc

	readChunk := func(idx int) {
		start, end := idx*chunkSize, (idx+1)*chunkSize
		if idx == Nreaders-1 {
			end = totalSize
		}

		data := mapping[start:end]

		// var n int
		// for {
		// 	n++
		// 	i := bytes.Index(data, []byte("eckham"))
		// 	if i == -1 {
		// 		break
		// 	}
		// 	// log.Println("i=", i, string(data[i:i+50]))
		// 	data = data[i+1:]
		// }
		// result <- n

		log.Println("Here:", len(data))
		locs := regex.FindIndex(data)
		matches := len(locs)
		result <- matches

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

		var b int
		for r := range result {
			b += r
		}
		log.Println("Result =", b)
	}()

	start := time.Now()
	wg.Wait()
	log.Println("Elapsed:", time.Since(start))
	close(result)
	<-finished
}
