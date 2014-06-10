package main

import (
	// "bufio"
	"bytes"
	"log"
	"os"
	// "regexp"
	"sync"
	"time"

	"github.com/edsrzf/mmap-go"
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
	const Nreaders = 32

	chunkSize := totalSize / Nreaders

	result := make(chan int)

	// regex := regexp.MustCompile("(?i)beckham")
	// _ = regex

	readChunk := func(idx int) {
		start, end := idx*chunkSize, (idx+1)*chunkSize
		if idx == Nreaders-1 {
			end = totalSize
		}

		data := mapping[start:end]

		i := bytes.IndexAny(data, "eckham")
		log.Println("i=", i)
		result <- i

		//locs := regex.FindIndex(data)
		//matches := len(locs)

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
