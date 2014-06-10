package main

import (
	"log"
	"os"
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

	// runtime.ReadMemStats(&ms)
	// log.Println("Alloc'd:", ms.Alloc/MiB, "MiB")

	totalSize := len(mapping)

	const Nreaders = 32

	chunkSize := totalSize / Nreaders

	result := make(chan byte)

	readChunk := func(idx int) {
		var b byte
		start, end := idx*chunkSize, (idx+1)*chunkSize
		data := mapping[start:end]
		for _, v := range data {
			b += v
		}

		result <- b
	}

	var wg sync.WaitGroup

	for i := range N(Nreaders) {
		wg.Add(1)
		start, end := i*chunkSize, (i+1)*chunkSize
		log.Println("start, end =", start, end)
		go func(i int) {
			defer wg.Done()
			readChunk(i)
		}(i)
	}

	finished := make(chan struct{})
	go func() {
		defer close(finished)

		var b byte
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

	// amount := 10 * 1024 * 1024

	var b byte
	for _, v := range mapping {
		b += v
	}
	log.Println("Result =", b)
	// data := mapping[:amount]

	// start := time.Now()
	// index := suffixarray.New(data)
	// log.Println("Took", time.Since(start), "to build suffix array")

	// // sa.FindAllIndex(r, 100)

	// runtime.ReadMemStats(&ms)
	// log.Println("Alloc'd:", ms.Alloc/MiB, "MiB")

	// start = time.Now()
	// places := index.Lookup([]byte("dbpedia"), 10000)
	// log.Println("Lookup time:", time.Since(start))

	// log.Println("N places:", len(places))
	// log.Println("places:", places[:10])

}
