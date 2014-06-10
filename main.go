package main

import (
	"index/suffixarray"
	"log"
	"os"
	"runtime"
	"time"

	"github.com/edsrzf/mmap-go"
)

const MiB = 1024 * 1024

func main() {
	var ms runtime.MemStats

	fd, err := os.Open("/mem/output-xsd-fix-no-provenance-factuality.tql")
	if err != nil {
		panic(err)
	}
	defer fd.Close()

	mapping, err := mmap.Map(fd, mmap.RDONLY, 0)
	if err != nil {
		panic(err)
	}

	log.Println("Alloc'd:", ms.Alloc/MiB, "MiB")

	// var b byte
	amount := 10 * 1024 * 1024

	// for _, v := range mapping[:amount] {
	//      b += v
	// }
	// log.Println("Result =", b)
	data := mapping[:amount]

	start := time.Now()
	index := suffixarray.New(data)
	log.Println("Took", time.Since(start), "to build suffix array")

	// sa.FindAllIndex(r, 100)

	runtime.ReadMemStats(&ms)
	log.Println("Alloc'd:", ms.Alloc/MiB, "MiB")

	start = time.Now()
	places := index.Lookup([]byte("dbpedia"), 100)
	log.Println("Lookup time:", time.Since(start))

	log.Println("N places:", len(places))
	log.Println("places:", places[:10])

}
