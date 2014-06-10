package main

import (
	"log"
	"os"

	"github.com/edsrzf/mmap-go"
)

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

	var b byte
	amount := 1024 * 1024 * 1024

	for _, v := range mapping[:amount] {
		b += v
	}
	log.Println("Result =", b)
}
