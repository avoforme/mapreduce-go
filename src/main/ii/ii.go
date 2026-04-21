package main

import (
	"fmt"
	"mr/mapreduce"
	"os"
)

// mapF is called once per input file. filename is the name of the file being
// processed and contents is the file's full text. For the inverted index
// application, mapF should emit one key/value pair per word occurrence,
// using the word as the key and the filename as the value.
func mapF(filename string, contents string) (res []mapreduce.KeyValue) {
	// TODO:
	return
}

// reduceF is called once per unique word across all input files. key is the
// word and values is a slice of filenames in which that word appears (may
// contain duplicates if the word appears multiple times in the same file).
// reduceF should return a sorted, deduplicated, comma-separated list of
// document names.
func reduceF(key string, values []string) string {
	// TODO:
	return ""
}

// Can be run in 2 ways:
// 1) Sequential (e.g., go run ii/ii.go master sequential x1.txt .. xN.txt)
// 2) Master   (e.g., go run ii/ii.go master localhost:7777 x1.txt .. xN.txt)
func main() {
	if len(os.Args) < 4 {
		fmt.Printf("%s: see usage comments in file\n", os.Args[0])
	} else if os.Args[1] == "master" {
		var mr *mapreduce.Master
		if os.Args[2] == "sequential" {
			mr = mapreduce.Sequential("iiseq", os.Args[3:], 3, mapF, reduceF)
		} else {
			mr = mapreduce.Distributed("iidis", os.Args[3:], 3, os.Args[2])
		}
		mr.Wait()
	} else {
		mapreduce.RunWorker(os.Args[2], os.Args[3], mapF, reduceF, 100)
	}
}
