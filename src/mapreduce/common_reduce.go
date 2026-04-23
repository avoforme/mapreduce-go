package mapreduce

import (
	"encoding/json"
	"os"
	"sort"
)

// doReduce does the job of a reduce worker: it reads the intermediate
// key/value pairs (produced by the map phase) for this task, sorts the
// intermediate key/value pairs by key, calls the user-defined reduce function
// (reduceF) for each key, and writes the output to disk.
func doReduce(
	jobName string, // the name of the whole MapReduce job
	reduceTaskNumber int, // which reduce task this is
	nMap int, // the number of map tasks that were run ("M" in the paper)
	reduceF func(key string, values []string) string,
) {
	// TODO:
	// You will need to write this function.
	//
	// Step 1: Read all intermediate files for this reduce task.
	//   There are nMap intermediate files, one per map task.
	//   For map task m, the file is: reduceName(jobName, m, reduceTaskNumber)
	//   Open each file, create a json.NewDecoder, and call .Decode() in a loop
	//   until it returns an error (io.EOF means you've read all entries):
	//     dec := json.NewDecoder(f)
	//     for {
	//       var kv KeyValue
	//       if err := dec.Decode(&kv); err != nil { break }
	//       kvMap[kv.Key] = append(kvMap[kv.Key], kv.Value)
	//     }
	//

	kvMap := make(map[string][]string)
	for i := range nMap {
		file, err := os.OpenFile(reduceName(jobName, i, reduceTaskNumber), os.O_RDONLY, 0644)
		checkError(err)
		decode := json.NewDecoder(file)
		for {
			var kv KeyValue
			if decode.Decode(&kv)!= nil {
				break
			}
			
			kvMap[kv.Key] = append(kvMap[kv.Key], kv.Value)
			
		}
		file.Close()
	}

	// Step 2: Call reduceF for each unique key.
	//   Collect all keys, sort them (sort.Strings), then for each key call
	//   reduceF(key, kvMap[key]) to get the output value.
	//
    keys := make([]string, 0, len(kvMap))
	for k := range kvMap {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	

	// Step 3: Write output atomically using a temp file + os.Rename.
	//   The final output filename is: mergeName(jobName, reduceTaskNumber)
	//   See the Note in the project spec (Part A) for the required pattern:
	//   create the output with os.CreateTemp, write JSON-encoded KeyValue pairs,
	//   close it, then call os.Rename(tmp.Name(), mergeName(...)).
	//   JSON encoding:
	//     enc := json.NewEncoder(tmpFile)
	//     enc.Encode(KeyValue{key, reduceF(key, kvMap[key])})
	//
	// Use checkError to handle errors.

	tempFile, err := os.CreateTemp("", "mr-reduce-*")
	checkError(err)

	encoder := json.NewEncoder(tempFile)

	for _, key := range keys {
		err := encoder.Encode(KeyValue{key, reduceF(key, kvMap[key])})
		checkError(err)
	}

	tempFile.Close()
	err = os.Rename(tempFile.Name(), mergeName(jobName, reduceTaskNumber))
	checkError(err)


}
