package mapreduce

import (
	"hash/fnv"
)

// doMap does the job of a map worker: it reads one of the input files
// (inFile), calls the user-defined map function (mapF) for that file's
// contents, and partitions the output into nReduce intermediate files.
func doMap(
	jobName string, // the name of the MapReduce job
	mapTaskNumber int, // which map task this is
	inFile string,
	nReduce int, // the number of reduce task that will be run ("R" in the paper)
	mapF func(file string, contents string) []KeyValue,
) {
	// TODO:
	// You will need to write this function.
	//
	// Step 1: Read the input file.
	//   Use os.ReadFile(inFile) to load the entire file into a byte slice,
	//   then convert to string: contents := string(data)
	//   Pass inFile and contents to mapF to get a []KeyValue back.
	//
	// Step 2: Partition output into nReduce intermediate files.
	//   For each KeyValue returned by mapF, compute which reduce task it belongs to:
	//     r := int(ihash(kv.Key)) % nReduce
	//   The output filename for reduce task r is: reduceName(jobName, mapTaskNumber, r)
	//
	// Step 3: Write each partition using JSON encoding.
	//   Use json.NewEncoder(file) and enc.Encode(&kv) for each KeyValue.
	//   Example:
	//     enc := json.NewEncoder(file)
	//     for _, kv := range kvs {
	//       err := enc.Encode(&kv)
	//       checkError(err)
	//     }
	//
	// Step 4: Write atomically using a temp file + os.Rename.
	//   See the Note in the project spec (Part A) for the required pattern:
	//   create each output file with os.CreateTemp, write to it, close it,
	//   then call os.Rename(tmp.Name(), reduceName(...)) to atomically publish it.
	//   Use checkError to handle errors.

}

func ihash(s string) uint32 {
	h := fnv.New32a()
	h.Write([]byte(s))
	return h.Sum32()
}
