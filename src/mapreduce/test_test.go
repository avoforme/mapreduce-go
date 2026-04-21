package mapreduce

import (
	"fmt"
	"testing"
	"time"

	"bufio"
	"log"
	"os"
	"sort"
	"strconv"
	"syscall"
	"strings"
)

const (
	nNumber = 100000 // total integers written across all input files
	nMap    = 100    // number of map tasks (one input file per task)
	nReduce = 50     // number of reduce tasks
)

// MapFunc is the test map function. It splits the file contents into
// whitespace-separated tokens and emits each token as a key with an empty value.
// The keys (integers as strings) are later used by check() to verify output completeness.
func MapFunc(file string, value string) (res []KeyValue) {
	debug("Map %v\n", value)
	words := strings.Fields(value)
	for _, w := range words {
		kv := KeyValue{w, ""}
		res = append(res, kv)
	}
	return
}

// ReduceFunc is the test reduce function. It simply returns an empty string,
// preserving the key in the output. The values are ignored because the test
// only checks that every input key appears in the output, not the values.
func ReduceFunc(key string, values []string) string {
	for _, e := range values {
		debug("Reduce %s %v\n", key, e)
	}
	return ""
}

// check verifies the correctness of a completed MapReduce job.
// It reads all input files to collect every integer that was written, sorts them,
// then scans the merged output file (mrtmp.<jobName>) line by line and confirms each
// integer appears exactly once in sorted order. The test fails if any integer is
// missing or out of order, which would indicate that doMap or doReduce dropped data.
func check(t *testing.T, files []string, jobName string) {
	output, err := os.Open("mrtmp." + jobName)
	if err != nil {
		log.Fatal("check: ", err)
	}
	defer output.Close()

	var lines []string
	for _, f := range files {
		input, err := os.Open(f)
		if err != nil {
			log.Fatal("check: ", err)
		}
		defer input.Close()
		inputScanner := bufio.NewScanner(input)
		for inputScanner.Scan() {
			lines = append(lines, inputScanner.Text())
		}
	}

	sort.Strings(lines)

	outputScanner := bufio.NewScanner(output)
	i := 0
	for outputScanner.Scan() {
		var v1 int
		var v2 int
		text := outputScanner.Text()
		n, err := fmt.Sscanf(lines[i], "%d", &v1)
		if n == 1 && err == nil {
			n, err = fmt.Sscanf(text, "%d", &v2)
		}
		if err != nil || v1 != v2 {
			t.Fatalf("line %d: %d != %d err %v\n", i, v1, v2, err)
		}
		i++
	}
	if i != nNumber {
		t.Fatalf("Expected %d lines in output\n", nNumber)
	}
}

// checkWorker verifies that every worker that was still alive at shutdown
// processed at least one task. A zero count means a worker registered but
// was never assigned work, which would indicate a bug in schedule().
func checkWorker(t *testing.T, l []int) {
	for _, tasks := range l {
		if tasks == 0 {
			t.Fatalf("Some worker didn't do any work\n")
		}
	}
}

// makeInputs creates num input files, each containing an equal share of the
// integers 0..nNumber-1 (one integer per line), and returns their file names.
func makeInputs(num int) []string {
	var names []string
	var i = 0
	for f := 0; f < num; f++ {
		names = append(names, fmt.Sprintf("824-mrinput-%d.txt", f))
		file, err := os.Create(names[f])
		if err != nil {
			log.Fatal("mkInput: ", err)
		}
		w := bufio.NewWriter(file)
		for i < (f+1)*(nNumber/num) {
			fmt.Fprintf(w, "%d\n", i)
			i++
		}
		w.Flush()
		file.Close()
	}
	return names
}

// port returns a unique Unix domain socket path for the given suffix.
// The path encodes the current user ID and process ID so that concurrent
// test runs on the same machine do not collide with each other.
// Example: /var/tmp/824-1000/mr12345-master
func port(suffix string) string {
	s := "/var/tmp/824-"
	s += strconv.Itoa(os.Getuid()) + "/"
	os.Mkdir(s, 0777)
	s += "mr"
	s += strconv.Itoa(os.Getpid()) + "-"
	s += suffix
	return s
}

// setup creates nMap input files and starts a distributed MapReduce master
// listening on a Unix socket. Workers are started separately by each test.
func setup() *Master {
	files := makeInputs(nMap)
	master := port("master")
	mr := Distributed("test", files, nReduce, master)
	return mr
}

// cleanup removes all intermediate and output files produced by the job,
// as well as the original input files.
func cleanup(mr *Master) {
	mr.CleanupFiles()
	for _, f := range mr.files {
		removeFile(f)
	}
}

// Part A: Sequential MapReduce framework (20 points)

// TestSequentialSingle tests the simplest case: 1 map task and 1 reduce task.
func TestSequentialSingle(t *testing.T) {
	mr := Sequential("test", makeInputs(1), 1, MapFunc, ReduceFunc)
	mr.Wait()
	check(t, mr.files, mr.jobName)
	checkWorker(t, mr.stats)
	cleanup(mr)
}

// TestSequentialMany tests sequential execution with multiple map and reduce tasks.
func TestSequentialMany(t *testing.T) {
	mr := Sequential("test", makeInputs(5), 3, MapFunc, ReduceFunc)
	mr.Wait()
	check(t, mr.files, mr.jobName)
	checkWorker(t, mr.stats)
	cleanup(mr)
}

// Part D: Distributed MapReduce (21 points)
// Run with: go test -run Distributed mr/mapreduce

// TestDistributedBasic tests distributed execution with 2 workers and no failures.
func TestDistributedBasic(t *testing.T) {
	mr := setup()
	for i := 0; i < 2; i++ {
		go RunWorker(mr.address, port("worker"+strconv.Itoa(i)),
			MapFunc, ReduceFunc, -1)
	}
	mr.Wait()
	check(t, mr.files, mr.jobName)
	checkWorker(t, mr.stats)
	cleanup(mr)
}

// TestDistributedConcurrent runs the same job as TestDistributedBasic but with 4 workers.
// checkWorker verifies every worker got at least one task, catching
// schedule() implementations that dispatch tasks to only a subset of
// available workers (e.g., always reusing the same worker).
func TestDistributedConcurrent(t *testing.T) {
	mr := setup()
	for i := 0; i < 4; i++ {
		go RunWorker(mr.address, port("worker"+strconv.Itoa(i)),
			MapFunc, ReduceFunc, -1)
	}
	mr.Wait()
	check(t, mr.files, mr.jobName)
	checkWorker(t, mr.stats)
	cleanup(mr)
}

// parallelDelay is the per-task sleep injected by DelayedMapFunc.
// It must be long enough to make the serial/parallel gap clearly measurable,
// but short enough that the test suite stays fast.
const parallelDelay = 1 * time.Second

// DelayedMapFunc wraps MapFunc with a sleep to produce a measurable task duration.
// It is used by TestDistributedParallel to distinguish a truly concurrent scheduler from
// one that dispatches tasks one at a time.
func DelayedMapFunc(file string, value string) []KeyValue {
	time.Sleep(parallelDelay)
	return MapFunc(file, value)
}

// TestDistributedParallel verifies that schedule() dispatches tasks concurrently.
// It uses 4 workers and 4 map tasks that each sleep parallelDelay.
// With true parallel dispatch, all 4 tasks run simultaneously and the job
// completes in roughly parallelDelay. A serial scheduler would take ~4x
// parallelDelay, failing the timing assertion.
// checkWorker additionally verifies that every worker received at least one task.
func TestDistributedParallel(t *testing.T) {
	const nParallelMap = 4
	const nParallelReduce = 2

	files := makeInputs(nParallelMap)
	mr := Distributed("test", files, nParallelReduce, port("master"))

	for i := 0; i < 4; i++ {
		go RunWorker(mr.address, port("worker"+strconv.Itoa(i)),
			DelayedMapFunc, ReduceFunc, -1)
	}

	start := time.Now()
	mr.Wait()
	elapsed := time.Since(start)

	// Parallel: ~parallelDelay. Serial: ~4×parallelDelay.
	// Allow 3× headroom for scheduling, I/O, and race-detector overhead.
	if elapsed >= 3*parallelDelay {
		t.Fatalf("Job took %v; concurrent dispatch should keep it under %v",
			elapsed, 3*parallelDelay)
	}
	check(t, mr.files, mr.jobName)
	checkWorker(t, mr.stats)
	cleanup(mr)
}

// Part E: Handling Worker Failures (15 points)

// TestOneFailure tests that the master reassigns tasks when one of two workers
// fails mid-job. Worker 0 dies after 10 tasks; worker 1 runs to completion.
// Run with: go test -run Fail
func TestOneFailure(t *testing.T) {
	mr := setup()
	// worker 0 fails after 10 tasks; worker 1 runs indefinitely
	go RunWorker(mr.address, port("worker"+strconv.Itoa(0)),
		MapFunc, ReduceFunc, 10)
	go RunWorker(mr.address, port("worker"+strconv.Itoa(1)),
		MapFunc, ReduceFunc, -1)
	mr.Wait()
	check(t, mr.files, mr.jobName)
	checkWorker(t, mr.stats)
	cleanup(mr)
}

func TestManyFailures(t *testing.T) {
	// Raise the open-file limit for this process to avoid "too many open files"
	// errors when many short-lived workers open and close Unix sockets rapidly.
	var rl syscall.Rlimit
	if syscall.Getrlimit(syscall.RLIMIT_NOFILE, &rl) == nil && rl.Cur < 4096 {
		rl.Cur = 4096
		syscall.Setrlimit(syscall.RLIMIT_NOFILE, &rl)
	}
	mr := setup()
	i := 0
	done := false
	for !done {
		select {
		case done = <-mr.doneChannel:
			check(t, mr.files, mr.jobName)
			cleanup(mr)
		default:
			// Start 2 workers every two secs. The workers fail after 20 tasks
			w := port("worker" + strconv.Itoa(i))
			go RunWorker(mr.address, w, MapFunc, ReduceFunc, 20)
			i++
			w = port("worker" + strconv.Itoa(i))
			go RunWorker(mr.address, w, MapFunc, ReduceFunc, 20)
			i++
			time.Sleep(2 * time.Second)
		}
	}
}

// TestAllWorkersFail starts two workers that both die after 10 tasks (20 total
// out of 150 tasks: 100 map + 50 reduce), leaving the job incomplete.
// Two fresh unlimited workers are injected after 2 seconds to finish the job.
// Unlike TestOneFailure, there is no surviving initial worker: the master must
// correctly stall and recover when the worker pool is temporarily empty.
func TestAllWorkersFail(t *testing.T) {
	mr := setup()
	go RunWorker(mr.address, port("worker0"), MapFunc, ReduceFunc, 10)
	go RunWorker(mr.address, port("worker1"), MapFunc, ReduceFunc, 10)
	go func() {
		time.Sleep(2 * time.Second)
		go RunWorker(mr.address, port("worker2"), MapFunc, ReduceFunc, -1)
		go RunWorker(mr.address, port("worker3"), MapFunc, ReduceFunc, -1)
	}()
	mr.Wait()
	check(t, mr.files, mr.jobName)
	checkWorker(t, mr.stats)
	cleanup(mr)
}

// Part F: Straggler Handling (10 points)
// Run with: go test -run Straggler mr/mapreduce

// stragglerDelay is the artificial slowdown injected into SlowMapFunc.
// It must be long enough that a job without backup tasks would clearly exceed
// the test's timeout, but short enough that the overall test suite stays fast.
const stragglerDelay = 5 * time.Second

// SlowMapFunc wraps MapFunc with a sleep to simulate a straggler worker.
// The output is identical to MapFunc; only the latency differs.
func SlowMapFunc(file string, value string) []KeyValue {
	time.Sleep(stragglerDelay)
	return MapFunc(file, value)
}

// TestStraggler verifies that schedule() launches backup tasks for slow workers.
// It uses a small job (4 map tasks, 2 reduce tasks) with two workers: one fast
// and one slow (sleeps stragglerDelay per map task). Without backup tasks the
// job would take at least stragglerDelay; with backup tasks the fast worker
// picks up the straggler's task and the job completes well within the timeout.
// Run with: go test -run Straggler mr/mapreduce
func TestStraggler(t *testing.T) {
	const nStragglerMap = 4
	const nStragglerReduce = 2

	files := makeInputs(nStragglerMap)
	mr := Distributed("straggler", files, nStragglerReduce, port("master"))

	// worker0 is slow for map tasks; worker1 is fast for both phases.
	go RunWorker(mr.address, port("worker0"), SlowMapFunc, ReduceFunc, -1)
	go RunWorker(mr.address, port("worker1"), MapFunc, ReduceFunc, -1)

	start := time.Now()
	mr.Wait()
	elapsed := time.Since(start)

	// With backup tasks the fast worker should absorb the straggler's task
	// well before stragglerDelay elapses.
	if elapsed >= stragglerDelay {
		t.Fatalf("Job took %v; straggler handling should keep it under %v",
			elapsed, stragglerDelay)
	}
	check(t, mr.files, mr.jobName)
	checkWorker(t, mr.stats)
	cleanup(mr)
}
