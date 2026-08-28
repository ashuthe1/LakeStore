// lakectl is the LakeStore command-line client.
//
//	lakectl --cluster 127.0.0.1:7101,127.0.0.1:7102,127.0.0.1:7103 put k v
//	lakectl get k
//	lakectl del k
//	lakectl scan [start_key] [limit]
//	lakectl status
//	lakectl bench --n 10000 --value-size 100 [--read]
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/ashuthe1/LakeStore/client"
)

func usage() {
	fmt.Fprintf(os.Stderr, `lakectl - LakeStore cluster client

Usage:
  lakectl [--cluster addr,addr,...] <command> [args]

Commands:
  put <key> <value>      Write a key
  get <key>              Read a key (consistent, leader-routed)
  get-stale <key>        Read a key from any node (may lag)
  del <key>              Delete a key
  scan [start] [limit]   List keys in order (default limit 100)
  status                 Show the status of every node
  bench                  Run a write/read benchmark (see bench --help)

Environment:
  LAKESTORE_CLUSTER      Default for --cluster
`)
	os.Exit(2)
}

func main() {
	defaultCluster := os.Getenv("LAKESTORE_CLUSTER")
	if defaultCluster == "" {
		defaultCluster = "127.0.0.1:7101,127.0.0.1:7102,127.0.0.1:7103"
	}
	clusterFlag := flag.String("cluster", defaultCluster, "comma-separated node addresses")
	flag.Usage = usage
	flag.Parse()

	args := flag.Args()
	if len(args) == 0 {
		usage()
	}

	addrs := strings.Split(*clusterFlag, ",")
	for i := range addrs {
		addrs[i] = strings.TrimSpace(addrs[i])
	}
	c := client.New(addrs)
	defer c.Close()
	ctx := context.Background()

	switch args[0] {
	case "put":
		if len(args) != 3 {
			fatal("usage: lakectl put <key> <value>")
		}
		must(c.Put(ctx, []byte(args[1]), []byte(args[2])))
		fmt.Println("OK")

	case "get", "get-stale":
		if len(args) != 2 {
			fatal("usage: lakectl " + args[0] + " <key>")
		}
		var val []byte
		var err error
		if args[0] == "get" {
			val, err = c.Get(ctx, []byte(args[1]))
		} else {
			val, err = c.GetStale(ctx, []byte(args[1]))
		}
		if errors.Is(err, client.ErrNotFound) {
			fmt.Fprintln(os.Stderr, "(not found)")
			os.Exit(1)
		}
		must(err)
		fmt.Println(string(val))

	case "del":
		if len(args) != 2 {
			fatal("usage: lakectl del <key>")
		}
		must(c.Delete(ctx, []byte(args[1])))
		fmt.Println("OK")

	case "scan":
		start := []byte(nil)
		limit := 100
		if len(args) >= 2 {
			start = []byte(args[1])
		}
		if len(args) >= 3 {
			n, err := strconv.Atoi(args[2])
			must(err)
			limit = n
		}
		pairs, err := c.Scan(ctx, start, limit)
		must(err)
		for _, kv := range pairs {
			fmt.Printf("%s\t%s\n", kv[0], kv[1])
		}
		fmt.Fprintf(os.Stderr, "(%d keys)\n", len(pairs))

	case "status":
		for _, addr := range addrs {
			st, err := c.Status(ctx, addr)
			if err != nil {
				fmt.Printf("%-22s UNREACHABLE (%v)\n", addr, shortErr(err))
				continue
			}
			fmt.Printf("%-22s node=%d %-8s term=%d leader=%d commit=%d applied=%d\n",
				addr, st.NodeId, st.State, st.Term, st.LeaderId, st.CommitIndex, st.LastApplied)
		}

	case "bench":
		benchCmd(c, args[1:])

	default:
		usage()
	}
}

func benchCmd(c *client.Client, args []string) {
	fs := flag.NewFlagSet("bench", flag.ExitOnError)
	n := fs.Int("n", 10000, "number of operations")
	valueSize := fs.Int("value-size", 100, "value size in bytes")
	conc := fs.Int("concurrency", 16, "concurrent workers")
	read := fs.Bool("read", false, "benchmark reads instead of writes (keys must exist)")
	fs.Parse(args)

	ctx := context.Background()
	value := []byte(strings.Repeat("v", *valueSize))

	type result struct {
		latencies []time.Duration
		errs      int
	}
	work := make(chan int, *n)
	for i := 0; i < *n; i++ {
		work <- i
	}
	close(work)

	results := make(chan result, *conc)
	start := time.Now()
	for w := 0; w < *conc; w++ {
		go func() {
			r := result{}
			for i := range work {
				key := []byte(fmt.Sprintf("bench_%08d", i))
				t0 := time.Now()
				var err error
				if *read {
					_, err = c.Get(ctx, key)
					if errors.Is(err, client.ErrNotFound) {
						err = nil
					}
				} else {
					err = c.Put(ctx, key, value)
				}
				if err != nil {
					r.errs++
				} else {
					r.latencies = append(r.latencies, time.Since(t0))
				}
			}
			results <- r
		}()
	}

	var all []time.Duration
	errs := 0
	for w := 0; w < *conc; w++ {
		r := <-results
		all = append(all, r.latencies...)
		errs += r.errs
	}
	elapsed := time.Since(start)

	sort.Slice(all, func(i, j int) bool { return all[i] < all[j] })
	pct := func(p float64) time.Duration {
		if len(all) == 0 {
			return 0
		}
		i := int(float64(len(all)) * p)
		if i >= len(all) {
			i = len(all) - 1
		}
		return all[i]
	}

	op := "writes"
	if *read {
		op = "reads"
	}
	fmt.Printf("%d %s in %v (%d errors)\n", len(all), op, elapsed.Round(time.Millisecond), errs)
	fmt.Printf("throughput: %.0f ops/sec\n", float64(len(all))/elapsed.Seconds())
	fmt.Printf("latency p50=%v p95=%v p99=%v max=%v\n",
		pct(0.50).Round(time.Microsecond), pct(0.95).Round(time.Microsecond),
		pct(0.99).Round(time.Microsecond), pct(1.0).Round(time.Microsecond))
}

func shortErr(err error) string {
	s := err.Error()
	if len(s) > 60 {
		return s[:60] + "..."
	}
	return s
}

func fatal(msg string) {
	fmt.Fprintln(os.Stderr, msg)
	os.Exit(2)
}

func must(err error) {
	if err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(1)
	}
}
