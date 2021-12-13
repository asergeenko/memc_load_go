package main

import (
	"compress/gzip"
	"encoding/csv"
	"flag"
	"fmt"
	"io"
	"log"
	"math"
	"math/rand"
	ua "memc_load/appsinstalled"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/bradfitz/gomemcache/memcache"
	"github.com/golang/protobuf/proto"
)

const NORMAL_ERR_RATE = 0.01

var FILE_DONE = []string{"file_done"}
var DONE = []string{"done"}

const CONNECT_TIMEOUT = 5 * time.Second

// Constants below are for exponential backoff algorithm

const MIN_DELAY = 0.1
const MAX_DELAY = 15 * 60.0
const DELAY_FACTOR = 2.0
const DELAY_JITTER = 0.1
const NUM_TRIES = 3

// protobuf message UserApps
type UserApps struct {
	apps []uint32
	lat  float64
	lon  float64
}

type AppsInstalled struct {
	devType  string
	devId    string
	userApps UserApps
}

type LogPipeline struct {
	errors     uint
	processed  uint
	mcClients  map[string]*memcache.Client
	deviceMemc map[string]string
	wgWorkers  sync.WaitGroup
	wgFile     sync.WaitGroup
	ch         chan []string
	fileDone   chan int
	workDone   chan int
	mu         sync.Mutex
}

func SetWithRetries(memc_client *memcache.Client, item *memcache.Item) bool {
	delay := MIN_DELAY
	for i := 1; i <= NUM_TRIES; i++ {
		if memc_client.Set(item) == nil {
			return true
		} else {
			time.Sleep(time.Duration(delay * float64(time.Second)))
			delay = math.Min(delay*DELAY_FACTOR, MAX_DELAY)
			delay = math.Max(rand.NormFloat64()*DELAY_JITTER+delay, MIN_DELAY)
		}
	}
	log.Printf("Connection failed after %d tries", NUM_TRIES)
	return false
}

func DotRename(path string) {
	os.Rename(path, filepath.Join(filepath.Dir(path), "."+filepath.Base(path)))
}

func InsertAppsinstalled(memc_client *memcache.Client, ai *AppsInstalled) bool {
	user_apps := ai.userApps
	key := fmt.Sprintf("%s:%s", ai.devType, ai.devId)

	data, err := proto.Marshal(&ua.UserApps{Apps: user_apps.apps, Lat: &user_apps.lat, Lon: &user_apps.lon})
	if err == nil {
		return SetWithRetries(memc_client, &memcache.Item{Key: key, Value: data})
	}
	return false
}

func ParseAppsinstalled(line []string) AppsInstalled {
	var appsinstalled AppsInstalled
	var userapps UserApps

	if lat, err := strconv.ParseFloat(line[2], 64); err == nil {
		userapps.lat = lat
	} else {
		return appsinstalled
	}

	if lon, err := strconv.ParseFloat(line[3], 64); err == nil {
		userapps.lon = lon
	} else {
		return appsinstalled
	}

	for _, raw := range strings.Split(line[4], ",") {
		if app, err := strconv.ParseUint(raw, 10, 32); err == nil {
			userapps.apps = append(userapps.apps, uint32(app))
		} else {
			return appsinstalled
		}
	}
	appsinstalled.devType = line[0]
	appsinstalled.devId = line[1]

	appsinstalled.userApps = userapps
	return appsinstalled
}

func ProcessLines(p *LogPipeline, idx int) {
	log.Printf("Processor %d started", idx)
	for {
		select {
		case <-p.workDone:
			p.wgWorkers.Done()
		case <-p.fileDone:
			p.wgFile.Done()
		case line := <-p.ch:
			if len(line) == 0 {
				break
			}
			appsinstalled := ParseAppsinstalled(line)
			if appsinstalled.devType == "" {
				p.mu.Lock()
				p.errors += 1
				p.mu.Unlock()
				continue
			}
			memc_addr, exists := p.deviceMemc[appsinstalled.devType]
			if !exists {
				p.mu.Lock()
				p.errors += 1
				p.mu.Unlock()
				log.Printf("Unknown device type: %s", appsinstalled.devType)
				continue
			}
			ok := InsertAppsinstalled(p.mcClients[memc_addr], &appsinstalled)
			if ok {
				p.mu.Lock()
				p.processed += 1
				p.mu.Unlock()
			} else {
				p.mu.Lock()
				p.errors += 1
				p.mu.Unlock()
			}
		}
	}
}

func ReadFiles(p *LogPipeline, pattern *string, workers *int) {
	log.Printf("Producer started")
	files, _ := filepath.Glob(*pattern)
	for _, fd := range files {
		log.Printf("Processing %s", fd)
		p.processed = 0
		p.errors = 0

		f, _ := os.Open(fd)
		gr, _ := gzip.NewReader(f)
		cr := csv.NewReader(gr)
		cr.Comma = '\t'
		for {
			rec, err := cr.Read()
			if err == io.EOF {
				p.wgFile.Add(*workers)
				for i := 0; i < *workers; i++ {
					p.fileDone <- 0
				}
				break
			}
			p.ch <- rec
		}
		p.wgFile.Wait()
		if p.processed == 0 {
			gr.Close()
			f.Close()
			DotRename(fd)
			continue
		}
		err_rate := float64(p.errors) / float64(p.processed)

		if err_rate < NORMAL_ERR_RATE {
			log.Printf("Acceptable error rate (%f). Successfull load", err_rate)
			log.Printf("Processed: %d, errors: %d", p.processed, p.errors)
		} else {
			log.Printf("High error rate (%f > %f). Failed load", err_rate, NORMAL_ERR_RATE)
		}
		gr.Close()
		f.Close()
		DotRename(fd)
	}
	for i := 0; i < *workers; i++ {
		p.workDone <- 0
	}
	close(p.ch)
}

func main() {
	workers := flag.Int("workers", 5, "number of workers")
	logfile := flag.String("logfile", "memc_load.log", "log file path")
	pattern := flag.String("pattern", "./data/appsinstalled/*.tsv.gz", "source files pattern")
	idfa := flag.String("idfa", "127.0.0.1:33013", "ip:port memcached connection for idfa device")
	gaid := flag.String("gaid", "127.0.0.1:33014", "ip:port memcached connection for gaid device")
	adid := flag.String("adid", "127.0.0.1:33015", "ip:port memcached connection for adid device")
	dvid := flag.String("dvid", "127.0.0.1:33016", "ip:port memcached connection for dvid device")
	flag.Parse()

	start := time.Now()

	device_memc := map[string]string{"idfa": *idfa, "gaid": *gaid, "adid": *adid, "dvid": *dvid}

	file, err := os.OpenFile(*logfile, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0664)
	if err != nil {
		file = os.Stdin
		log.Printf("Error creating log file %s. Log will be forwarded to stdin", *logfile)
	}

	log.SetOutput(file)

	pipeline := LogPipeline{deviceMemc: device_memc, ch: make(chan []string, 400), fileDone: make(chan int), workDone: make(chan int)}

	pipeline.mcClients = make(map[string]*memcache.Client, 4)
	for _, value := range pipeline.deviceMemc {
		pipeline.mcClients[value] = memcache.New(value)
		pipeline.mcClients[value].Timeout = CONNECT_TIMEOUT
	}

	pipeline.wgWorkers.Add(*workers)
	for i := 0; i < *workers; i++ {
		go ProcessLines(&pipeline, i)
	}
	go ReadFiles(&pipeline, pattern, workers)
	pipeline.wgWorkers.Wait()

	elapsed := time.Since(start)
	log.Printf("Execution time: %s", elapsed)
}
