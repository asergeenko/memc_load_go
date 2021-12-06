package main

import (
    "flag"
    ua "memc_load/appsinstalled"
    "compress/gzip"
    "encoding/csv"
    "fmt"
    "log"
    "os"
    "github.com/bradfitz/gomemcache/memcache"
    "github.com/golang/protobuf/proto"
    "time"
    "path/filepath"
    "strings"
    "strconv"
    "sync"
    "io"
)

const NORMAL_ERR_RATE = 0.01
var FILE_DONE = []string{"file_done"}
var DONE = []string{"done"}

// protobuf message UserApps
type UserApps struct {
    apps       []uint32
    lat        float64
    lon        float64
}


type AppsInstalled struct {
    dev_type   string
    dev_id     string
    user_apps  UserApps 
}

func dot_rename(path string) {
    os.Rename(path, filepath.Join(filepath.Dir(path), "." + filepath.Base(path)))
}

func insert_appsinstalled(memc_client *memcache.Client, ai *AppsInstalled) bool {
   user_apps := ai.user_apps
   key := fmt.Sprintf("%s:%s", ai.dev_type, ai.dev_id)

   data, err := proto.Marshal(&ua.UserApps{Apps: user_apps.apps, Lat: &user_apps.lat, Lon: &user_apps.lon})
   if err == nil {
        if memc_client.Set(&memcache.Item{Key: key, Value: data}) == nil {
                    return true
                }
   }
   return false   
}

func parse_appsinstalled(line []string) AppsInstalled {
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

    for _, raw := range(strings.Split(line[4], ",")) {
                if app, err := strconv.ParseUint(raw, 10, 32); err == nil {
                    userapps.apps = append(userapps.apps, uint32(app))
                } else {
                  return appsinstalled
               }
            }
    appsinstalled.dev_type = line[0]
    appsinstalled.dev_id = line[1]

    appsinstalled.user_apps = userapps
    return appsinstalled
}

type LogPipeline struct {
    errors       uint
    processed    uint
    mc_clients   map[string]*memcache.Client
    device_memc  map[string] string
    wg_workers   sync.WaitGroup
    wg_file      sync.WaitGroup
    ch           chan []string
    mu           sync.Mutex
}

func set_connection(p *LogPipeline) {
    p.mc_clients = make(map[string]*memcache.Client, 4)
    for _, value := range(p.device_memc) {
        p.mc_clients[value] = memcache.New(value)
    }
}

func consumer(p *LogPipeline, idx int) {
    log.Printf("Consumer %d started", idx)
    for {
       line := <-p.ch
       if len(line) == 0 {
           break
       }
       if line[0] == DONE[0] {
           p.wg_workers.Done()
           continue
       }
       if line[0] == FILE_DONE[0] {
           p.wg_file.Done()
           continue
       }                 
       appsinstalled := parse_appsinstalled(line)
       if appsinstalled.dev_type == "" {
           p.mu.Lock()
           p.errors += 1
           p.mu.Unlock()
           continue
       }
       memc_addr, exists := p.device_memc[appsinstalled.dev_type]
       if !exists {
           p.mu.Lock()
           p.errors += 1
           p.mu.Unlock()
           log.Fatal("Unknown device type: %s", appsinstalled.dev_type)
           continue
       }
       ok := insert_appsinstalled(p.mc_clients[memc_addr],&appsinstalled)
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

func producer(p *LogPipeline, pattern *string, workers *int) {
    log.Printf("Producer started")
    files, _ := filepath.Glob(*pattern)
    for _, fd := range(files) {
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
                p.wg_file.Add(*workers)
                for i := 0; i < *workers; i ++ {
                   p.ch <- FILE_DONE
                }               
                break
            }
            p.ch <- rec
        }
        p.wg_file.Wait()
        if p.processed == 0 {
            gr.Close()
            f.Close()
            dot_rename(fd)
            continue
        }
        err_rate := float64(p.errors) / float64(p.processed)

        if err_rate < NORMAL_ERR_RATE {
            log.Printf("Acceptable error rate (%f). Successfull load", err_rate)
            log.Printf("Processed: %d, errors: %d", p.processed, p.errors)
        } else {
            log.Fatal("High error rate (%f > %f). Failed load", err_rate, NORMAL_ERR_RATE)
        }
        gr.Close()
        f.Close()
        dot_rename(fd)       
    }
    for i := 0; i < *workers; i++ {
        p.ch <- DONE
    }
    close(p.ch)
}

func run(p *LogPipeline, pattern *string, workers *int) {
    set_connection(p)
    p.wg_workers.Add(*workers)
    for i := 0; i < *workers; i ++ {
        go consumer(p, i)
    }
    go producer(p, pattern, workers)         
    p.wg_workers.Wait()
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

    file, err := os.OpenFile(*logfile, os.O_CREATE | os.O_APPEND | os.O_WRONLY, 0664)
    if err != nil {
        file = os.Stdin
        log.Printf("Error creating log file %s. Log will be forwarded to stdin", *logfile)
    }

    log.SetOutput(file)
    
    pipeline := LogPipeline{device_memc: device_memc, ch: make(chan []string,400)}
    run(&pipeline,pattern,workers)
    
    elapsed := time.Since(start)    
    log.Printf("Execution time: %s", elapsed)
}