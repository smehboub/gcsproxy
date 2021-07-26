package main

import (
	"context"
        "os"
	"flag"
	"io"
	"log"
	"net/http"
	"strconv"
	"time"

	"cloud.google.com/go/storage"
	"github.com/gorilla/mux"
	"google.golang.org/api/option"

        "io/ioutil"
        "path/filepath"

	"gopkg.in/yaml.v2"
	"github.com/daichirata/gcsproxy/headers"
)

var (
	bind        = flag.String("b", "127.0.0.1:8080", "Bind address")
	verbose     = flag.Bool("v", false, "Show access log")
	credentials = flag.String("c", "", "The path to the keyfile. If not present, client will use your default application credentials.")
)

var (
	client *storage.Client
	ctx    = context.Background()
)

func handleError(w http.ResponseWriter, err error) {
	if err != nil {
		if err == storage.ErrObjectNotExist {
			http.Error(w, "", http.StatusNotFound)
		} else {
			http.Error(w, "", http.StatusInternalServerError)
		}
		return
	}
}

func header(r *http.Request, key string) (string, bool) {
	if r.Header == nil {
		return "", false
	}
	if candidate := r.Header[key]; len(candidate) > 0 {
		return candidate[0], true
	}
	return "", false
}

func setStrHeader(w http.ResponseWriter, key string, value string) {
	if value != "" {
		w.Header().Add(key, value)
	}
}

func setIntHeader(w http.ResponseWriter, key string, value int64) {
	if value > 0 {
		w.Header().Add(key, strconv.FormatInt(value, 10))
	}
}

func setTimeHeader(w http.ResponseWriter, key string, value time.Time) {
	if !value.IsZero() {
		w.Header().Add(key, value.UTC().Format(http.TimeFormat))
	}
}

type wrapResponseWriter struct {
	http.ResponseWriter
	status int
}

func (w *wrapResponseWriter) WriteHeader(status int) {
	w.ResponseWriter.WriteHeader(status)
	w.status = status
}

type Config struct {
	Buckets map[string]string `yaml:"buckets"`
}


var config Config

func wrapper(fn func(w http.ResponseWriter, r *http.Request)) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		proc := time.Now()
		writer := &wrapResponseWriter{
			ResponseWriter: w,
			status:         http.StatusOK,
		}
		fn(writer, r)
		addr := r.RemoteAddr
		if ip, found := header(r, "X-Forwarded-For"); found {
			addr = ip
		}
		if *verbose {
			log.Printf("[%s] %.3f %d %s %s",
				addr,
				time.Now().Sub(proc).Seconds(),
				writer.status,
				r.Method,
				r.URL,
			)
		}
	}
}

func check(e error) {
	if e != nil {
		panic(e)
	}
}

func proxy(w http.ResponseWriter, r *http.Request) {
	params := mux.Vars(r)
	obj := client.Bucket(config.Buckets[r.Host]).Object(params["object"])

        err := headers.SetHeaders(ctx, obj, w)
        if err != nil {
		handleError(w, err)
		return
        }
	objr, err := obj.NewReader(ctx)
	if err != nil {
		handleError(w, err)
		return
	}
	io.Copy(w, objr)
}

func main() {
	flag.Parse()

	var err error
	if *credentials != "" {
		client, err = storage.NewClient(ctx, option.WithCredentialsFile(*credentials))
	} else {
		client, err = storage.NewClient(ctx)
	}
	if err != nil {
		log.Fatalf("Failed to create client: %v", err)
	}

        var buckets_config_path = os.Getenv("BUCKETS_CONFIG_PATH")

        if buckets_config_path == "" {
                log.Fatalf("empty BUCKETS_CONFIG_PATH variable")
        }

        filename, _ := filepath.Abs(buckets_config_path)

        yamlFile, err := ioutil.ReadFile(filename)
        check(err)
        err = yaml.Unmarshal(yamlFile, &config)
        check(err)

	r := mux.NewRouter()
	r.HandleFunc("/{object:.*}", wrapper(proxy)).Methods("GET", "HEAD")
	log.Printf("[service] listening on %s", *bind)
	if err := http.ListenAndServe(*bind, r); err != nil {
		log.Fatal(err)
	}
}
