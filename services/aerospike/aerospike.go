package main

import (
	. "github.com/aerospike-labs/minion/service"

	"archive/tar"
	"bufio"
	"bytes"
	"compress/gzip"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"unicode/utf8"
)

const (
	AEROSPIKE_TGZ_URL string = "https://www.aerospike.com/artifacts/aerospike-server-community/%s/aerospike-server-community-%s.tar.gz"
	AEROSPIKE_SHA_URL string = "https://www.aerospike.com/artifacts/aerospike-server-community/%s/aerospike-server-community-%s.tar.gz.sha256"
)

var (
	host string = "localhost:3003"
)

var (
	ErrorInvalidChecksum error = errors.New("Invalid Checksum")
	ErrorMissingVersion  error = errors.New("Missing 'version' Parameter")

	svcPath string = os.Getenv("SERVICE_PATH")

	statsMapper = map[string]func(n string, m map[string]int) int{

		"memory_total":       get("total-bytes-memory"),
		"memory_used":        get("used-bytes-memory"),
		"memory_used_data":   get("data-used-bytes-memory"),
		"memory_used_index":  get("index-used-bytes-memory"),
		"memory_used_sindex": get("sindex-used-bytes-memory"),

		"disk_total": get("total-bytes-disk"),
		"disk_used":  get("used-bytes-disk"),

		"cluster_size": id(),

		"objects":         id(),
		"objects_expired": get("stat_expired_objects"),
		"objects_evicted": get("stat_evicted_objects"),

		"transactions":         get("transactions"),
		"transactions_waiting": get("waiting_transactions"),

		"proxy":      get("stat_proxy_reqs"),
		"proxy_ok":   get("stat_proxy_success"),
		"proxy_errs": get("stat_proxy_errs"),

		"migrate_msgs_sent":         id(),
		"migrate_msgs_recv":         id(),
		"migrate_progress_send":     id(),
		"migrate_progress_recv":     id(),
		"migrate_incoming_accepted": get("migrate_num_incoming_accepted"),
		"migrate_incoming_refused":  get("migrate_num_incoming_refused"),

		"read":              get("stat_read_reqs"),
		"read_ok":           get("stat_read_success"),
		"read_err":          sum(get("stat_read_errs_notfound"), get("stat_read_errs_other")),
		"read_err_notfound": get("stat_read_errs_notfound"),
		"read_err_other":    get("stat_read_errs_other"),

		"write":              get("stat_write_reqs"),
		"write_ok":           get("stat_write_success"),
		"write_err":          sum(get("stat_write_errs_notfound"), get("stat_write_errs_other")),
		"write_err_notfound": get("stat_write_errs_notfound"),
		"write_err_other":    get("stat_write_errs_other"),

		"query":       get("query_reqs"),
		"query_ok":    get("query_success"),
		"query_err":   get("query_fail"),
		"query_abort": get("query_abort"),

		"query_agg":       get("query_agg"),
		"query_agg_ok":    get("query_agg_success"),
		"query_agg_err":   get("query_agg_err"),
		"query_agg_abort": get("query_agg_abort"),

		"udf_lua_err": get("udf_lua_errs"),

		"udf_delete":     get("udf_delete_reqs"),
		"udf_delete_ok":  get("udf_delete_success"),
		"udf_delete_err": get("udf_delete_err_others"),

		"udf_read":     get("udf_read_reqs"),
		"udf_read_ok":  get("udf_read_success"),
		"udf_read_err": get("udf_read_errs_other"),

		"udf_write":     get("udf_write_reqs"),
		"udf_write_ok":  get("udf_write_success"),
		"udf_write_err": get("udf_write_err_others"),
	}
)

type AerospikeService struct{}

func (svc *AerospikeService) Install(params map[string]interface{}) error {

	var err error
	var tgz []byte
	var sha []byte

	// the following should come from `params`
	version, ok := params["version"]
	if !ok {
		return ErrorMissingVersion
	}

	// download the tgz
	tgzUrl := fmt.Sprintf(AEROSPIKE_TGZ_URL, version, version)
	tgzResp, err := http.Get(tgzUrl)
	defer tgzResp.Body.Close()
	if err != nil {
		return err
	} else {
		tgz, err = ioutil.ReadAll(tgzResp.Body)
	}

	// download the sha
	shaUrl := fmt.Sprintf(AEROSPIKE_SHA_URL, version, version)
	shaResp, err := http.Get(shaUrl)
	defer shaResp.Body.Close()
	if err != nil {
		return err
	} else {
		shaRaw, err := ioutil.ReadAll(shaResp.Body)
		if err != nil {
			return err
		}
		sha, err = hex.DecodeString(string(shaRaw[:64]))
		if err != nil {
			return err
		}
	}

	// compute checksum of tgz
	sum := sha256.Sum256(tgz)

	// are checksums equal?
	if !bytes.Equal(sha[:], sum[:]) {
		return ErrorInvalidChecksum
	}

	// checksums good, let's extract files

	tgzReader := bytes.NewReader(tgz)

	gzipReader, err := gzip.NewReader(tgzReader)
	if err != nil {
		return err
	}

	// workPath = filepath.Join(svcPath, )

	tarReader := tar.NewReader(gzipReader)
	for {
		hdr, err := tarReader.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}
		dstPath := filepath.Join(svcPath, hdr.Name)

		switch hdr.Typeflag {
		case tar.TypeReg | tar.TypeRegA:
			dstDir := filepath.Dir(dstPath)
			os.MkdirAll(dstDir, 0755)

			dst, err := os.Create(dstPath)
			if err != nil {
				return err
			}

			if _, err := io.Copy(dst, tarReader); err != nil {
				return err
			}
			dst.Close()

			if err = os.Chmod(dstPath, 0755); err != nil {
				return err
			}

		case tar.TypeDir:
			os.MkdirAll(dstPath, 0755)
		}

	}

	// run aerospike init
	aerospikePath := filepath.Join(svcPath, "aerospike-server")
	aerospikeCommand := filepath.Join(aerospikePath, "bin", "aerospike")

	cmd := exec.Command(aerospikeCommand, "init")
	cmd.Dir = filepath.Join(svcPath, "aerospike-server")
	_, err = cmd.CombinedOutput()
	if err != nil {
		return err
	}

	logpath := filepath.Join("aerospike-server", "var", "log")
	err = os.MkdirAll(logpath, 755)
	if err != nil {
		return err
	}

	runpath := filepath.Join("aerospike-server", "var", "run")
	os.MkdirAll(runpath, 755)
	err = os.MkdirAll(runpath, 755)
	if err != nil {
		return err
	}

	return nil
}

func (svc *AerospikeService) Remove() error {

	var err error

	// run aerospike destroy
	aerospikePath := filepath.Join(svcPath, "aerospike-server")
	aerospikeCommand := filepath.Join(aerospikePath, "bin", "aerospike")

	cmd := exec.Command(aerospikeCommand, "destroy")
	cmd.Dir = filepath.Join(svcPath, "aerospike-server")
	_, err = cmd.CombinedOutput()
	if err != nil {
		return err
	}

	os.RemoveAll(svcPath)

	return nil
}

func (svc *AerospikeService) Status() (Status, error) {
	stdout, _, err := svc.run("status")
	if err != nil {
		return StatusUnknown, err
	}

	if strings.Contains(stdout, "running") {
		return Running, nil
	} else {
		return Stopped, nil
	}
}

func (svc *AerospikeService) Start() error {

	// copy file from $CONFIG_PATH/aerospike.conf to
	// ./aerospike-server/etc/aerospike.conf

	var err error

	os.Setenv("AEROSPIKE_HOME", filepath.Join(svcPath, "aerospike-server"))

	src_path := os.ExpandEnv(filepath.Join("$CONFIG_PATH", "aerospike.conf"))
	dst_path := filepath.Join("aerospike-server", "etc", "aerospike.conf")

	if _, err := os.Stat(src_path); err != nil {
		return err
	}

	src_data, err := ioutil.ReadFile(src_path)
	if err != nil {
		return err
	}

	err = ioutil.WriteFile(dst_path, []byte(os.ExpandEnv(string(src_data))), 0755)
	if err != nil {
		return err
	}

	_, _, err = svc.run("start")
	return err
}

func (svc *AerospikeService) Stop() error {
	_, _, err := svc.run("stop")
	return err
}

func ScanPairs(data []byte, atEOF bool) (advance int, token []byte, err error) {
	start := 0
	// Scan until ';', marking end of word.
	for width, i := 0, start; i < len(data); i += width {
		var r rune
		r, width = utf8.DecodeRune(data[i:])
		if r == ';' {
			return i + width, data[start:i], nil
		}
	}
	// If we're at EOF, we have a final, non-empty, non-terminated word. Return it.
	if atEOF && len(data) > start {
		return len(data), data[start:], nil
	}
	// Request more data.
	return start, nil, nil
}

func id() func(string, map[string]int) int {
	return func(n string, m map[string]int) int {
		return m[n]
	}
}

func get(k string) func(string, map[string]int) int {
	return func(n string, m map[string]int) int {
		return m[k]
	}
}

func sum(vals ...func(n string, m map[string]int) int) func(string, map[string]int) int {

	return func(n string, m map[string]int) int {
		i := 0
		for _, val := range vals {
			i += val(n, m)
		}
		return i
	}
}

func histogramField(c rune) bool {
	return c == ',' || c == ';' || c == ':'
}

func statistics(conn net.Conn, stats map[string]interface{}) error {

	var err error
	var out string

	fmt.Fprintf(conn, "statistics\n")

	out, err = bufio.NewReader(conn).ReadString('\n')
	if err != nil {
		return err
	}

	rawStats := map[string]int{}

	scanner := bufio.NewScanner(strings.NewReader(out))
	scanner.Split(ScanPairs)
	for scanner.Scan() {
		pair := scanner.Text()
		parts := strings.SplitN(pair, "=", 2)
		rawStats[parts[0]], err = strconv.Atoi(parts[1])
	}

	if err := scanner.Err(); err != nil {
		fmt.Printf("error: Invalid input: %s", err)
	}

	for k, fn := range statsMapper {
		if fn == nil {
			stats[k] = rawStats[k]
		} else {
			stats[k] = fn(k, rawStats)
		}
	}

	return err
}

func processHistogram(out []byte, iStart, iNameEnd, iHeadersEnd, iValuesEnd int, stats map[string]interface{}) error {

	name := string(out[iStart:iNameEnd])

	sHeaders := string(out[iNameEnd+1 : iHeadersEnd])
	headers := strings.Split(sHeaders, ",")

	sValues := string(out[iHeadersEnd+1 : iValuesEnd])
	values := strings.Split(sValues, ",")

	for i, h := range headers {
		prefix := "latency:" + name
		switch i {
		case 0:
			stats[prefix+":start"] = h
			stats[name+":end"] = values[i]
		default:
			switch h[0] {
			case '<':
				stats[prefix+":lt:"+h[1:]] = values[i]
			case '>':
				stats[prefix+":gt:"+h[1:]] = values[i]
			default:
				stats[prefix+":"+h] = values[i]
			}
		}
	}

	return nil
}

func latency(conn net.Conn, stats map[string]interface{}) error {

	var err error
	var out []byte

	fmt.Fprintf(conn, "latency:\n")

	out, err = bufio.NewReader(conn).ReadBytes('\n')
	if err != nil {
		return err
	}

	//
	// OUTPUT:
	//
	// reads:21:54:10-GMT,ops/sec,>1ms,>8ms,>64ms;21:54:20,2335.3,0.51,0.00,0.00;
	// writes_master:21:54:10-GMT,ops/sec,>1ms,>8ms,>64ms;21:54:20,997.1,0.95,0.00,0.00;
	// proxy:21:54:10-GMT,ops/sec,>1ms,>8ms,>64ms;21:54:20,0.0,0.00,0.00,0.00;
	// writes_reply:21:54:10-GMT,ops/sec,>1ms,>8ms,>64ms;21:54:20,997.1,0.95,0.00,0.00;
	// udf:21:54:10-GMT,ops/sec,>1ms,>8ms,>64ms;21:54:20,0.0,0.00,0.00,0.00;
	// query:21:54:10-GMT,ops/sec,>1ms,>8ms,>64ms;21:54:20,3156.1,2.78,0.00,0.00;
	//

	iStart := 0
	iNameEnd := 0
	iHeadersEnd := 0
	iValuesEnd := 0

	for i, r := range out {
		switch {
		case iNameEnd == 0 && r == ':':
			iNameEnd = i
		case iHeadersEnd == 0 && r == ';':
			iHeadersEnd = i
		case iValuesEnd == 0 && r == ';':
			iValuesEnd = i
			processHistogram(out, iStart, iNameEnd, iHeadersEnd, iValuesEnd, stats)
			iStart = i + 1
			iNameEnd = 0
			iHeadersEnd = 0
			iValuesEnd = 0
		}
	}

	return err
}

func (svc *AerospikeService) Stats() (map[string]interface{}, error) {

	var err error
	stats := map[string]interface{}{}

	conn, err := net.Dial("tcp", host)
	if err != nil {
		return stats, err
	}

	// Process 'statistics'
	statistics(conn, stats)

	// Process 'latency:'
	latency(conn, stats)

	return stats, nil
}

// Run a Service Command
func (svc *AerospikeService) run(commandName string) (string, string, error) {

	var stdout bytes.Buffer
	var stderr bytes.Buffer
	var cmd *exec.Cmd
	var err error

	aerospikePath := filepath.Join(svcPath, "aerospike-server")
	aerospikeCommand := filepath.Join(aerospikePath, "bin", "aerospike")

	cmd = exec.Command(aerospikeCommand, commandName)
	cmd.Env = os.Environ()
	cmd.Env = append(cmd.Env, "TERM=dumb")
	cmd.Dir = aerospikePath
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr

	err = cmd.Run()
	outs := stdout.String()
	errs := stderr.String()

	if err != nil {
		fmt.Println("err: ", err.Error())
	}

	if len(errs) > 0 {
		fmt.Println("err: ", errs)
	}
	if len(outs) > 0 {
		fmt.Println("out: ", outs)
	}

	return outs, errs, err
}

// Main - should call service.Run, to run the service,
// and process the commands and arguments.
func main() {
	flag.StringVar(&host, "host", host, "Aerospike address and port.")
	flag.Parse()

	Run(&AerospikeService{})
}
