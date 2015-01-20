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
	ErrorInvalidChecksum error = errors.New("Invalid Checksum")
	ErrorMissingVersion  error = errors.New("Missing 'version' Parameter")

	svcPath string = os.Getenv("SERVICE_PATH")

	statsMapper = map[string]func(m map[string]int) int{

		"read":              get("stats_read_req"),
		"read_ok":           get("stat_read_success"),
		"read_err":          sum(get("stat_read_errs_notfound"), get("stat_read_errs_other")),
		"read_err_notfound": get("stat_read_errs_notfound"),
		"read_err_other":    get("stat_read_errs_other"),

		"write":              get("stat_write_reqs"),
		"write_ok":           get("stat_write_success"),
		"write_err":          sum(get("stat_write_errs_notfound"), get("stat_write_errs_other")),
		"write_err_notfound": get("stat_write_errs_notfound"),
		"write_err_other":    get("stat_write_errs_other"),

		"objects_evicted": get("stat_evicted_objects"),
		"objects_expired": get("stat_expired_objects"),

		"proxy":     get("stat_proxy_reqs"),
		"proxy_ok":  get("stat_proxy_success"),
		"proxy_err": get("stat_proxy_errs"),

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

func get(k string) func(map[string]int) int {
	return func(m map[string]int) int {
		return m[k]
	}
}

func sum(vals ...func(m map[string]int) int) func(map[string]int) int {

	return func(m map[string]int) int {
		i := 0
		for _, val := range vals {
			i += val(m)
		}
		return i
	}
}

func (svc *AerospikeService) Stats() (map[string]interface{}, error) {

	var err error
	stats := map[string]interface{}{}

	conn, err := net.Dial("tcp", "localhost:3003")
	if err != nil {
		return stats, err
	}

	fmt.Fprintf(conn, "statistics\n")

	statistics, err := bufio.NewReader(conn).ReadString('\n')

	rawStats := map[string]int{}

	scanner := bufio.NewScanner(strings.NewReader(statistics))
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
			stats[k] = fn(rawStats)
		}
	}

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
	cmd.Dir = aerospikePath
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr

	err = cmd.Run()
	if err != nil {
		return "", "", err
	}

	outs := stdout.String()
	errs := stderr.String()

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
	Run(&AerospikeService{})
}
