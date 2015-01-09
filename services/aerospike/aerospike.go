package main

import (
	. "github.com/aerospike-labs/minion/service"

	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io/ioutil"
	"net/http"
	"os"
	"path/filepath"
	// "os/exec"
	// "strings"
)

const (
	AEROSPIKE_TGZ_URL string = "https://www.aerospike.com/artifacts/aerospike-server-community/%s/aerospike-server-community-%s.tar.gz"
	AEROSPIKE_SHA_URL string = "https://www.aerospike.com/artifacts/aerospike-server-community/%s/aerospike-server-community-%s.tar.gz.sha256"
)

var (
	ErrorInvalidChecksum error = errors.New("Invalid Checksum")
	ErrorMissingVersion  error = errors.New("Missing 'version' Parameter")
)

type AerospikeService struct{}

func (b *AerospikeService) Install(params map[string]interface{}) error {

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

	svcPath := os.Getenv("SERVICE_PATH")

	r, err := zip.NewReader(bytes.NewReader(tgz), tgzResp.ContentLength)
	if err != nil {
		return err
	}
	for _, zf := range r.File {
		dstPath := filepath.Join(svcPath, zf.Name)
		dst, err := os.Create(dstPath)
		if err != nil {
			return err
		}
		defer dst.Close()
		src, err := zf.Open()
		if err != nil {
			return err
		}
		defer src.Close()

		io.Copy(dst, src)
	}

	return nil
}

func (b *AerospikeService) Remove() error {
	return nil
}

func (b *AerospikeService) Status() (Status, error) {
	return Running, nil
}

func (b *AerospikeService) Start() error {
	return nil
}

func (b *AerospikeService) Stop() error {
	return nil
}

func (b *AerospikeService) Stats() (map[string]interface{}, error) {
	return map[string]interface{}{}, nil
}

// Bundle Main - should call BundleRun, to run the bundle,
// and process the commands and arguments.
func main() {
	Run(&AerospikeService{})
}
