package main

import (
	. "github.com/aerospike-labs/minion/service"

	"bytes"
	"crypto/sha256"
	"errors"
	"fmt"
	"io/ioutil"
	"net/http"
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
	fmt.Printf("tgz: %s\n", tgzUrl)
	tgzResp, err := http.Get(tgzUrl)
	defer tgzResp.Body.Close()
	if err != nil {
		return err
	} else {
		tgz, err = ioutil.ReadAll(tgzResp.Body)
	}

	// download the sha
	shaUrl := fmt.Sprintf(AEROSPIKE_SHA_URL, version, version)
	fmt.Printf("sha: %s\n", shaUrl)
	shaResp, err := http.Get(shaUrl)
	defer shaResp.Body.Close()
	if err != nil {
		return err
	} else {
		sha, err = ioutil.ReadAll(shaResp.Body)
	}

	// compute checksum of tgz
	sum := sha256.Sum256(tgz)

	fmt.Printf("sha: %X\n", sha)
	fmt.Printf("sum: %X\n", sum)

	// are checksums equal?
	if !bytes.Equal(sha, sum[:]) {
		return ErrorInvalidChecksum
	}

	println("shit yeah")

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
