package main

import (
	. "github.com/aerospike-labs/minion/service"

	"archive/tar"
	"bytes"
	"compress/gzip"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
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

	tgzReader := bytes.NewReader(tgz)

	gzipReader, err := gzip.NewReader(tgzReader)
	if err != nil {
		return err
	}

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
			dstBase := filepath.Dir(dstPath)
			os.MkdirAll(dstBase, 0755)

			if err = os.Chmod(dstPath, 0755); err != nil {
				return err
			}

			dst, err := os.Create(dstPath)
			if err != nil {
				return err
			}
			if _, err := io.Copy(dst, tarReader); err != nil {
				return err
			}
		case tar.TypeDir:
			os.MkdirAll(dstPath, 0755)
		}

	}

	// run aerospike init
	aerospikePath := filepath.Join(svcPath, "aerospike-server")
	aerospikeCommand := filepath.Join("bin", "aerospike")

	if err = os.Chmod(aerospikeCommand, 0755); err != nil {
		return err
	}

	cmd := exec.Command(aerospikeCommand, "init")
	cmd.Dir = aerospikePath
	out, err := cmd.CombinedOutput()
	println("out: ", string(out))
	if err != nil {
		return err
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
