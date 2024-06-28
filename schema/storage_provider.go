package schema

import (
	"KYVE-DLT/loader/collector"
	"KYVE-DLT/tools"
	"bytes"
	"compress/gzip"
	"crypto/sha256"
	"errors"
	"fmt"
	"net/http"
)

func downloadBundle(bundle collector.Bundle) (*bytes.Buffer, error) {

	baseUrl := ""
	switch bundle.StorageProviderId {
	case "1":
		baseUrl = "https://arweave.net"
	case "2":
		baseUrl = "https://arweave.net"
	case "3":
		baseUrl = "https://storage.kyve.network"
	}

	// Download bundle
	resp, err := http.Get(fmt.Sprintf("%s/%s", baseUrl, bundle.StorageId))
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	responseBuffer := new(bytes.Buffer)
	responseBuffer.ReadFrom(resp.Body)
	rawData := responseBuffer.Bytes()

	// Verify checksum
	tools.AwaitEnoughMemory("TODO", false)
	sha256hash := sha256.Sum256(rawData)
	if fmt.Sprintf("%x", sha256hash) != bundle.DataHash {
		return nil, errors.New("checksum does not match")
	}

	// uncompress gzip
	tools.AwaitEnoughMemory("TODO", false)
	reader, err := gzip.NewReader(responseBuffer)
	if err != nil {
		return nil, err
	}
	bundleBuffer := new(bytes.Buffer)
	bundleBuffer.ReadFrom(reader)

	return bundleBuffer, nil
}
