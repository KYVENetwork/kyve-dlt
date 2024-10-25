package schema

import (
	"bytes"
	"compress/gzip"
	"crypto/sha256"
	"errors"
	"fmt"
	"github.com/KYVENetwork/KYVE-DLT/loader/collector"
	"github.com/KYVENetwork/KYVE-DLT/utils"
	"net/http"
)

func downloadBundle(bundle collector.Bundle, extra ExtraData) (DownloadResult, error) {

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
		return DownloadResult{}, err
	}
	defer resp.Body.Close()

	responseBuffer := new(bytes.Buffer)
	responseBuffer.ReadFrom(resp.Body)
	rawData := responseBuffer.Bytes()

	// Verify checksum
	utils.AwaitEnoughMemory(extra.Name)
	sha256hash := sha256.Sum256(rawData)
	if fmt.Sprintf("%x", sha256hash) != bundle.DataHash {
		return DownloadResult{}, errors.New("checksum does not match")
	}

	// uncompress gzip
	utils.AwaitEnoughMemory(extra.Name)
	reader, err := gzip.NewReader(responseBuffer)
	if err != nil {
		return DownloadResult{}, err
	}
	bundleBuffer := new(bytes.Buffer)
	bundleBuffer.ReadFrom(reader)

	return DownloadResult{
		Data:             bundleBuffer,
		CompressedSize:   int64(len(rawData)),
		UncompressedSize: int64(bundleBuffer.Len()),
	}, nil
}
