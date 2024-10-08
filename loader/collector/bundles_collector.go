package collector

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/KYVENetwork/KYVE-DLT/utils"
	"io/ioutil"
	"net/http"
	"strconv"
	"strings"
)

var (
	logger = utils.DltLogger("collector")
)

func NewSource(config SourceConfig) (Source, error) {

	if config.PoolId < 0 {
		return Source{}, errors.New("invalid pool-id")
	}

	if config.FromBundleId < 0 || config.FromBundleId > config.ToBundleId {
		return Source{}, errors.New("invalid from-bundle-id")
	}

	if config.ToBundleId < 0 {
		return Source{}, errors.New("invalid to-bundle-id")
	}

	if strings.HasSuffix(config.Endpoint, "/") {
		config.Endpoint = config.Endpoint[:len(config.Endpoint)-1]
	}

	return Source{
		poolId:       config.PoolId,
		fromBundleId: config.FromBundleId,
		toBundleId:   config.ToBundleId,
		batchSize:    config.BatchSize,
		endpoint:     config.Endpoint,
	}, nil
}

func (s Source) FetchBundles(ctx context.Context, offset int64, connectionName string, handler func(bundles []Bundle, err error)) {
	response, responseError := http.Get(
		fmt.Sprintf(
			"%s/kyve/v1/bundles/%d?pagination.limit=%d&pagination.offset=%d",
			s.endpoint,
			s.poolId,
			s.batchSize,
			offset,
		))
	if responseError != nil {
		handler(nil, fmt.Errorf("initial bundle request failed: %s", responseError.Error()))
	}
	defer response.Body.Close()

	// Handle Initial Bundle request
	initialBundles, paginationKey, bodyError := handleBody(response)
	if bodyError != nil {
		handler(nil, fmt.Errorf("initial bundle request failed: %s", responseError.Error()))
	}

	if len(initialBundles) == 0 {
		handler(nil, fmt.Errorf("could not find any bundles yet; from-bundle-id too high or cron interval too short"))
		return
	}

	highestBundleId, err := strconv.ParseInt(initialBundles[len(initialBundles)-1].Id, 10, 64)
	if err != nil {
		handler(nil, fmt.Errorf("malformed bundle response, invalid bundle-id: %s", err.Error()))
		return
	}

	if highestBundleId > s.toBundleId || paginationKey == "" {
		logger.Info().Str("connection", connectionName).Msg("reached last bundle")

		var bundles []Bundle
		for _, b := range initialBundles {
			select {
			// Graceful shutdown
			case <-ctx.Done():
				return
			default:
				bundleId, err := strconv.ParseInt(b.Id, 10, 64)
				if err != nil {
					handler(nil, fmt.Errorf("malformed bundle response, invalid bundle-id: %s", err.Error()))
					return
				}
				if bundleId <= s.toBundleId {
					bundles = append(bundles, b)
				} else {
					break
				}
			}
		}
		handler(bundles, nil)
		return
	} else {
		handler(initialBundles, nil)
	}

	// Iterate remaining bundles
	currentBundleId := offset
	for currentBundleId < s.toBundleId {
		select {
		// Graceful shutdown
		case <-ctx.Done():
			return
		default:
			newBundles, nextKey, err := s.fetch(paginationKey)
			if err != nil {
				handler(nil, err)
				continue
			}

			bundles := make([]Bundle, 0)
			for _, bundle := range newBundles {
				id, idErr := strconv.ParseUint(bundle.Id, 10, 64)
				if idErr != nil {
					handler(nil, fmt.Errorf("malformed bundle response, invalid bundle-id: %s", idErr.Error()))
					return
				}
				if int64(id) <= s.toBundleId {
					currentBundleId = int64(id)
					bundles = append(bundles, bundle)
				} else {
					break
				}
			}
			handler(bundles, nil)

			if nextKey == "" {
				logger.Info().Msg("reached last bundle")
				return
			}
			paginationKey = nextKey
		}
	}
}

func (s Source) fetch(paginationKey string) ([]Bundle, string, error) {
	response, responseError := http.Get(
		fmt.Sprintf(
			"%s/kyve/v1/bundles/%d?pagination.limit=%d&pagination.key=%s",
			s.endpoint,
			s.poolId,
			s.batchSize,
			strings.ReplaceAll(paginationKey, "+", "%2b"),
		))
	if responseError != nil {
		return nil, "", fmt.Errorf("bundle request failed: %s", responseError.Error())
	}
	if response.StatusCode != 200 {
		return nil, "", fmt.Errorf("invalid status code: %d", response.StatusCode)
	}
	defer response.Body.Close()

	return handleBody(response)
}

func handleBody(resp *http.Response) (bundles []Bundle, nextKey string, err error) {
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, "", fmt.Errorf("reading response body failed: %s", err.Error())
	}

	var data Response
	err = json.Unmarshal(body, &data)
	if err != nil {
		return nil, "", fmt.Errorf("parsing JSON failed: %s", err.Error())
	}

	return data.FinalizedBundles, data.Pagination.NextKey, nil
}
