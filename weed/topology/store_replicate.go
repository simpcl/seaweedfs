package topology

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"strings"

	"weed/glog"
	"weed/operation"
	"weed/security"
	"weed/storage"
	"weed/util"
)

func ReplicatedWrite(masterNode string, s *storage.Store,
	volumeId storage.VolumeId, needle *storage.Needle,
	r *http.Request, overwrite bool) (size uint32, errorStatus string) {

	//check JWT
	jwt := security.GetJwt(r)

	needToReplicate := true
	if s.HasVolume(volumeId) {
		ret, err := s.Write(volumeId, needle, overwrite)
		if err != nil {
			errorStatus = "Failed to write to local disk: " + err.Error()
			return
		}
		if ret != needle.DataSize {
			errorStatus = fmt.Sprintf("Failed to write to local disk: ret %d != data size %d ", ret, needle.DataSize)
			return
		}
		glog.V(4).Infof("write needle %s successfuly", storage.NewFileIdFromNeedle(volumeId, needle).String())
		needToReplicate = s.GetVolume(volumeId).NeedToReplicate()
		size = ret
	}
	if !needToReplicate {
		return
	}
	if r.FormValue("type") == "replicate" {
		return
	}
	if err := distributedOperation(masterNode, s, volumeId, func(location operation.Location) error {
		u := url.URL{
			Scheme: "http",
			Host:   location.Url,
			Path:   r.URL.Path,
		}
		q := url.Values{
			"type": {"replicate"},
		}
		if needle.LastModified > 0 {
			q.Set("ts", strconv.FormatUint(needle.LastModified, 10))
		}
		if needle.IsChunkedManifest() {
			q.Set("cm", "true")
		}
		u.RawQuery = q.Encode()

		pairMap := make(map[string]string)
		if needle.HasPairs() {
			tmpMap := make(map[string]string)
			err := json.Unmarshal(needle.Pairs, &tmpMap)
			if err != nil {
				glog.V(0).Infoln("Unmarshal pairs error:", err)
			}
			for k, v := range tmpMap {
				pairMap[storage.PairNamePrefix+k] = v
			}
		}

		var err error
		if overwrite {
			_, err = operation.UploadWithPut(u.String(),
				string(needle.Name), bytes.NewReader(needle.Data), needle.IsGzipped(), string(needle.Mime),
				pairMap, jwt)
		} else {
			_, err = operation.Upload(u.String(),
				string(needle.Name), bytes.NewReader(needle.Data), needle.IsGzipped(), string(needle.Mime),
				pairMap, jwt)
		}
		return err
	}); err != nil {
		size = 0
		errorStatus = fmt.Sprintf("Failed to write to replicas for volume %d: %v", volumeId, err)
	}
	return
}

func ReplicatedDelete(masterNode string, store *storage.Store,
	volumeId storage.VolumeId, n *storage.Needle,
	r *http.Request) (uint32, error) {

	//check JWT
	jwt := security.GetJwt(r)

	ret, err := store.Delete(volumeId, n)
	if err != nil {
		glog.V(0).Infoln("delete error:", err)
		return ret, err
	}

	needToReplicate := !store.HasVolume(volumeId)
	if !needToReplicate && ret > 0 {
		needToReplicate = store.GetVolume(volumeId).NeedToReplicate()
	}
	if needToReplicate { //send to other replica locations
		if r.FormValue("type") != "replicate" {
			if err = distributedOperation(masterNode, store, volumeId, func(location operation.Location) error {
				return util.Delete("http://"+location.Url+r.URL.Path+"?type=replicate", jwt)
			}); err != nil {
				ret = 0
			}
		}
	}
	return ret, err
}

type DistributedOperationResult map[string]error

func (dr DistributedOperationResult) Error() error {
	var errs []string
	for k, v := range dr {
		if v != nil {
			errs = append(errs, fmt.Sprintf("[%s]: %v", k, v))
		}
	}
	if len(errs) == 0 {
		return nil
	}
	return errors.New(strings.Join(errs, "\n"))
}

type RemoteResult struct {
	Host  string
	Error error
}

func distributedOperation(masterNode string, store *storage.Store, volumeId storage.VolumeId, op func(location operation.Location) error) error {
	if lookupResult, lookupErr := operation.Lookup(masterNode, volumeId.String()); lookupErr == nil {
		length := 0
		selfUrl := (store.Ip + ":" + strconv.Itoa(store.Port))
		results := make(chan RemoteResult)
		for _, location := range lookupResult.Locations {
			if location.Url != selfUrl {
				length++
				go func(location operation.Location, results chan RemoteResult) {
					results <- RemoteResult{location.Url, op(location)}
				}(location, results)
			}
		}
		ret := DistributedOperationResult(make(map[string]error))
		for i := 0; i < length; i++ {
			result := <-results
			ret[result.Host] = result.Error
		}
		if volume := store.GetVolume(volumeId); volume != nil {
			if length+1 < volume.ReplicaPlacement.GetCopyCount() {
				return fmt.Errorf("replicating opetations [%d] is less than volume's replication copy count [%d]", length+1, volume.ReplicaPlacement.GetCopyCount())
			}
		}
		return ret.Error()
	} else {
		glog.V(0).Infoln()
		return fmt.Errorf("Failed to lookup for %d: %v", volumeId, lookupErr)
	}
	return nil
}
