package operation

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/url"

	"weed/util"
)

type AllocateVolumeResult struct {
	Error string
}

func AllocateVolume(dataNode string, volumeId string, collection string, replication string, ttl string, preallocate int64) error {
	values := make(url.Values)
	values.Add("volume", volumeId)
	values.Add("collection", collection)
	values.Add("replication", replication)
	values.Add("ttl", ttl)
	values.Add("preallocate", fmt.Sprintf("%d", preallocate))
	jsonBlob, err := util.Post("http://"+dataNode+"/admin/assign_volume", values)
	if err != nil {
		return fmt.Errorf("Post %s error: %w", dataNode, err)
	}
	var ret AllocateVolumeResult
	if err := json.Unmarshal(jsonBlob, &ret); err != nil {
		return fmt.Errorf("Invalid JSON result for %s: %s", "/admin/assign_volum", string(jsonBlob))
	}
	if ret.Error != "" {
		return errors.New(ret.Error)
	}
	return nil
}
