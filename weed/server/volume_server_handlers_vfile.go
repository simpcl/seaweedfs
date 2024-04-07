package weed_server

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"os"
	"path/filepath"

	"weed/glog"
	"weed/operation"
	"weed/storage"
	"weed/util"
)

type VolReq struct {
	Collection string `json:"collection"`
	VolumeId   int    `json:"volumeId"`
	RemoteAddr string `json:"remoteAddr"`
	WithIdx    bool   `json:"withIdx"`
	MaxRetries int    `json:"maxRetries"`
}

func (vs *VolumeServer) fetchVolumeFileHandler(w http.ResponseWriter, r *http.Request) {
	// parse request
	var volReq VolReq
	body, err := ioutil.ReadAll(r.Body)
	if err != nil {
		glog.V(0).Infof("read body error: %v", err)
		writeJsonError(w, r, http.StatusBadRequest, err)
		return
	}
	err = json.Unmarshal(body, &volReq)
	if err != nil {
		glog.V(0).Infof("decode json error: %v", err)
		writeJsonError(w, r, http.StatusBadRequest, err)
		return
	}
	// volReq check
	if volReq.RemoteAddr == "" {
		err = fmt.Errorf("remoteAddr should not be empty")
		glog.V(0).Info(err)
		writeJsonError(w, r, http.StatusBadRequest, err)
		return
	}
	if volReq.RemoteAddr == vs.store.PublicUrl {
		err = fmt.Errorf("remoteAddr can not equal to current weedServer")
		glog.V(0).Info(err)
		writeJsonError(w, r, http.StatusBadRequest, err)
		return
	}
	if volReq.MaxRetries <= 0 {
		volReq.MaxRetries = 1
	}

	vid := storage.VolumeId(volReq.VolumeId)

	fpaths := []string{}
	suffixes := []string{"dat"}
	if volReq.WithIdx {
		suffixes = append(suffixes, "idx")
	}
	for _, suffix := range suffixes {
		exist, localFilePath, err := vs.store.Locations[0].CheckVolumeFileExist(volReq.Collection, vid, suffix)
		if err != nil {
			e := fmt.Errorf("check volume file %s exist error: %s", localFilePath, err.Error())
			glog.V(0).Info(e)
			writeJsonError(w, r, http.StatusInternalServerError, e)
			return
		} else if exist {
			e := fmt.Errorf("check volume file %s exist: already exist", localFilePath)
			glog.V(0).Info(e)
			writeJsonError(w, r, http.StatusBadRequest, e)
			return
		}

		filename := filepath.Base(localFilePath)
		glog.V(0).Infof("fetching volume file %s", filename)
		err = operation.DownloadVolumeFile(filename, localFilePath+".dwn", volReq.RemoteAddr, volReq.MaxRetries)
		if err != nil {
			e := fmt.Errorf("download volume file %s error: %s", filename, err.Error())
			glog.V(0).Info(e)
			writeJsonError(w, r, http.StatusInternalServerError, e)
			util.RemoveFile(localFilePath + ".dwn")
			return
		}
		fpaths = append(fpaths, localFilePath)
	}
	for _, fpath := range fpaths {
		err = os.Rename(fpath+".dwn", fpath)
		if err != nil {
			e := fmt.Errorf("rename volume file %s to %s error: %s", fpath+".dwn", fpath, err.Error())
			glog.V(0).Info(e)
			writeJsonError(w, r, http.StatusInternalServerError, e)
			return
		}
	}

	// loading
	err = vs.store.MountVolume(vid)
	if err != nil {
		glog.V(0).Infof("mount volume %d error: %s", volReq.VolumeId, err.Error())
		writeJsonError(w, r, http.StatusInternalServerError, err)
		return
	}

	writeJsonQuiet(w, r, http.StatusOK, "Volume file fetched")
}

func (vs *VolumeServer) downloadVolumeFileHandler(w http.ResponseWriter, r *http.Request) {
	filename := r.FormValue("file")
	localFilePath, contentMd5, err := vs.store.Locations[0].GetVolumeFilePath(filename)
	if err != nil {
		e := fmt.Errorf("download volume file %s error: %v", filename, err)
		glog.V(0).Info(e)
		return
	}

	glog.V(0).Infof("http serve file: %s, md5: %s", localFilePath, contentMd5)
	w.Header().Set("Content-Disposition", "attachment; filename="+filename)
	w.Header().Set("Content-Type", "application/octet-stream")
	w.Header().Set("Content-Md5", contentMd5)

	http.ServeFile(w, r, localFilePath)
	return
}

func (vs *VolumeServer) deleteVolumeFileHandler(w http.ResponseWriter, r *http.Request) {
	filename := r.FormValue("file")
	forceStr := r.FormValue("force")
	force := false
	if forceStr == "true" {
		force = true
	}

	err := vs.store.Locations[0].DeleteVolumeFile(filename, force)
	if err != nil {
		e := fmt.Errorf("delete volume file %s error: %s", filename, err.Error())
		glog.V(0).Info(e)
		writeJsonError(w, r, http.StatusInternalServerError, e)
		return
	}

	glog.V(0).Infof("volume file %s deleted", filename)
	writeJsonQuiet(w, r, http.StatusOK, "Volume file deleted")
	return
}
