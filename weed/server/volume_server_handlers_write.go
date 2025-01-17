package weed_server

import (
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"

	"weed/glog"
	"weed/operation"
	"weed/storage"
	"weed/topology"
)

func (vs *VolumeServer) PostHandler(w http.ResponseWriter, r *http.Request) {
	if e := r.ParseForm(); e != nil {
		glog.V(0).Infoln("form parse error:", e)
		writeJsonError(w, r, http.StatusBadRequest, e)
		return
	}
	vid, _, _, _, _ := parseURLPath(r.URL.Path)
	volumeId, ve := storage.NewVolumeId(vid)
	if ve != nil {
		glog.V(0).Infoln("NewVolumeId error:", ve)
		writeJsonError(w, r, http.StatusBadRequest, ve)
		return
	}
	needle, ne := storage.NewNeedle(r)
	if ne != nil {
		writeJsonError(w, r, http.StatusBadRequest, ne)
		return
	}

	var overwrite = true
	if strings.EqualFold(r.Method, "POST") {
		overwrite = false
	} else if strings.EqualFold(r.Method, "PUT") {
		overwrite = true
	} else {
		writeJsonError(w, r, http.StatusBadRequest, errors.New("Wrong http method"))
		return
	}

	ret := operation.UploadResult{}
	size, errorStatus := topology.ReplicatedWrite(vs.GetMaster(),
		vs.store, volumeId, needle, r, overwrite)
	httpStatus := http.StatusCreated
	if errorStatus != "" {
		httpStatus = http.StatusInternalServerError
		ret.Error = errorStatus
	}
	if needle.HasName() {
		ret.Name = string(needle.Name)
	}
	ret.Size = size
	etag := needle.Etag()
	w.Header().Set("Etag", etag)
	writeJsonQuiet(w, r, httpStatus, ret)
}

func (vs *VolumeServer) DeleteHandler(w http.ResponseWriter, r *http.Request) {
	n := new(storage.Needle)
	vid, fid, _, _, _ := parseURLPath(r.URL.Path)
	volumeId, _ := storage.NewVolumeId(vid)
	n.ParsePath(fid)

	glog.V(2).Infoln("deleting", n)

	cookie := n.Cookie

	_, ok := vs.store.ReadVolumeNeedle(volumeId, n)
	if ok != nil {
		m := make(map[string]uint32)
		m["size"] = 0
		writeJsonQuiet(w, r, http.StatusNotFound, m)
		return
	}

	if n.Cookie != cookie {
		glog.V(0).Infoln("delete", r.URL.Path, "with unmaching cookie from ", r.RemoteAddr, "agent", r.UserAgent())
		writeJsonError(w, r, http.StatusBadRequest, errors.New("File Random Cookie does not match."))
		return
	}

	count := int64(n.Size)

	if n.IsChunkedManifest() {
		chunkManifest, e := operation.LoadChunkManifest(n.Data, n.IsGzipped())
		if e != nil {
			writeJsonError(w, r, http.StatusInternalServerError, fmt.Errorf("Load chunks manifest error: %v", e))
			return
		}
		// make sure all chunks had deleted before delete manifest
		if e := chunkManifest.DeleteChunks(vs.GetMaster()); e != nil {
			writeJsonError(w, r, http.StatusInternalServerError, fmt.Errorf("Delete chunks error: %v", e))
			return
		}
		count = chunkManifest.Size
	}

	n.LastModified = uint64(time.Now().Unix())
	if len(r.FormValue("ts")) > 0 {
		modifiedTime, err := strconv.ParseInt(r.FormValue("ts"), 10, 64)
		if err == nil {
			n.LastModified = uint64(modifiedTime)
		}
	}

	_, err := topology.ReplicatedDelete(vs.GetMaster(), vs.store, volumeId, n, r)

	if err == nil {
		m := make(map[string]int64)
		m["size"] = count
		writeJsonQuiet(w, r, http.StatusAccepted, m)
	} else {
		writeJsonError(w, r, http.StatusInternalServerError, fmt.Errorf("Deletion Failed: %v", err))
	}

}

//Experts only: takes multiple fid parameters. This function does not propagate deletes to replicas.
func (vs *VolumeServer) batchDeleteHandler(w http.ResponseWriter, r *http.Request) {
	r.ParseForm()
	var ret []operation.DeleteResult
	now := uint64(time.Now().Unix())
	for _, fid := range r.Form["fid"] {
		vid, id_cookie, err := operation.ParseFileId(fid)
		if err != nil {
			ret = append(ret, operation.DeleteResult{
				Fid:    fid,
				Status: http.StatusBadRequest,
				Error:  err.Error()})
			continue
		}
		n := new(storage.Needle)
		volumeId, _ := storage.NewVolumeId(vid)
		n.ParsePath(id_cookie)
		glog.V(4).Infoln("batch deleting", n)
		cookie := n.Cookie
		if _, err := vs.store.ReadVolumeNeedle(volumeId, n); err != nil {
			ret = append(ret, operation.DeleteResult{
				Fid:    fid,
				Status: http.StatusNotFound,
				Error:  err.Error(),
			})
			continue
		}

		if n.IsChunkedManifest() {
			ret = append(ret, operation.DeleteResult{
				Fid:    fid,
				Status: http.StatusNotAcceptable,
				Error:  "ChunkManifest: not allowed in batch delete mode.",
			})
			continue
		}

		if n.Cookie != cookie {
			ret = append(ret, operation.DeleteResult{
				Fid:    fid,
				Status: http.StatusBadRequest,
				Error:  "File Random Cookie does not match.",
			})
			glog.V(0).Infoln("deleting", fid, "with unmaching cookie from ", r.RemoteAddr, "agent", r.UserAgent())
			return
		}
		n.LastModified = now
		if size, err := vs.store.Delete(volumeId, n); err != nil {
			ret = append(ret, operation.DeleteResult{
				Fid:    fid,
				Status: http.StatusInternalServerError,
				Error:  err.Error()},
			)
		} else {
			ret = append(ret, operation.DeleteResult{
				Fid:    fid,
				Status: http.StatusAccepted,
				Size:   int(size)},
			)
		}
	}

	writeJsonQuiet(w, r, http.StatusAccepted, ret)
}
