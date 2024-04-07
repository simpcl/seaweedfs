package storage

import (
	"errors"
	"io/ioutil"
	"os"
	"strings"
	"sync"

	"fmt"

	"weed/glog"
	"weed/util"
)

type DiskLocation struct {
	Directory      string
	MaxVolumeCount int
	volumes        map[VolumeId]*Volume
	sync.RWMutex
}

func NewDiskLocation(dir string, maxVolumeCount int) *DiskLocation {
	location := &DiskLocation{Directory: dir, MaxVolumeCount: maxVolumeCount}
	location.volumes = make(map[VolumeId]*Volume)
	return location
}

func (l *DiskLocation) volumeIdFromPath(dir os.FileInfo) (VolumeId, string, error) {
	name := dir.Name()
	if !dir.IsDir() && (strings.HasSuffix(name, ".dat") || strings.HasSuffix(name, ".idx")) {
		collection := ""
		base := name[:len(name)-len(".dat")]
		i := strings.LastIndex(base, "_")
		if i > 0 {
			collection, base = base[0:i], base[i+1:]
		}
		vol, err := NewVolumeId(base)
		return vol, collection, err
	}

	return 0, "", fmt.Errorf("Path is not a volume: %s", name)
}

func (l *DiskLocation) loadExistingVolume(dir os.FileInfo, needleMapKind NeedleMapType, mutex *sync.RWMutex) {
	name := dir.Name()
	if !dir.IsDir() && strings.HasSuffix(name, ".dat") {
		vid, collection, err := l.volumeIdFromPath(dir)
		if err == nil {
			mutex.RLock()
			_, found := l.volumes[vid]
			mutex.RUnlock()
			if !found {
				if v, e := NewVolume(l.Directory, collection, vid, needleMapKind, nil, nil, 0); e == nil {
					mutex.Lock()
					l.volumes[vid] = v
					mutex.Unlock()
					glog.V(0).Infof("data file %s, replicaPlacement=%s v=%d size=%d ttl=%s",
						l.Directory+"/"+name, v.ReplicaPlacement, v.Version(), v.Size(), v.Ttl.String())
				} else {
					glog.V(0).Infof("new volume %s error %s", name, e)
				}
			}
		}
	}
}

func (l *DiskLocation) concurrentLoadingVolumes(needleMapKind NeedleMapType, concurrentFlag bool) {
	var concurrency int
	if concurrentFlag {
		//You could choose a better optimized concurency value after testing at your environment
		concurrency = 10
	} else {
		concurrency = 1
	}

	task_queue := make(chan os.FileInfo, 10*concurrency)
	go func() {
		if dirs, err := ioutil.ReadDir(l.Directory); err == nil {
			for _, dir := range dirs {
				task_queue <- dir
			}
		}
		close(task_queue)
	}()

	var wg sync.WaitGroup
	var mutex sync.RWMutex
	for workerNum := 0; workerNum < concurrency; workerNum++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for dir := range task_queue {
				l.loadExistingVolume(dir, needleMapKind, &mutex)
			}
		}()
	}
	wg.Wait()

}

func (l *DiskLocation) loadExistingVolumes(needleMapKind NeedleMapType) {
	l.Lock()
	defer l.Unlock()

	l.concurrentLoadingVolumes(needleMapKind, true)

	glog.V(0).Infoln("Store started on dir:", l.Directory, "with", len(l.volumes), "volumes", "max", l.MaxVolumeCount)
}

func (l *DiskLocation) DeleteCollectionFromDiskLocation(collection string) (e error) {
	l.Lock()
	defer l.Unlock()

	for k, v := range l.volumes {
		if v.Collection == collection {
			e = l.deleteVolumeById(k)
			if e != nil {
				return
			}
		}
	}
	return
}

func (l *DiskLocation) deleteVolumeById(vid VolumeId) (e error) {
	v, ok := l.volumes[vid]
	if !ok {
		return
	}
	e = v.Destroy()
	if e != nil {
		return
	}
	delete(l.volumes, vid)
	return
}

func (l *DiskLocation) LoadVolume(vid VolumeId, needleMapKind NeedleMapType) bool {
	if dirs, err := ioutil.ReadDir(l.Directory); err == nil {
		for _, dir := range dirs {
			volId, _, err := l.volumeIdFromPath(dir)
			if vid == volId && err == nil {
				var mutex sync.RWMutex
				l.loadExistingVolume(dir, needleMapKind, &mutex)
				return true
			}
		}
	}

	return false
}

func (l *DiskLocation) DeleteVolume(vid VolumeId) error {
	l.Lock()
	defer l.Unlock()

	_, ok := l.volumes[vid]
	if !ok {
		return fmt.Errorf("Volume not found, VolumeId: %d", vid)
	}
	return l.deleteVolumeById(vid)
}

func (l *DiskLocation) UnloadVolume(vid VolumeId) error {
	l.Lock()
	defer l.Unlock()

	v, ok := l.volumes[vid]
	if !ok {
		return fmt.Errorf("Volume not loaded, VolumeId: %d", vid)
	}
	v.Close()
	delete(l.volumes, vid)
	return nil
}

func (l *DiskLocation) SetVolume(vid VolumeId, volume *Volume) {
	l.Lock()
	defer l.Unlock()

	l.volumes[vid] = volume
}

func (l *DiskLocation) FindVolume(vid VolumeId) (*Volume, bool) {
	l.RLock()
	defer l.RUnlock()

	v, ok := l.volumes[vid]
	return v, ok
}

func (l *DiskLocation) VolumesLen() int {
	l.RLock()
	defer l.RUnlock()

	return len(l.volumes)
}

func (l *DiskLocation) Close() {
	l.Lock()
	defer l.Unlock()

	for _, v := range l.volumes {
		v.Close()
	}
	return
}

func (l *DiskLocation) GetVolumeFilePath(filename string) (string, string, error) {
	l.RLock()
	defer l.RUnlock()

	localFilePath := fmt.Sprintf("%s/%s", l.Directory, filename)
	finfo, err := os.Stat(localFilePath)
	if err != nil {
		return localFilePath, "", err
	}

	vid, _, err := l.volumeIdFromPath(finfo)
	if err != nil {
		return localFilePath, "", err
	}

	_, ok := l.volumes[vid]
	if ok { // can not allow for opening the mounted volume file
		return localFilePath, "", errors.New("can not open the mounted volume file")
	}

	contentMd5, err := util.CalculateFileMd5(localFilePath)
	if err != nil {
		return localFilePath, "", fmt.Errorf("calculate md5 of file %s error: %v", localFilePath, err)
	}

	return localFilePath, contentMd5, nil
}

func (l *DiskLocation) CheckVolumeFileExist(collection string, vid VolumeId, suffix string) (bool, string, error) {
	l.RLock()
	defer l.RUnlock()

	var filename string
	if collection != "" {
		filename = fmt.Sprintf("%s_%d.%s", collection, vid, suffix)
	} else {
		filename = fmt.Sprintf("%d.%s", vid, suffix)
	}
	localFilePath := fmt.Sprintf("%s/%s", l.Directory, filename)

	_, ok := l.volumes[vid]
	if ok { // volume exists
		return true, localFilePath, nil
	}

	_, err := os.Stat(localFilePath)
	if os.IsNotExist(err) {
		return false, localFilePath, nil
	} else if err != nil {
		return false, localFilePath, err
	}

	return true, localFilePath, nil
}

func (l *DiskLocation) DeleteVolumeFile(filename string, force bool) error {
	l.RLock()
	defer l.RUnlock()

	localFilePath := fmt.Sprintf("%s/%s", l.Directory, filename)
	finfo, err := os.Stat(localFilePath)
	if err != nil {
		return err
	}

	vid, _, err := l.volumeIdFromPath(finfo)
	if err != nil {
		return err
	}

	_, ok := l.volumes[vid]
	if ok { // can not allow for deleting the mounted volume file
		return errors.New("can not delete the mounted volume file")
	}

	if !force {
		return os.Rename(localFilePath, localFilePath+".del")
	}

	return util.RemoveFile(localFilePath)
}
