package operation

import (
	"fmt"
	"io"
	"os"

	"weed/glog"
	"weed/util"
)

func DownloadVolumeFile(filename string, localFilePath string, server string, retries int) error {
	var sourceMd5 string
	var err error

	file, err := os.OpenFile(localFilePath, os.O_RDWR|os.O_CREATE|os.O_EXCL, 0666)
	if err != nil {
		glog.V(0).Infof("Open file %s error: %s", localFilePath, err.Error())
		return err
	}
	file.Close()

	targetUrl := fmt.Sprintf("http://%s/admin/vfile/download?file=%s", server, filename)
	i := 0
	for {
		sourceMd5, err = util.DownloadFilePartialContent(localFilePath, targetUrl)
		i++
		if err == nil {
			break
		}
		if err != io.ErrUnexpectedEOF {
			glog.V(1).Infof("DownloadFilePartialContent %s error: %s", err.Error())
			return err
		}
		glog.V(0).Infof("DownloadFilePartialContent %s ErrUnexpectedEOF, retry %d", localFilePath, i)
		if i >= retries {
			return err
		}
	}

	currentMd5, err := util.CalculateFileMd5(localFilePath)
	if err != nil {
		return err
	}

	if sourceMd5 != currentMd5 {
		return fmt.Errorf("md5sum not equal, sourceMd5: %s, currentMd5: %s", sourceMd5, currentMd5)
	}
	return nil
}
