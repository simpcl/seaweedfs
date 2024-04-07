package util

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"net/url"
	"os"
	"strings"

	"weed/glog"
	"weed/security"
)

var (
	client    *http.Client
	Transport *http.Transport
)

func init() {
	Transport = &http.Transport{
		MaxIdleConnsPerHost: 1024,
	}
	client = &http.Client{Transport: Transport}
}

func PostBytes(url string, body []byte) ([]byte, error) {
	r, err := client.Post(url, "application/octet-stream", bytes.NewReader(body))
	if err != nil {
		return nil, fmt.Errorf("Post to %s: %v", url, err)
	}
	defer r.Body.Close()
	if r.StatusCode >= 400 {
		return nil, fmt.Errorf("%s: %s", url, r.Status)
	}
	b, err := ioutil.ReadAll(r.Body)
	if err != nil {
		return nil, fmt.Errorf("Read response body: %v", err)
	}
	return b, nil
}

func Post(url string, values url.Values) ([]byte, error) {
	r, err := client.PostForm(url, values)
	if err != nil {
		return nil, err
	}
	defer r.Body.Close()
	b, err := ioutil.ReadAll(r.Body)
	if r.StatusCode >= 400 {
		if err != nil {
			return nil, fmt.Errorf("%s: %d - %s", url, r.StatusCode, string(b))
		} else {
			return nil, fmt.Errorf("%s: %s", url, r.Status)
		}
	}
	if err != nil {
		return nil, err
	}
	return b, nil
}

func Get(url string) ([]byte, error) {
	r, err := client.Get(url)
	if err != nil {
		return nil, err
	}
	defer r.Body.Close()
	b, err := ioutil.ReadAll(r.Body)
	if r.StatusCode >= 400 {
		return nil, fmt.Errorf("%s: %s", url, r.Status)
	}
	if err != nil {
		return nil, err
	}
	return b, nil
}

func Head(url string) (http.Header, error) {
	r, err := client.Head(url)
	if err != nil {
		return nil, err
	}
	if r.StatusCode >= 400 {
		return nil, fmt.Errorf("%s: %s", url, r.Status)
	}
	return r.Header, nil
}

func Delete(url string, jwt security.EncodedJwt) error {
	req, err := http.NewRequest("DELETE", url, nil)
	if jwt != "" {
		req.Header.Set("Authorization", "BEARER "+string(jwt))
	}
	if err != nil {
		return err
	}
	resp, e := client.Do(req)
	if e != nil {
		return e
	}
	defer resp.Body.Close()
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return err
	}
	switch resp.StatusCode {
	case http.StatusNotFound, http.StatusAccepted, http.StatusOK:
		return nil
	}
	m := make(map[string]interface{})
	if e := json.Unmarshal(body, m); e == nil {
		if s, ok := m["error"].(string); ok {
			return errors.New(s)
		}
	}
	return errors.New(string(body))
}

func GetBufferStream(url string, values url.Values, allocatedBytes []byte, eachBuffer func([]byte)) error {
	r, err := client.PostForm(url, values)
	if err != nil {
		return err
	}
	defer r.Body.Close()
	if r.StatusCode != 200 {
		return fmt.Errorf("%s: %s", url, r.Status)
	}
	bufferSize := len(allocatedBytes)
	for {
		n, err := r.Body.Read(allocatedBytes)
		if n == bufferSize {
			eachBuffer(allocatedBytes)
		}
		if err != nil {
			if err == io.EOF {
				return nil
			}
			return err
		}
	}
	return nil
}

func GetUrlStream(url string, values url.Values, readFn func(io.Reader) error) error {
	r, err := client.PostForm(url, values)
	if err != nil {
		return err
	}
	defer r.Body.Close()
	if r.StatusCode != 200 {
		return fmt.Errorf("%s: %s", url, r.Status)
	}
	return readFn(r.Body)
}

func DownloadUrl(fileUrl string) (filename string, rc io.ReadCloser, e error) {
	response, err := client.Get(fileUrl)
	if err != nil {
		return "", nil, err
	}
	contentDisposition := response.Header["Content-Disposition"]
	if len(contentDisposition) > 0 {
		idx := strings.Index(contentDisposition[0], "filename=")
		if idx != -1 {
			filename = contentDisposition[0][idx+len("filename="):]
			filename = strings.Trim(filename, "\"")
		}
	}
	rc = response.Body
	return
}

func Do(req *http.Request) (resp *http.Response, err error) {
	return client.Do(req)
}

func DownloadFilePartialContent(localFilePath string, targetUrl string) (string, error) {
	fileInfo, err := os.Stat(localFilePath)
	if err != nil {
		return "", err
	}
	fileSize := fileInfo.Size()

	file, err := os.OpenFile(localFilePath, os.O_WRONLY|os.O_APPEND, 0666)
	if err != nil {
		return "", err
	}
	defer file.Close()

	req, err := http.NewRequest("GET", targetUrl, nil)
	if err != nil {
		return "", err
	}

	if fileSize > 0 {
		glog.V(0).Infof("Set http range header bytes=%d-", fileSize)
		req.Header.Set("Range", fmt.Sprintf("bytes=%d-", fileSize))
	}

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusPartialContent {
		return "", fmt.Errorf("response status code %d", resp.StatusCode)
	}

	contentMd5 := resp.Header.Get("Content-Md5")

	buffer := make([]byte, 4*1024*1024)
	_, err = io.CopyBuffer(file, resp.Body, buffer)
	return contentMd5, err
}

func NormalizeUrl(url string) string {
	if strings.HasPrefix(url, "http://") || strings.HasPrefix(url, "https://") {
		return url
	}
	return "http://" + url
}
