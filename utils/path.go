package utils

import (
	"crypto/md5"
	"encoding/hex"
	"os"
	"strings"
)

func substr(s string, pos, length int) string {
	runes := []rune(s)
	l := pos + length
	if l > len(runes) {
		l = len(runes)
	}
	return string(runes[pos:l])
}

// GetParentFullPath ...
func GetParentFullPath(in string) (parentFullPath string) {
	parentFullPath = substr(in, 0, strings.LastIndex(in, "/"))
	if parentFullPath == "" {
		parentFullPath = "/"
	}
	return
}

// GetSelfName ...
func GetSelfName(in string) (selfName string) {
	tmp := strings.Split(in, "/")
	selfName = tmp[len(tmp)-1]
	if selfName == "" {
		selfName = "/"
	}
	return
}

// GetParentName ...
func GetParentName(in string) (parentName string) {
	tmp := strings.Split(in, "/")
	parentName = tmp[len(tmp)-2]
	if parentName == "" {
		parentName = "/"
	}
	return
}

// MD5 ...
func MD5(in string) string {
	h := md5.New()
	h.Write([]byte(in))
	cipherStr := h.Sum(nil)
	hexStr := hex.EncodeToString(cipherStr)
	return hexStr
}

// LocalPathExists ...
func LocalPathExists(path string) (bool, error) {
	_, err := os.Stat(path)
	if err == nil {
		return true, nil
	}
	if os.IsNotExist(err) {
		return false, nil
	}
	return false, err
}
