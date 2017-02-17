package utils

import (
	"crypto/md5"
	"encoding/hex"
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

func GetParentFullPath(in string) (parentFullPath string) {
	parentFullPath = substr(in, 0, strings.LastIndex(in, "/"))
	if parentFullPath == "" {
		parentFullPath = "/"
	}
	return
}

func GetSelfName(in string) (selfName string) {
	tmp := strings.Split(in, "/")
	selfName = tmp[len(tmp)-1]
	if selfName == "" {
		selfName = "/"
	}
	return
}

func GetParentName(in string) (parentName string) {
	tmp := strings.Split(in, "/")
	parentName = tmp[len(tmp)-2]
	if parentName == "" {
		parentName = "/"
	}
	return
}

func MD5(in string) string {
	h := md5.New()
	h.Write([]byte(in))
	cipherStr := h.Sum(nil)
	hexStr := hex.EncodeToString(cipherStr)
	return hexStr
}
