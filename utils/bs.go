package utils

import (
	"reflect"
	"unsafe"
)

func B2S(buf []byte) string {
        return *(*string)(unsafe.Pointer(&buf))
}

func S2B(s *string) []byte {
        return *(*[]byte)(unsafe.Pointer((*reflect.SliceHeader)(unsafe.Pointer(s))))
}
