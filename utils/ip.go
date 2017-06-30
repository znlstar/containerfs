package utils

import (
	"net"
	"strconv"
	"strings"
)

//InetNtoa  Convert uint to net.IP
func InetNtoa(ipnr int32) net.IP {
	var bytes [4]byte
	bytes[0] = byte(ipnr & 0xFF)
	bytes[1] = byte((ipnr >> 8) & 0xFF)
	bytes[2] = byte((ipnr >> 16) & 0xFF)
	bytes[3] = byte((ipnr >> 24) & 0xFF)

	return net.IPv4(bytes[3], bytes[2], bytes[1], bytes[0])
}

//Inet_aton  Convert net.IP to int32
func Inet_aton(ipnr net.IP) int32 {
	bits := strings.Split(ipnr.String(), ".")

	b0, _ := strconv.Atoi(bits[0])
	b1, _ := strconv.Atoi(bits[1])
	b2, _ := strconv.Atoi(bits[2])
	b3, _ := strconv.Atoi(bits[3])

	var sum int32

	sum += int32(b0) << 24
	sum += int32(b1) << 16
	sum += int32(b2) << 8
	sum += int32(b3)

	return sum
}
