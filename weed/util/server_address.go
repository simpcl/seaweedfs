package util

import (
	"fmt"
	"net"
	"strconv"
	"strings"
	"weed/glog"
)

type ServerAddress string

func NewServerAddress(host string, port int, grpcPort int) ServerAddress {
	if grpcPort == 0 || grpcPort == port+10000 {
		return ServerAddress(JoinHostPort(host, port))
	}
	return ServerAddress(JoinHostPort(host, port) + "." + strconv.Itoa(grpcPort))
}

func NewServerAddressWithGrpcPort(address string, grpcPort int) ServerAddress {
	if grpcPort == 0 {
		return ServerAddress(address)
	}
	_, port, _ := hostAndPort(address)
	if uint64(grpcPort) == port+10000 {
		return ServerAddress(address)
	}
	return ServerAddress(address + "." + strconv.Itoa(grpcPort))
}

func (sa ServerAddress) String() string {
	return sa.ToHttpAddress()
}

func (sa ServerAddress) ToHttpAddress() string {
	portsSepIndex := strings.LastIndex(string(sa), ":")
	if portsSepIndex < 0 {
		return string(sa)
	}
	if portsSepIndex+1 >= len(sa) {
		return string(sa)
	}
	ports := string(sa[portsSepIndex+1:])
	sepIndex := strings.LastIndex(string(ports), ".")
	if sepIndex >= 0 {
		host := string(sa[0:portsSepIndex])
		return net.JoinHostPort(host, ports[0:sepIndex])
	}
	return string(sa)
}

func (sa ServerAddress) ToGrpcAddress() string {
	portsSepIndex := strings.LastIndex(string(sa), ":")
	if portsSepIndex < 0 {
		return string(sa)
	}
	if portsSepIndex+1 >= len(sa) {
		return string(sa)
	}
	ports := string(sa[portsSepIndex+1:])
	sepIndex := strings.LastIndex(ports, ".")
	if sepIndex >= 0 {
		host := string(sa[0:portsSepIndex])
		return net.JoinHostPort(host, ports[sepIndex+1:])
	}
	return ServerToGrpcAddress(string(sa))
}

func FromStringsToSAs(strings []string) []ServerAddress {
	var addresses []ServerAddress
	for _, addr := range strings {
		addresses = append(addresses, ServerAddress(addr))
	}
	return addresses
}

func FromStringToSAs(sas string) (addresses []ServerAddress) {
	parts := strings.Split(sas, ",")
	for _, address := range parts {
		if address != "" {
			addresses = append(addresses, ServerAddress(address))
		}
	}
	return
}

func hostAndPort(address string) (host string, port uint64, err error) {
	colonIndex := strings.LastIndex(address, ":")
	if colonIndex < 0 {
		return "", 0, fmt.Errorf("server should have hostname:port format: %v", address)
	}
	port, err = strconv.ParseUint(address[colonIndex+1:], 10, 64)
	if err != nil {
		return "", 0, fmt.Errorf("server port parse error: %v", err)
	}

	return address[:colonIndex], port, err
}

func ServerToGrpcAddress(server string) (serverGrpcAddress string) {

	host, port, parseErr := hostAndPort(server)
	if parseErr != nil {
		glog.Fatalf("server address %s parse error: %v", server, parseErr)
	}

	grpcPort := int(port) + 10000

	return JoinHostPort(host, grpcPort)
}

func ParseUrl(input string) (address ServerAddress, path string, err error) {
	if !strings.HasPrefix(input, "http://") {
		return "", "", fmt.Errorf("url %s needs prefix 'http://'", input)
	}
	input = input[7:]
	pathSeparatorIndex := strings.Index(input, "/")
	hostAndPorts := input
	if pathSeparatorIndex > 0 {
		path = input[pathSeparatorIndex:]
		hostAndPorts = input[0:pathSeparatorIndex]
	}
	commaSeparatorIndex := strings.Index(input, ":")
	if commaSeparatorIndex < 0 {
		err = fmt.Errorf("port should be specified in %s", input)
		return
	}
	address = ServerAddress(hostAndPorts)
	return
}
