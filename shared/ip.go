package shared

import (
	"net"
	"os"
)

func GetHostIPv4(hostname string) (string, error) {
	addrs, err := net.LookupHost(hostname)
	if err != nil {
		return "", err
	}
	for _, addr := range addrs {
		IP := net.ParseIP(addr)
		if IP.IsLoopback() || IP.To4() == nil || IP.IsLinkLocalUnicast() {
			continue
		}
		return addr, nil
	}
	return addrs[0], nil
}

func GetLocalIPv4() (string, error) {
	ifas, err := net.Interfaces()
	if err != nil {
		return "", err
	}
outer:
	for _, ifa := range ifas {
		ad, _ := ifa.Addrs()
		if len(ad) == 0 {
			continue
		}
		for _, c := range ad {
			ipNet, ok := c.(*net.IPNet)
			if !ok {
				continue outer
			}
			if ipv4 := ipNet.IP.To4(); ipv4 != nil {
				if ipv4.IsLoopback() {
					continue
				}
				return ipv4.String(), nil
			}
		}
	}
	if hostname, err := os.Hostname(); err == nil {
		return GetHostIPv4(hostname)
	}
	return "127.0.0.1", nil
}
