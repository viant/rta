package shared

import "net"

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
