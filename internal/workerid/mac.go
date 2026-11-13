package workerid

import (
	"errors"
	"net"
	"sort"
)

// GenOnMAC generates a worker ID based on the MAC address.
// It selects the first valid network interface and derives a 10‑bit worker ID
// from the last two bytes of its MAC address.
func GenOnMAC() (int64, error) {
	interfaces, err := net.Interfaces()
	if err != nil {
		return 0, err
	}

	// Sort interfaces by index. Go does not expose NIC speed, so index ordering
	// is the stable heuristic.
	sort.Slice(interfaces, func(i, j int) bool {
		return interfaces[i].Index < interfaces[j].Index
	})

	for _, iface := range interfaces {
		// Skip loopback interfaces.
		if iface.Flags&net.FlagLoopback != 0 {
			continue
		}
		// Skip interfaces that are not up.
		if iface.Flags&net.FlagUp == 0 {
			continue
		}
		// Skip virtual interfaces (veth, docker, etc.).
		if isVirtual(iface.Name) {
			continue
		}

		mac := iface.HardwareAddr
		if len(mac) < 6 {
			continue
		}

		// Take the last two bytes of the MAC address: workerId = ((mac[4] & 0b11) << 8) | mac[5]
		// This produces a 10‑bit worker ID (0–1023).
		workerId := int64((uint16(mac[4]&0b11) << 8) | uint16(mac[5]))
		return workerId, nil
	}

	return 0, errors.New("no available MAC address found")
}

// isVirtual returns true if the interface name indicates a virtual NIC.
func isVirtual(name string) bool {
	virtualPrefixes := []string{
		"veth", "docker", "br-", "vmnet", "virbr", "lo", "tap", "tun",
	}

	for _, p := range virtualPrefixes {
		if len(name) >= len(p) && name[:len(p)] == p {
			return true
		}
	}
	return false
}
