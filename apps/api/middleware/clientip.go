package middleware

import (
	"net"
	"net/http"
	"net/netip"
	"strings"
)

// TrustedProxies is the set of reverse proxies whose X-Forwarded-For
// header is believed. An empty set ignores the header entirely, so
// clients cannot spoof their address.
type TrustedProxies []netip.Prefix

// ParseTrustedProxies parses a comma-separated list of IPs and CIDRs,
// for example "10.0.0.0/8,192.168.1.10". Invalid entries are returned
// as an error.
func ParseTrustedProxies(value string) (TrustedProxies, error) {
	var proxies TrustedProxies

	for _, entry := range strings.Split(value, ",") {
		entry = strings.TrimSpace(entry)
		if entry == "" {
			continue
		}

		if !strings.Contains(entry, "/") {
			addr, err := netip.ParseAddr(entry)
			if err != nil {
				return nil, err
			}
			proxies = append(proxies, netip.PrefixFrom(addr, addr.BitLen()))
			continue
		}

		prefix, err := netip.ParsePrefix(entry)
		if err != nil {
			return nil, err
		}
		proxies = append(proxies, prefix.Masked())
	}

	return proxies, nil
}

func (t TrustedProxies) contains(addr netip.Addr) bool {
	addr = addr.Unmap()
	for _, prefix := range t {
		if prefix.Contains(addr) {
			return true
		}
	}
	return false
}

// ClientIP returns the address of the client that sent the request. When
// the direct peer is a trusted proxy, X-Forwarded-For is walked from the
// right and the first address that is not a trusted proxy is used.
func (t TrustedProxies) ClientIP(r *http.Request) string {
	host, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		host = r.RemoteAddr
	}

	peer, err := netip.ParseAddr(host)
	if err != nil || !t.contains(peer) {
		return host
	}

	hops := strings.Split(r.Header.Get("X-Forwarded-For"), ",")
	for i := len(hops) - 1; i >= 0; i-- {
		hop, err := netip.ParseAddr(strings.TrimSpace(hops[i]))
		if err != nil {
			break
		}
		if !t.contains(hop) {
			return hop.Unmap().String()
		}
	}

	return host
}
