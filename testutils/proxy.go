package testutils

import (
	"context"
	"crypto/tls"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"sync"
	"time"

	"github.com/cenkalti/backoff"

	"gopkg.in/src-d/go-log.v1"
)

const (
	ip   = "localhost"
	port = "8888"
	addr = ip + ":" + port
	URL  = "https://" + addr

	pingInterval = 500 * time.Millisecond
	pingTimeout  = 15 * time.Second
)

/*
Current file provides functionality of https and http proxy with ability to mock up responses
for testing purposes. Usually it's needed when client is not accessible from testing environment.

In this case several steps need to be done:
1) save http.DefaultTransport to a variable healthyTransport
2) use healthyTransport as an argument to the proxy constructor
3) spin up your proxy server
4) change http.DefaultTransport to the one that uses your proxy
5) defer changing http.DefaultTransport to the regular one after test is finished

Snippet:
	healthyTransport := http.DefaultTransport
	defer func() { http.DefaultTransport = healthyTransport }()

	proxy, err := testutils.NewProxy(healthyTransport, &testutils.Options{Code: 404})
	require.NoError(t, err)

	proxy.Start()
	defer func() { proxy.Stop() }()

	require.NoError(t, testutils.SetTransportProxy())
	...
*/

// TODO(@lwsanty): implement configs for different endpoints if needed

// Proxy is a struct of proxy wrapper
type Proxy struct {
	mu        sync.Mutex
	server    *http.Server
	transport http.RoundTripper
	// options for mock ups
	options *Options
	// FailEachNthRequestCounter is a counter required for failing condition on each nth request
	FailEachNthRequestCounter int
	// FailThresholdCounter is a counter required for failing condition after threshold has been reached
	FailThresholdCounter int
}

// Options represents the amount of options for mock ups
type Options struct {
	// Error is a trigger to fail with http.Error
	Error bool
	// ErrorText is an argument for http.Error in the case of Error == true
	ErrorText string
	// Code is a status code returned in response
	Code int
	// Delay represents the amount of time that handler will wait before response
	Delay time.Duration
	// FailEachNthRequest defines the period of requests failure
	FailEachNthRequest int
	// FailEachNthCode defines the status code to be returned during periodical requests failure
	FailEachNthCode int
	// FailThreshold defines the threshold of successfully processed requests, after which mocked responses will be returned
	FailThreshold int
	// FailThresholdCode defines the status code to be returned after threshold overcome
	FailThresholdCode int
	// pemPath and keyPath are paths to certs that required by https server
	PemPath string
	KeyPath string
}

// NewProxy is a proxy constructor
func NewProxy(transport http.RoundTripper, options *Options) (*Proxy, error) {
	processOptions(options)
	proxy := &Proxy{
		transport: transport,
		options:   options,
		server: &http.Server{
			Addr: ":" + port,
			// Disable HTTP/2.
			TLSNextProto: make(map[string]func(*http.Server, *tls.Conn, http.Handler)),
		},
	}
	proxy.initHandler()

	return proxy, nil
}

func (p *Proxy) initHandler() {
	p.server.Handler = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodConnect {
			p.handleTunneling(w, r)
		} else {
			p.handleHTTP(w, r)
		}
	})
}

// Start starts proxy server
func (p *Proxy) Start() error {
	go func() {
		if err := p.server.ListenAndServeTLS(p.options.PemPath, p.options.KeyPath); err != nil {
			if err != http.ErrServerClosed {
				log.Errorf(err, "https server failed")
			}
		}
	}()

	bo := backoff.NewExponentialBackOff()
	bo.MaxInterval = pingInterval
	bo.MaxElapsedTime = pingTimeout
	return backoff.Retry(func() error {
		conn, err := net.DialTimeout("tcp", addr, time.Second/2)
		if err == nil {
			conn.Close()
			return nil
		}
		return fmt.Errorf("start server: timeout")
	}, bo)
}

// Stop stops proxy server
func (p *Proxy) Stop() {
	if err := p.server.Shutdown(context.Background()); err != nil {
		log.Errorf(err, "https server shutdown failed")
	}
}

// SetTransportProxy changes http.DefaultTransport to the one that uses current server as a proxy
func SetTransportProxy() error {
	u, err := url.Parse(URL)
	if err != nil {
		return err
	}

	http.DefaultTransport = &http.Transport{
		Proxy: http.ProxyURL(u),
		// Disable HTTP/2.
		TLSNextProto:    make(map[string]func(authority string, c *tls.Conn) http.RoundTripper),
		TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
	}

	return nil
}

func (p *Proxy) handleTunneling(w http.ResponseWriter, r *http.Request) {
	time.Sleep(p.options.Delay)

	if p.options.Error {
		w.WriteHeader(p.options.Code)
		http.Error(w, p.options.ErrorText, p.options.Code)
		return
	}

	if p.options.FailEachNthRequest > 0 {
		p.mu.Lock()
		p.FailEachNthRequestCounter++
		c := p.FailEachNthRequestCounter
		p.mu.Unlock()

		if c%p.options.FailEachNthRequest == 0 {
			http.Error(w, "periodical fail", p.options.FailEachNthCode)
			return
		}
	}

	if p.options.FailThreshold > 0 {
		p.mu.Lock()
		p.FailThresholdCounter++
		c := p.FailThresholdCounter
		p.mu.Unlock()

		if c > p.options.FailThreshold {
			http.Error(w, "periodical fail", p.options.FailThresholdCode)
			return
		}
	}

	destConn, err := net.DialTimeout("tcp", r.Host, 10*time.Second)
	if err != nil {
		http.Error(w, err.Error(), http.StatusServiceUnavailable)
		return
	}

	w.WriteHeader(p.options.Code)

	hijacker, ok := w.(http.Hijacker)
	if !ok {
		http.Error(w, "Hijacking not supported", http.StatusInternalServerError)
		return
	}
	clientConn, _, err := hijacker.Hijack()
	if err != nil {
		http.Error(w, err.Error(), http.StatusServiceUnavailable)
	}
	go transfer(destConn, clientConn)
	go transfer(clientConn, destConn)
}

func transfer(destination io.WriteCloser, source io.ReadCloser) {
	defer destination.Close()
	defer source.Close()
	io.Copy(destination, source)
}

func (p *Proxy) handleHTTP(w http.ResponseWriter, req *http.Request) {
	resp, err := p.transport.RoundTrip(req)
	if err != nil {
		http.Error(w, err.Error(), http.StatusServiceUnavailable)
		return
	}
	defer resp.Body.Close()
	copyHeader(w.Header(), resp.Header)
	w.WriteHeader(resp.StatusCode)
	io.Copy(w, resp.Body)
}

func copyHeader(dst, src http.Header) {
	for k, vv := range src {
		for _, v := range vv {
			dst.Add(k, v)
		}
	}
}

func processOptions(o *Options) {
	ok := http.StatusOK
	if o.Code == 0 {
		o.Code = ok
	}
	if o.FailEachNthCode == 0 {
		o.FailEachNthCode = ok
	}
	if o.FailThresholdCode == 0 {
		o.FailThresholdCode = ok
	}
}
