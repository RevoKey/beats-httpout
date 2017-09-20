package http

import (
	"expvar"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"net/url"
	"time"

	"github.com/elastic/beats/libbeat/logp"
	"github.com/elastic/beats/libbeat/outputs"

	"github.com/elastic/beats/libbeat/outputs/outil"
	"github.com/elastic/beats/libbeat/outputs/transport"
)

// Client definition
type Client struct {
	Connection
	tlsConfig *transport.TLSConfig
	params    map[string]string

	// additional configs
	compressionLevel int
	proxyURL         *url.URL
}

// ClientSettings definition
type ClientSettings struct {
	URL                string
	Proxy              *url.URL
	TLS                *transport.TLSConfig
	Username, Password string
	Parameters         map[string]string
	Headers            map[string]string
	Index              outil.Selector
	Pipeline           *outil.Selector
	Timeout            time.Duration
	CompressionLevel   int
}

// Connection definition
type Connection struct {
	URL      string
	Username string
	Password string
	Headers  map[string]string

	http      *http.Client
	connected bool

	encoder bodyEncoder
	version string
}

// Metrics that can retrieved through the expvar web interface.
var (
	ackedEvents            = expvar.NewInt("libbeat.http.published_and_acked_events")
	eventsNotAcked         = expvar.NewInt("libbeat.http.published_but_not_acked_events")
	publishEventsCallCount = expvar.NewInt("libbeat.http.call_count.PublishEvents")

	statReadBytes   = expvar.NewInt("libbeat.http.publish.read_bytes")
	statWriteBytes  = expvar.NewInt("libbeat.http.publish.write_bytes")
	statReadErrors  = expvar.NewInt("libbeat.http.publish.read_errors")
	statWriteErrors = expvar.NewInt("libbeat.http.publish.write_errors")
)

// NewClient instantiates a new client.
func NewClient(s ClientSettings) (*Client, error) {
	proxy := http.ProxyFromEnvironment
	if s.Proxy != nil {
		proxy = http.ProxyURL(s.Proxy)
	}

	u, err := url.Parse(s.URL)
	if err != nil {
		return nil, fmt.Errorf("failed to parse http URL: %v", err)
	}
	if u.User != nil {
		s.Username = u.User.Username()
		s.Password, _ = u.User.Password()
		u.User = nil

		// Re-write URL without credentials.
		s.URL = u.String()
	}

	logp.Info("Http url: %s", s.URL)

	// TODO: add socks5 proxy support
	var dialer, tlsDialer transport.Dialer

	dialer = transport.NetDialer(s.Timeout)
	tlsDialer, err = transport.TLSDialer(dialer, s.TLS, s.Timeout)
	if err != nil {
		return nil, err
	}

	iostats := &transport.IOStats{
		Read:        statReadBytes,
		Write:       statWriteBytes,
		ReadErrors:  statReadErrors,
		WriteErrors: statWriteErrors,
	}
	dialer = transport.StatsDialer(dialer, iostats)
	tlsDialer = transport.StatsDialer(tlsDialer, iostats)

	params := s.Parameters

	var encoder bodyEncoder
	compression := s.CompressionLevel
	if compression == 0 {
		encoder = newJSONEncoder(nil)
	} else {
		encoder, err = newGzipEncoder(compression, nil)
		if err != nil {
			return nil, err
		}
	}

	client := &Client{
		Connection: Connection{
			URL:      s.URL,
			Username: s.Username,
			Password: s.Password,
			Headers:  s.Headers,
			http: &http.Client{
				Transport: &http.Transport{
					Dial:    dialer.Dial,
					DialTLS: tlsDialer.Dial,
					Proxy:   proxy,
				},
				Timeout: s.Timeout,
			},
			encoder: encoder,
		},
		tlsConfig: s.TLS,
		params:    params,

		compressionLevel: compression,
		proxyURL:         s.Proxy,
	}

	return client, nil
}

// Connect Connectable interface
func (conn *Connection) Connect(timeout time.Duration) error {
	// todo check connectable
	conn.connected = true
	return nil
}

// Close Connectable interfate
func (conn *Connection) Close() error {
	conn.connected = false
	return nil
}

// PublishEvents sends all events to http endpoint. On error a slice with all
// events not published will be returned. The input slice backing memory will
// be reused by return the value.
func (client *Client) PublishEvents(data []outputs.Data) (nextEvents []outputs.Data, err error) {
	begin := time.Now()
	publishEventsCallCount.Add(1)

	if len(data) == 0 {
		return nil, nil
	}

	if !client.connected {
		return data, ErrNotConnected
	}

	var failedEvents []outputs.Data

	sendErr := error(nil)
	for i, event := range data {
		sendErr = client.PublishEvent(event)
		if sendErr != nil {
			logp.Err("Failed to PublishEvent: %s", sendErr)
			failedEvents = append(failedEvents, data[i])
		}
	}

	debugf("PublishEvents: %d metrics have been published over HTTP in %v.",
		len(data),
		time.Now().Sub(begin))

	ackedEvents.Add(int64(len(data) - len(failedEvents)))
	eventsNotAcked.Add(int64(len(failedEvents)))
	if len(failedEvents) > 0 {
		return failedEvents, sendErr
	}

	return nil, nil
}

// PublishEvent sends one event to http endpoint.
func (client *Client) PublishEvent(data outputs.Data) error {
	if !client.connected {
		return ErrNotConnected
	}

	event := data.Event

	debugf("Publish event: %s", event)

	status, _, err := client.Request("POST", "", client.params, event)
	if err != nil {
		logp.Warn("Fail to send a single request: %s", err)
		if err == ErrJSONEncodeFailed {
			// don't retry unencodable values
			return nil
		}
	}
	switch {
	case status == 0: // event was not send yet
		return nil
	case status >= 500 || status == 429: // server error, retry
		return err
	case status >= 300 && status < 500:
		// other error => don't retry
		return nil
	}

	return nil
}

// Request send a http request
func (conn *Connection) Request(method, path string, params map[string]string, body interface{}) (int, []byte, error) {
	url := makeURL(conn.URL, path, "", params)
	debugf("%s %s %v", method, url, body)

	if body == nil {
		return conn.execRequest(method, url, nil)
	}

	if err := conn.encoder.Marshal(body); err != nil {
		logp.Warn("Failed to json encode body (%v): %#v", err, body)
		return 0, nil, ErrJSONEncodeFailed
	}
	return conn.execRequest(method, url, conn.encoder.Reader())
}

func (conn *Connection) execRequest(method, url string, body io.Reader) (int, []byte, error) {
	req, err := http.NewRequest(method, url, body)
	if err != nil {
		logp.Warn("Failed to create request", err)
		return 0, nil, err
	}
	if body != nil {
		conn.encoder.AddHeader(&req.Header)
	}
	return conn.execHTTPRequest(req)
}

func (conn *Connection) execHTTPRequest(req *http.Request) (int, []byte, error) {
	req.Header.Add("Accept", "application/json")
	if conn.Username != "" || conn.Password != "" {
		req.SetBasicAuth(conn.Username, conn.Password)
	}

	resp, err := conn.http.Do(req)
	if err != nil {
		conn.connected = false
		return 0, nil, err
	}
	defer closing(resp.Body)

	status := resp.StatusCode
	if status >= 300 {
		conn.connected = false
		return status, nil, fmt.Errorf("%v", resp.Status)
	}

	obj, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		conn.connected = false
		return status, nil, err
	}
	return status, obj, nil
}

func closing(c io.Closer) {
	err := c.Close()
	if err != nil {
		logp.Warn("Close failed with: %v", err)
	}
}
