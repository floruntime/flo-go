package flo

import (
	"fmt"
	"io"
	"log"
	"net"
	"strconv"
	"strings"
	"sync"
	"time"
)

// Client is a Flo client for connecting to Flo servers.
type Client struct {
	endpoint  string
	namespace string
	timeout   time.Duration
	debug     bool

	conn      net.Conn
	requestID uint64
	mu        sync.Mutex

	// Sub-clients for different operations — all accessed as fields for consistency.
	KV     *KVClient
	Queue  *QueueClient
	Stream *StreamClient
	Action *ActionClient
}

// ClientOption is a function that configures a Client.
type ClientOption func(*Client)

// WithNamespace sets the default namespace for operations.
func WithNamespace(namespace string) ClientOption {
	return func(c *Client) {
		c.namespace = namespace
	}
}

// WithTimeout sets the connection and operation timeout.
func WithTimeout(timeout time.Duration) ClientOption {
	return func(c *Client) {
		c.timeout = timeout
	}
}

// WithDebug enables debug logging.
func WithDebug(debug bool) ClientOption {
	return func(c *Client) {
		c.debug = debug
	}
}

// NewClient creates a new Flo client.
func NewClient(endpoint string, opts ...ClientOption) *Client {
	c := &Client{
		endpoint:  endpoint,
		namespace: "default",
		timeout:   5 * time.Second,
		debug:     false,
	}

	for _, opt := range opts {
		opt(c)
	}

	c.KV = &KVClient{client: c}
	c.Queue = &QueueClient{client: c}
	c.Stream = &StreamClient{client: c}
	c.Action = &ActionClient{client: c}

	return c
}

// Connect establishes a connection to the server.
func (c *Client) Connect() error {
	host, port, err := parseEndpoint(c.endpoint)
	if err != nil {
		return err
	}

	addr := net.JoinHostPort(host, strconv.Itoa(port))
	dialer := net.Dialer{Timeout: c.timeout}

	conn, err := dialer.Dial("tcp", addr)
	if err != nil {
		return fmt.Errorf("%w: %s - %v", ErrConnectionFailed, c.endpoint, err)
	}

	c.conn = conn

	if c.debug {
		log.Printf("[flo] Connected to %s", c.endpoint)
	}

	return nil
}

// Close closes the connection to the server.
func (c *Client) Close() error {
	if c.conn != nil {
		err := c.conn.Close()
		c.conn = nil
		if c.debug {
			log.Printf("[flo] Disconnected")
		}
		return err
	}
	return nil
}

// IsConnected returns true if the client is connected.
func (c *Client) IsConnected() bool {
	return c.conn != nil
}

// Namespace returns the default namespace.
func (c *Client) Namespace() string {
	return c.namespace
}

// parseEndpoint parses an endpoint string into host and port.
func parseEndpoint(endpoint string) (string, int, error) {
	// Handle IPv6 addresses in brackets
	if strings.HasPrefix(endpoint, "[") {
		idx := strings.LastIndex(endpoint, "]:")
		if idx == -1 {
			return "", 0, fmt.Errorf("%w: %s (expected [host]:port)", ErrInvalidEndpoint, endpoint)
		}
		host := endpoint[1:idx]
		portStr := endpoint[idx+2:]
		port, err := strconv.Atoi(portStr)
		if err != nil {
			return "", 0, fmt.Errorf("%w: invalid port %s", ErrInvalidEndpoint, portStr)
		}
		if port < 1 || port > 65535 {
			return "", 0, fmt.Errorf("%w: port out of range %d", ErrInvalidEndpoint, port)
		}
		return host, port, nil
	}

	// Handle IPv4 or hostname
	idx := strings.LastIndex(endpoint, ":")
	if idx == -1 {
		return "", 0, fmt.Errorf("%w: %s (expected host:port)", ErrInvalidEndpoint, endpoint)
	}

	host := endpoint[:idx]
	portStr := endpoint[idx+1:]

	port, err := strconv.Atoi(portStr)
	if err != nil {
		return "", 0, fmt.Errorf("%w: invalid port %s", ErrInvalidEndpoint, portStr)
	}

	if port < 1 || port > 65535 {
		return "", 0, fmt.Errorf("%w: port out of range %d", ErrInvalidEndpoint, port)
	}

	return host, port, nil
}

// nextRequestID returns the next request ID.
func (c *Client) nextRequestID() uint64 {
	c.requestID++
	return c.requestID
}

// getNamespace returns the effective namespace.
func (c *Client) getNamespace(override string) string {
	if override != "" {
		return override
	}
	return c.namespace
}

// sendRequest sends a request and receives the response.
func (c *Client) sendRequest(opCode OpCode, namespace string, key, value, options []byte) (*rawResponse, error) {
	if c.conn == nil {
		return nil, ErrNotConnected
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	requestID := c.nextRequestID()

	// Serialize request
	request, err := serializeRequest(requestID, opCode, []byte(namespace), key, value, options)
	if err != nil {
		return nil, err
	}

	if c.debug {
		log.Printf("[flo] -> %d ns=%s key=%q", opCode, namespace, key)
	}

	// Set timeout
	if c.timeout > 0 {
		c.conn.SetDeadline(time.Now().Add(c.timeout))
	}

	// Send request
	if _, err := c.conn.Write(request); err != nil {
		return nil, fmt.Errorf("%w: %v", ErrUnexpectedEOF, err)
	}

	// Read response header
	header := make([]byte, HeaderSize)
	if _, err := io.ReadFull(c.conn, header); err != nil {
		return nil, fmt.Errorf("%w: reading header: %v", ErrUnexpectedEOF, err)
	}

	// Parse header
	status, dataLen, respRequestID, expectedCRC, err := parseResponseHeader(header)
	if err != nil {
		return nil, err
	}

	// Read response data
	var data []byte
	if dataLen > 0 {
		data = make([]byte, dataLen)
		if _, err := io.ReadFull(c.conn, data); err != nil {
			return nil, fmt.Errorf("%w: reading data: %v", ErrUnexpectedEOF, err)
		}
	}

	// Verify CRC32
	computedCRC := computeCRC32(header, data)
	if computedCRC != expectedCRC {
		return nil, fmt.Errorf("%w: 0x%08X != 0x%08X", ErrInvalidChecksum, computedCRC, expectedCRC)
	}

	if c.debug {
		log.Printf("[flo] <- %s %d bytes", status, len(data))
	}

	return &rawResponse{
		Status:    status,
		Data:      data,
		RequestID: respRequestID,
	}, nil
}

// sendAndCheck sends a request and checks the response status.
func (c *Client) sendAndCheck(opCode OpCode, namespace string, key, value, options []byte, allowNotFound bool) (*rawResponse, error) {
	resp, err := c.sendRequest(opCode, namespace, key, value, options)
	if err != nil {
		return nil, err
	}

	if err := checkStatus(resp.Status, resp.Data, allowNotFound); err != nil {
		return nil, err
	}

	return resp, nil
}
