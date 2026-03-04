// Copyright 2024 The NATS Authors
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// nats-echo-mcp is an MCP (Model Context Protocol) server that exposes
// NATS messaging as tools over WebSocket. A Claude agent can connect
// to this server and use its tools to interact with NATS subjects.
//
// Available tools:
//   - echo:    Send a message to a NATS subject and return the reply (default subject: "echo")
//   - request: Send a request to any NATS subject and wait for a reply
//   - publish: Publish a fire-and-forget message to a NATS subject
//
// Usage:
//
//	# Terminal 1: Start a NATS echo responder
//	nats reply echo --command "echo {{.Body}}"
//
//	# Terminal 2: Start the MCP server
//	go run . -s nats://localhost:4222 -addr :1234
//
//	# Terminal 3: Connect Claude
//	claude --mcp-config '{"url":"ws://localhost:1234"}'
package main

import (
	"bufio"
	"crypto/sha1"
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"sync"
	"time"

	"github.com/nats-io/nats.go"
)

// JSON-RPC message types.

type jsonrpcRequest struct {
	JSONRPC string          `json:"jsonrpc"`
	ID      json.RawMessage `json:"id,omitempty"`
	Method  string          `json:"method"`
	Params  json.RawMessage `json:"params,omitempty"`
}

type jsonrpcResponse struct {
	JSONRPC string          `json:"jsonrpc"`
	ID      json.RawMessage `json:"id,omitempty"`
	Result  interface{}     `json:"result,omitempty"`
	Error   *rpcError       `json:"error,omitempty"`
}

type rpcError struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
}

// MCP protocol types.

type initializeResult struct {
	ProtocolVersion string     `json:"protocolVersion"`
	Capabilities    mcpCaps    `json:"capabilities"`
	ServerInfo      serverInfo `json:"serverInfo"`
}

type mcpCaps struct {
	Tools *toolsCap `json:"tools,omitempty"`
}

type toolsCap struct {
	ListChanged bool `json:"listChanged"`
}

type serverInfo struct {
	Name    string `json:"name"`
	Version string `json:"version"`
}

type mcpTool struct {
	Name        string      `json:"name"`
	Description string      `json:"description"`
	InputSchema inputSchema `json:"inputSchema"`
}

type inputSchema struct {
	Type       string              `json:"type"`
	Properties map[string]property `json:"properties"`
	Required   []string            `json:"required,omitempty"`
}

type property struct {
	Type        string `json:"type"`
	Description string `json:"description"`
}

type toolsListResult struct {
	Tools []mcpTool `json:"tools"`
}

type callToolParams struct {
	Name      string          `json:"name"`
	Arguments json.RawMessage `json:"arguments"`
}

type callToolResult struct {
	Content []contentItem `json:"content"`
	IsError bool          `json:"isError,omitempty"`
}

type contentItem struct {
	Type string `json:"type"`
	Text string `json:"text"`
}

// Tool argument types.

type echoArgs struct {
	Message string `json:"message"`
	Subject string `json:"subject"`
}

type publishArgs struct {
	Subject string `json:"subject"`
	Message string `json:"message"`
}

type requestArgs struct {
	Subject string `json:"subject"`
	Message string `json:"message"`
}

// Minimal WebSocket implementation (RFC 6455) for text frames.

const (
	wsGUID       = "258EAFA5-E914-47DA-95CA-5AB5DC76E97"
	opText       = 1
	opClose      = 8
	opPing       = 9
	opPong       = 10
)

type wsConn struct {
	conn net.Conn
	br   *bufio.Reader
	mu   sync.Mutex
}

func upgradeWebSocket(w http.ResponseWriter, r *http.Request) (*wsConn, error) {
	if !strings.EqualFold(r.Header.Get("Upgrade"), "websocket") {
		http.Error(w, "expected websocket upgrade", http.StatusBadRequest)
		return nil, fmt.Errorf("not a websocket request")
	}
	key := r.Header.Get("Sec-WebSocket-Key")
	if key == "" {
		http.Error(w, "missing Sec-WebSocket-Key", http.StatusBadRequest)
		return nil, fmt.Errorf("missing websocket key")
	}

	h := sha1.New()
	h.Write([]byte(key + wsGUID))
	accept := base64.StdEncoding.EncodeToString(h.Sum(nil))

	hj, ok := w.(http.Hijacker)
	if !ok {
		http.Error(w, "server does not support hijacking", http.StatusInternalServerError)
		return nil, fmt.Errorf("hijack not supported")
	}
	conn, buf, err := hj.Hijack()
	if err != nil {
		return nil, err
	}

	resp := "HTTP/1.1 101 Switching Protocols\r\n" +
		"Upgrade: websocket\r\n" +
		"Connection: Upgrade\r\n" +
		"Sec-WebSocket-Accept: " + accept + "\r\n\r\n"
	if _, err := conn.Write([]byte(resp)); err != nil {
		conn.Close()
		return nil, err
	}

	return &wsConn{conn: conn, br: buf.Reader}, nil
}

// readMessage reads a single WebSocket text message, handling control frames.
func (ws *wsConn) readMessage() ([]byte, error) {
	for {
		// Read frame header: 2 bytes minimum.
		header := make([]byte, 2)
		if _, err := io.ReadFull(ws.br, header); err != nil {
			return nil, err
		}

		fin := header[0]&0x80 != 0
		opcode := header[0] & 0x0f
		masked := header[1]&0x80 != 0
		length := uint64(header[1] & 0x7f)

		switch {
		case length == 126:
			ext := make([]byte, 2)
			if _, err := io.ReadFull(ws.br, ext); err != nil {
				return nil, err
			}
			length = uint64(binary.BigEndian.Uint16(ext))
		case length == 127:
			ext := make([]byte, 8)
			if _, err := io.ReadFull(ws.br, ext); err != nil {
				return nil, err
			}
			length = binary.BigEndian.Uint64(ext)
		}

		var mask [4]byte
		if masked {
			if _, err := io.ReadFull(ws.br, mask[:]); err != nil {
				return nil, err
			}
		}

		payload := make([]byte, length)
		if _, err := io.ReadFull(ws.br, payload); err != nil {
			return nil, err
		}
		if masked {
			for i := range payload {
				payload[i] ^= mask[i%4]
			}
		}

		switch opcode {
		case opText:
			if !fin {
				// For simplicity, we don't handle fragmented frames.
				return nil, fmt.Errorf("fragmented frames not supported")
			}
			return payload, nil
		case opClose:
			// Send close frame back and return EOF.
			ws.writeFrame(opClose, nil)
			return nil, io.EOF
		case opPing:
			ws.writeFrame(opPong, payload)
			continue
		case opPong:
			continue
		default:
			continue
		}
	}
}

// writeFrame writes a single WebSocket frame (server→client, unmasked).
func (ws *wsConn) writeFrame(opcode byte, payload []byte) error {
	ws.mu.Lock()
	defer ws.mu.Unlock()

	frame := []byte{0x80 | opcode}
	length := len(payload)
	switch {
	case length <= 125:
		frame = append(frame, byte(length))
	case length <= 65535:
		frame = append(frame, 126)
		ext := make([]byte, 2)
		binary.BigEndian.PutUint16(ext, uint16(length))
		frame = append(frame, ext...)
	default:
		frame = append(frame, 127)
		ext := make([]byte, 8)
		binary.BigEndian.PutUint64(ext, uint64(length))
		frame = append(frame, ext...)
	}
	frame = append(frame, payload...)
	_, err := ws.conn.Write(frame)
	return err
}

// writeJSON marshals v as JSON and sends it as a WebSocket text frame.
func (ws *wsConn) writeJSON(v interface{}) error {
	data, err := json.Marshal(v)
	if err != nil {
		return err
	}
	return ws.writeFrame(opText, data)
}

func (ws *wsConn) close() {
	ws.writeFrame(opClose, nil)
	ws.conn.Close()
}

func usage() {
	log.Printf("Usage: nats-echo-mcp [-s server] [-addr listen-addr]\n")
	flag.PrintDefaults()
}

func main() {
	var urls = flag.String("s", nats.DefaultURL, "The NATS server URLs (separated by comma)")
	var addr = flag.String("addr", ":1234", "Address for the MCP WebSocket server to listen on")
	var showHelp = flag.Bool("h", false, "Show help message")

	log.SetFlags(0)
	flag.Usage = usage
	flag.Parse()

	if *showHelp {
		usage()
		os.Exit(0)
	}

	// Connect to NATS.
	nc, err := nats.Connect(*urls,
		nats.Name("NATS Echo MCP Server"),
		nats.ReconnectWait(time.Second),
		nats.MaxReconnects(60),
	)
	if err != nil {
		log.Fatalf("Error connecting to NATS: %v", err)
	}
	defer nc.Close()
	log.Printf("Connected to NATS at %s", nc.ConnectedUrl())

	// Define the tools we expose via MCP.
	tools := []mcpTool{
		{
			Name:        "echo",
			Description: "Send a message to a NATS subject and return the reply. Defaults to the 'echo' subject.",
			InputSchema: inputSchema{
				Type: "object",
				Properties: map[string]property{
					"message": {Type: "string", Description: "The message to echo"},
					"subject": {Type: "string", Description: "NATS subject to send to (default: echo)"},
				},
				Required: []string{"message"},
			},
		},
		{
			Name:        "publish",
			Description: "Publish a message to a NATS subject (fire-and-forget, no reply expected).",
			InputSchema: inputSchema{
				Type: "object",
				Properties: map[string]property{
					"subject": {Type: "string", Description: "NATS subject to publish to"},
					"message": {Type: "string", Description: "The message payload"},
				},
				Required: []string{"subject", "message"},
			},
		},
		{
			Name:        "request",
			Description: "Send a request to a NATS subject and wait for a reply.",
			InputSchema: inputSchema{
				Type: "object",
				Properties: map[string]property{
					"subject": {Type: "string", Description: "NATS subject to send the request to"},
					"message": {Type: "string", Description: "The request payload"},
				},
				Required: []string{"subject", "message"},
			},
		},
	}

	// handleMCP processes a single MCP client connection over WebSocket.
	handleMCP := func(w http.ResponseWriter, r *http.Request) {
		ws, err := upgradeWebSocket(w, r)
		if err != nil {
			log.Printf("WebSocket upgrade failed: %v", err)
			return
		}
		defer ws.close()

		log.Printf("MCP client connected from %s", r.RemoteAddr)
		defer log.Printf("MCP client disconnected from %s", r.RemoteAddr)

		for {
			data, err := ws.readMessage()
			if err != nil {
				if err != io.EOF {
					log.Printf("Read error: %v", err)
				}
				return
			}

			var req jsonrpcRequest
			if err := json.Unmarshal(data, &req); err != nil {
				log.Printf("Invalid JSON-RPC message: %v", err)
				continue
			}

			switch req.Method {
			case "initialize":
				ws.writeJSON(jsonrpcResponse{
					JSONRPC: "2.0",
					ID:      req.ID,
					Result: initializeResult{
						ProtocolVersion: "2024-11-05",
						Capabilities: mcpCaps{
							Tools: &toolsCap{ListChanged: false},
						},
						ServerInfo: serverInfo{
							Name:    "nats-echo-mcp",
							Version: "0.1.0",
						},
					},
				})

			case "notifications/initialized":
				log.Printf("MCP session initialized")

			case "ping":
				ws.writeJSON(jsonrpcResponse{
					JSONRPC: "2.0",
					ID:      req.ID,
					Result:  map[string]interface{}{},
				})

			case "tools/list":
				ws.writeJSON(jsonrpcResponse{
					JSONRPC: "2.0",
					ID:      req.ID,
					Result:  toolsListResult{Tools: tools},
				})

			case "tools/call":
				var params callToolParams
				if err := json.Unmarshal(req.Params, &params); err != nil {
					ws.writeJSON(jsonrpcResponse{
						JSONRPC: "2.0",
						ID:      req.ID,
						Error:   &rpcError{Code: -32602, Message: "Invalid params: " + err.Error()},
					})
					continue
				}
				result := handleToolCall(nc, params)
				ws.writeJSON(jsonrpcResponse{
					JSONRPC: "2.0",
					ID:      req.ID,
					Result:  result,
				})

			default:
				if req.ID != nil {
					ws.writeJSON(jsonrpcResponse{
						JSONRPC: "2.0",
						ID:      req.ID,
						Error:   &rpcError{Code: -32601, Message: "Method not found: " + req.Method},
					})
				}
			}
		}
	}

	// Set up HTTP server.
	mux := http.NewServeMux()
	mux.HandleFunc("/", handleMCP)

	server := &http.Server{Addr: *addr, Handler: mux}

	go func() {
		c := make(chan os.Signal, 1)
		signal.Notify(c, os.Interrupt)
		<-c
		log.Println("Shutting down...")
		server.Close()
		nc.Drain()
	}()

	log.Printf("MCP WebSocket server listening on ws://localhost%s", *addr)
	log.Printf("Connect with: claude --mcp-config '{\"url\":\"ws://localhost%s\"}'", *addr)
	if err := server.ListenAndServe(); err != http.ErrServerClosed {
		log.Fatalf("Server error: %v", err)
	}
}

func handleToolCall(nc *nats.Conn, params callToolParams) callToolResult {
	switch params.Name {
	case "echo":
		var args echoArgs
		if err := json.Unmarshal(params.Arguments, &args); err != nil {
			return errResult("Error parsing arguments: " + err.Error())
		}
		subject := args.Subject
		if subject == "" {
			subject = "echo"
		}
		msg, err := nc.Request(subject, []byte(args.Message), 5*time.Second)
		if err != nil {
			return errResult(fmt.Sprintf("NATS request to %q failed: %v", subject, err))
		}
		return textResult(string(msg.Data))

	case "publish":
		var args publishArgs
		if err := json.Unmarshal(params.Arguments, &args); err != nil {
			return errResult("Error parsing arguments: " + err.Error())
		}
		if err := nc.Publish(args.Subject, []byte(args.Message)); err != nil {
			return errResult(fmt.Sprintf("NATS publish to %q failed: %v", args.Subject, err))
		}
		return textResult(fmt.Sprintf("Published to %q", args.Subject))

	case "request":
		var args requestArgs
		if err := json.Unmarshal(params.Arguments, &args); err != nil {
			return errResult("Error parsing arguments: " + err.Error())
		}
		msg, err := nc.Request(args.Subject, []byte(args.Message), 5*time.Second)
		if err != nil {
			return errResult(fmt.Sprintf("NATS request to %q failed: %v", args.Subject, err))
		}
		return textResult(string(msg.Data))

	default:
		return errResult(fmt.Sprintf("Unknown tool: %s", params.Name))
	}
}

func textResult(text string) callToolResult {
	return callToolResult{
		Content: []contentItem{{Type: "text", Text: text}},
	}
}

func errResult(text string) callToolResult {
	return callToolResult{
		Content: []contentItem{{Type: "text", Text: text}},
		IsError: true,
	}
}
