# nats-echo-mcp

An MCP (Model Context Protocol) server that bridges Claude (or any MCP client) to NATS messaging over WebSocket.

## Overview

This example starts a WebSocket server that speaks the MCP protocol. When Claude calls a tool, the server translates it into a NATS operation (request-reply or publish) and returns the result.

### Tools

| Tool | Description |
|------|-------------|
| `echo` | Send a message to a NATS subject (default: `echo`) and return the reply |
| `request` | Send a request to any NATS subject and wait for a reply |
| `publish` | Publish a fire-and-forget message to a NATS subject |

## Quick Start

You'll need three terminals:

**1. Start a NATS server** (if not already running):

```sh
nats-server
```

**2. Start a NATS echo responder** on the `echo` subject:

```sh
# Using the nats CLI
nats reply echo --command "echo '{{.Body}}'"

# Or using the nats-rply example from this repo
go run ../nats-rply/main.go echo "{{received}}"
```

**3. Start the MCP server:**

```sh
go run . -s nats://localhost:4222 -addr :1234
```

**4. Connect Claude:**

```sh
claude \
  --append-system-prompt 'IMPORTANT: You are connected to a NATS MCP server. Use the echo tool to send messages through NATS and get replies. Use the request tool to send requests to arbitrary NATS subjects. Use the publish tool for fire-and-forget messages.' \
  --mcp-config '{"url": "ws://localhost:1234"}'
```

## Flags

```
-s       NATS server URLs, comma-separated (default: nats://localhost:4222)
-addr    WebSocket listen address (default: :1234)
-h       Show help
```

## Architecture

```
Claude ──WebSocket──▶ nats-echo-mcp ──NATS──▶ responder(s)
  MCP client           MCP server             any NATS service
```

The MCP server acts as a bridge: it accepts MCP tool calls over WebSocket and translates them into NATS requests. This lets Claude interact with any NATS service without knowing anything about NATS itself.
