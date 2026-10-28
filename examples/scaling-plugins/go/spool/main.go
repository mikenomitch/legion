// Command spool is a Legion scaling plugin, a source and a target, written
// against the `legion-scaling/1` protocol (docs/autoscaling-plugins.md) with
// the Go standard library only.
//
// It works on a spool directory, which the operator names in its config:
//
//   - as a source, `query = "<queue>"` reads how many files wait in
//     `<dir>/queues/<queue>/` (a queue's depth);
//   - as a target, the policy's count lives in `<dir>/targets/<policy>`, a
//     stand-in for a real provider's API that `target/scale` writes and
//     `target/status` reads.
//
// Install it in the autoscaler's app config:
//
//	plugins = { spool = { command = ["/opt/legion/spool"], config = { dir = "/var/spool/render" } } }
package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
)

const protocol = "legion-scaling/1"

var safeName = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._-]*$`)

type request struct {
	ID     json.RawMessage `json:"id"`
	Method string          `json:"method"`
	Params json.RawMessage `json:"params"`
}

type rpcError struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
}

func (e *rpcError) Error() string { return e.Message }

func badParams(format string, args ...any) error {
	return &rpcError{Code: -32602, Message: fmt.Sprintf(format, args...)}
}

type plugin struct{ dir string }

func (p *plugin) initialize(raw json.RawMessage) (any, error) {
	var params struct {
		Protocol string `json:"protocol"`
		Config   struct {
			Dir string `json:"dir"`
		} `json:"config"`
	}
	if err := json.Unmarshal(raw, &params); err != nil {
		return nil, badParams("initialize: %v", err)
	}
	if params.Protocol != protocol {
		return nil, badParams("unsupported protocol %q", params.Protocol)
	}
	if params.Config.Dir == "" {
		return nil, badParams("config.dir is required: the spool directory")
	}
	p.dir = params.Config.Dir
	return map[string]any{
		"name":         "spool",
		"version":      "0.1.0",
		"kinds":        []string{"source", "target"},
		"capabilities": map[string]any{"nodes": false},
		"policyConfigSchema": map[string]any{
			"type":                 "object",
			"properties":           map[string]any{"label": map[string]any{"type": "string"}},
			"additionalProperties": false,
		},
	}, nil
}

func (p *plugin) query(raw json.RawMessage) (any, error) {
	var params struct {
		Query string `json:"query"`
	}
	if err := json.Unmarshal(raw, &params); err != nil {
		return nil, badParams("source/query: %v", err)
	}
	if !safeName.MatchString(params.Query) {
		return nil, badParams("query must name a queue (letters, digits, '.', '_', '-')")
	}
	entries, err := os.ReadDir(filepath.Join(p.dir, "queues", params.Query))
	if os.IsNotExist(err) {
		return map[string]any{"value": nil, "reason": "no queue " + params.Query}, nil
	}
	if err != nil {
		return nil, err
	}
	return map[string]any{"value": len(entries)}, nil
}

func (p *plugin) countFile(raw json.RawMessage) (string, json.RawMessage, error) {
	var params struct {
		Policy  string          `json:"policy"`
		Desired json.RawMessage `json:"desired"`
	}
	if err := json.Unmarshal(raw, &params); err != nil {
		return "", nil, badParams("%v", err)
	}
	name := strings.ReplaceAll(params.Policy, "/", "_")
	if !safeName.MatchString(name) {
		return "", nil, badParams("policy must be a policy name")
	}
	return filepath.Join(p.dir, "targets", name), params.Desired, nil
}

func (p *plugin) status(raw json.RawMessage) (any, error) {
	path, _, err := p.countFile(raw)
	if err != nil {
		return nil, err
	}
	count := 0
	if text, err := os.ReadFile(path); err == nil {
		if count, err = strconv.Atoi(strings.TrimSpace(string(text))); err != nil {
			return nil, fmt.Errorf("%s does not hold a count", path)
		}
	} else if !os.IsNotExist(err) {
		return nil, err
	}
	return map[string]any{"count": count, "ready": count}, nil
}

func (p *plugin) scale(raw json.RawMessage) (any, error) {
	path, desired, err := p.countFile(raw)
	if err != nil {
		return nil, err
	}
	count, err := strconv.ParseUint(string(desired), 10, 32)
	if err != nil {
		return nil, badParams("desired must be a count")
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return nil, err
	}
	return map[string]any{}, os.WriteFile(path, []byte(strconv.FormatUint(count, 10)+"\n"), 0o644)
}

func (p *plugin) handle(line []byte) map[string]any {
	reply := map[string]any{"jsonrpc": "2.0", "id": nil}
	var req request
	if err := json.Unmarshal(line, &req); err != nil {
		reply["error"] = rpcError{Code: -32700, Message: "parse error"}
		return reply
	}
	if len(req.ID) > 0 {
		reply["id"] = req.ID
	}
	methods := map[string]func(json.RawMessage) (any, error){
		"initialize":    p.initialize,
		"source/query":  p.query,
		"target/status": p.status,
		"target/scale":  p.scale,
	}
	method, ok := methods[req.Method]
	if !ok {
		reply["error"] = rpcError{Code: -32601, Message: "method not found: " + req.Method}
		return reply
	}
	if req.Method != "initialize" && p.dir == "" {
		reply["error"] = rpcError{Code: -32002, Message: "not initialized"}
		return reply
	}
	result, err := method(req.Params)
	if rerr, isRPC := err.(*rpcError); isRPC {
		reply["error"] = rerr
	} else if err != nil {
		reply["error"] = rpcError{Code: -32000, Message: err.Error()}
	} else {
		reply["result"] = result
	}
	return reply
}

func main() {
	p := &plugin{}
	in := bufio.NewScanner(os.Stdin)
	in.Buffer(make([]byte, 1<<20), 1<<20)
	out := json.NewEncoder(os.Stdout)
	for in.Scan() {
		if len(strings.TrimSpace(in.Text())) == 0 {
			continue
		}
		if err := out.Encode(p.handle(in.Bytes())); err != nil {
			fmt.Fprintln(os.Stderr, "spool:", err)
			os.Exit(1)
		}
	}
}
