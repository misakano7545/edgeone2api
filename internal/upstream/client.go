package upstream

import (
	"bufio"
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"math/big"
	"net/http"
	"strings"
	"sync"
	"time"
)

// RPCRequest represents a client-request RPC message
type RPCRequest struct {
	Type    string      `json:"type"`
	RpcID   string      `json:"rpcId"`
	Method  string      `json:"method"`
	Payload interface{} `json:"payload"`
}

// RPCResponse represents a server-response RPC message
type RPCResponse struct {
	Type   string `json:"type"`
	RpcID  string `json:"rpcId"`
	Result struct {
		OK    bool            `json:"ok"`
		Value json.RawMessage `json:"value"`
		Error *RPCError       `json:"error"`
	} `json:"result"`
}

// RPCError represents an RPC error
type RPCError struct {
	Code    string      `json:"code"`
	Message string      `json:"message"`
	Details interface{} `json:"details"`
}

// ContentItem represents a content item in the session.prompt payload
type ContentItem struct {
	Type     string        `json:"type"`
	Text     string        `json:"text,omitempty"`
	ToolCall string        `json:"toolCallId,omitempty"`
	Content  []ContentPart `json:"content,omitempty"`
	IsError  bool          `json:"isError,omitempty"`
}

// ContentPart is a nested content part (for tool results)
type ContentPart struct {
	Type string `json:"type"`
	Text string `json:"text"`
}

// PromptRequest is the session.prompt payload
type PromptRequest struct {
	SessionID string        `json:"sessionId"`
	Mode      string        `json:"mode"`
	Content   []ContentItem `json:"content"`
}

// PromptResponse is the session.prompt RPC result (accepted: true)
type PromptResponse struct {
	Accepted bool `json:"accepted"`
}

// SSEEnvelope is a single frame received on the event stream
type SSEEnvelope struct {
	Type    string          `json:"type"`
	RpcID   string          `json:"rpcId"`
	Method  string          `json:"method"`
	Payload json.RawMessage `json:"payload"`
}

// SessionEvent is the payload of a session/event envelope
type SessionEvent struct {
	SessionID string `json:"sessionId"`
	Event     Event  `json:"event"`
}

// Event is a single session event
type Event struct {
	Type string          `json:"type"`
	Seq  int             `json:"seq"`
	Time int64           `json:"time"`
	Data json.RawMessage `json:"data"`
}

// Fingerprint holds browser-like headers that make each session look like a
// separate browser, reducing the chance of cross-session rate limiting.
type Fingerprint struct {
	UserAgent   string
	AcceptLang  string
	SecChUA     string
	SecChUAPlat string
}

// pool of real User-Agents used to generate random fingerprints
var uaPool = []string{
	"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36",
	"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/127.0.0.0 Safari/537.36",
	"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/128.0.0.0 Safari/537.36",
	"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/129.0.0.0 Safari/537.36",
	"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36",
	"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36",
	"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/128.0.0.0 Safari/537.36",
	"Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/127.0.0.0 Safari/537.36",
	"Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:128.0) Gecko/20100101 Firefox/128.0",
	"Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:130.0) Gecko/20100101 Firefox/130.0",
	"Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:129.0) Gecko/20100101 Firefox/129.0",
}

var secChUAPool = []string{
	`"Chromium";v="126", "Google Chrome";v="126"`,
	`"Chromium";v="127", "Google Chrome";v="127"`,
	`"Chromium";v="128", "Google Chrome";v="128"`,
	`"Chromium";v="129", "Not A(Brand";v="99"`,
	`"Chromium";v="130", "Google Chrome";v="130"`,
	`"Chromium";v="126", "Not A(Brand";v="8"`,
}

var platformPool = []string{"Windows", "macOS", "Linux"}

var langPool = []string{"zh-CN,zh;q=0.9", "zh-CN,zh;q=0.9,en;q=0.8", "en-US,en;q=0.9", "en-US,en;q=0.9,zh-CN;q=0.8", "zh-TW,zh;q=0.9,en;q=0.8"}

func randomFingerprint() Fingerprint {
	n, _ := rand.Int(rand.Reader, big.NewInt(int64(len(uaPool))))
	ua := uaPool[n.Int64()]
	n, _ = rand.Int(rand.Reader, big.NewInt(int64(len(secChUAPool))))
	sc := secChUAPool[n.Int64()]
	n, _ = rand.Int(rand.Reader, big.NewInt(int64(len(platformPool))))
	plat := platformPool[n.Int64()]
	n, _ = rand.Int(rand.Reader, big.NewInt(int64(len(langPool))))
	lang := langPool[n.Int64()]
	return Fingerprint{
		UserAgent:   ua,
		AcceptLang:  lang,
		SecChUA:     sc,
		SecChUAPlat: plat,
	}
}

// Client is the DeepSeek Harness upstream client
type Client struct {
	baseURL string
	fp      Fingerprint
	hc      *http.Client // for RPC requests (has overall timeout)
	sseHC   *http.Client // for SSE streams (no overall timeout)
}

// NewClient creates a new upstream client with a random browser fingerprint.
// baseURL is the web chat origin, e.g. https://deepseek-harness.edgeone.cool
// headerTimeoutRPC bounds control-plane RPCs (session.create / selectModel /
// prompt). These are normally fast but queue on an overloaded harness; 60s is
// generous yet still bounded.
const headerTimeoutRPC = 60 * time.Second

// headerTimeoutSSE bounds the events.mux handshake only. The harness holds the
// connection until the session backend is ready, which under load takes well
// past 30s; 90s gives it room without hanging forever. Once headers arrive the
// stream has no deadline (see StreamEvents idleTimeout).
const headerTimeoutSSE = 90 * time.Second

func newTransport(headerTimeout time.Duration) *http.Transport {
	return &http.Transport{
		MaxIdleConns:          100,
		MaxIdleConnsPerHost:   20,
		IdleConnTimeout:       90 * time.Second,
		TLSHandshakeTimeout:   15 * time.Second,
		ResponseHeaderTimeout: headerTimeout,
		Proxy:                 http.ProxyFromEnvironment,
	}
}

func NewClient(baseURL string) *Client {
	if baseURL == "" {
		baseURL = "https://deepseek-harness.edgeone.cool"
	}
	baseURL = strings.TrimSuffix(baseURL, "/")
	return &Client{
		baseURL: baseURL,
		fp:      randomFingerprint(),
		hc:      &http.Client{Timeout: 120 * time.Second, Transport: newTransport(headerTimeoutRPC)},
		sseHC:   &http.Client{Transport: newTransport(headerTimeoutSSE)}, // SSE stream: no overall timeout (active output must never be cut off)
	}
}

// FingerprintSignature returns a stable identity for this client's browser
// fingerprint, used to track exhausted fingerprints.
func (c *Client) FingerprintSignature() string {
	return c.fp.UserAgent + "\x00" + c.fp.SecChUA + "\x00" + c.fp.SecChUAPlat + "\x00" + c.fp.AcceptLang
}

// CreateSession creates a new session and returns conversation ID and session ID.
// agentPreset optionally specifies the agent preset ("makers", "minimal", etc.).
// If empty, the server-side default ("makers") is used.
func (c *Client) CreateSession(ctx context.Context, agentPreset string) (convID, sessionID string, err error) {
	return c.CreateSessionOpts(ctx, agentPreset, nil)
}

// CreateSessionOpts is CreateSession with extra session.create payload fields
// (used to probe harness knobs such as disabling tool injection).
func (c *Client) CreateSessionOpts(ctx context.Context, agentPreset string, extra map[string]any) (convID, sessionID string, err error) {
	convID = randHex(32)
	payload := map[string]interface{}{}
	if agentPreset != "" {
		payload["agentPreset"] = agentPreset
	}
	for k, v := range extra {
		payload[k] = v
	}
	rpcReq := RPCRequest{
		Type:    "client-request",
		RpcID:   genRpcID(),
		Method:  "session.create",
		Payload: payload,
	}

	body, err := json.Marshal(rpcReq)
	if err != nil {
		return "", "", err
	}

	req, err := http.NewRequestWithContext(ctx, "POST", c.baseURL+"/api/session.create", bytes.NewReader(body))
	if err != nil {
		return "", "", err
	}
	c.setHeaders(req, convID)

	resp, err := c.hc.Do(req)
	if err != nil {
		return "", "", err
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		return "", "", fmt.Errorf("create session: status %d", resp.StatusCode)
	}

	var rpcResp RPCResponse
	if err := json.NewDecoder(resp.Body).Decode(&rpcResp); err != nil {
		return "", "", err
	}
	if !rpcResp.Result.OK {
		return "", "", fmt.Errorf("session.create failed: %s", rpcErrMsg(&rpcResp))
	}

	var result struct {
		SessionID string `json:"sessionId"`
	}
	if err := json.Unmarshal(rpcResp.Result.Value, &result); err != nil {
		return "", "", err
	}
	return convID, result.SessionID, nil
}

// SelectModel changes the model for a session.  provider and model are required;
// reasoningEffort is optional ("off", "high", "max").
func (c *Client) SelectModel(ctx context.Context, sessionID, convID, provider, model, reasoningEffort string) error {
	payload := map[string]interface{}{
		"sessionId": sessionID,
		"provider":  provider,
		"model":     model,
	}
	if reasoningEffort != "" {
		payload["reasoningEffort"] = reasoningEffort
	}
	rpcReq := RPCRequest{
		Type:    "client-request",
		RpcID:   genRpcID(),
		Method:  "session.selectModel",
		Payload: payload,
	}

	body, err := json.Marshal(rpcReq)
	if err != nil {
		return err
	}

	req, err := http.NewRequestWithContext(ctx, "POST", c.baseURL+"/api/session.selectModel", bytes.NewReader(body))
	if err != nil {
		return err
	}
	c.setHeaders(req, convID)

	resp, err := c.hc.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		return fmt.Errorf("session.selectModel: status %d", resp.StatusCode)
	}

	var rpcResp RPCResponse
	if err := json.NewDecoder(resp.Body).Decode(&rpcResp); err != nil {
		return err
	}
	if !rpcResp.Result.OK {
		return fmt.Errorf("session.selectModel failed: %s", rpcErrMsg(&rpcResp))
	}
	return nil
}

// SendPrompt sends a prompt to a session. mode is "steer" (direct LLM) or "queue".
func (c *Client) SendPrompt(ctx context.Context, sessionID, convID string, items []ContentItem) error {
	prompt := PromptRequest{
		SessionID: sessionID,
		Mode:      "steer",
		Content:   items,
	}
	rpcReq := RPCRequest{
		Type:    "client-request",
		RpcID:   genRpcID(),
		Method:  "session.prompt",
		Payload: prompt,
	}

	body, err := json.Marshal(rpcReq)
	if err != nil {
		return err
	}

	req, err := http.NewRequestWithContext(ctx, "POST", c.baseURL+"/api/session.prompt", bytes.NewReader(body))
	if err != nil {
		return err
	}
	c.setHeaders(req, convID)

	resp, err := c.hc.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		return fmt.Errorf("session.prompt: status %d", resp.StatusCode)
	}

	var rpcResp RPCResponse
	if err := json.NewDecoder(resp.Body).Decode(&rpcResp); err != nil {
		return err
	}
	if !rpcResp.Result.OK {
		return fmt.Errorf("session.prompt failed: %s", rpcErrMsg(&rpcResp))
	}

	var pr PromptResponse
	if err := json.Unmarshal(rpcResp.Result.Value, &pr); err != nil {
		return err
	}
	if !pr.Accepted {
		return fmt.Errorf("prompt not accepted")
	}
	return nil
}

// OpenEventStream connects to /api/events.mux and returns a channel of envelopes.
// It returns a cancel function to close the stream.
func (c *Client) OpenEventStream(ctx context.Context, convID string) (<-chan SSEEnvelope, func(), error) {
	req, err := http.NewRequestWithContext(ctx, "GET", c.baseURL+"/api/events.mux", nil)
	if err != nil {
		return nil, nil, err
	}
	c.setHeaders(req, convID)
	req.Header.Set("Accept", "text/event-stream")

	resp, err := c.sseHC.Do(req)
	if err != nil {
		return nil, nil, err
	}

	if resp.StatusCode != 200 {
		resp.Body.Close()
		return nil, nil, fmt.Errorf("events.mux: status %d", resp.StatusCode)
	}

	ch := make(chan SSEEnvelope, 256)
	done := make(chan struct{})
	var once sync.Once
	cancel := func() {
		once.Do(func() {
			close(done)
			resp.Body.Close()
		})
	}

	go func() {
		defer close(ch)
		defer resp.Body.Close()
		scanner := bufio.NewScanner(resp.Body)
		scanner.Buffer(make([]byte, 64*1024), 16*1024*1024)

		var data strings.Builder
		emit := func() {
			if data.Len() == 0 {
				return
			}
			raw := strings.TrimSpace(data.String())
			data.Reset()
			if raw == "" {
				return
			}
			var env SSEEnvelope
			if err := json.Unmarshal([]byte(raw), &env); err != nil {
				return
			}
			select {
			case ch <- env:
			case <-done:
			}
		}

		for scanner.Scan() {
			line := scanner.Text()
			if strings.HasPrefix(line, "data:") {
				data.WriteString(strings.TrimPrefix(line, "data:"))
				data.WriteString("\n")
			} else if line == "" {
				emit()
			}
			select {
			case <-done:
				return
			default:
			}
		}
		emit() // flush the last event
	}()

	return ch, cancel, nil
}

// setHeaders sets common headers for requests, using the client's browser fingerprint.
func (c *Client) setHeaders(req *http.Request, convID string) {
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Origin", c.baseURL)
	req.Header.Set("Referer", c.baseURL+"/")
	req.Header.Set("User-Agent", c.fp.UserAgent)
	req.Header.Set("Accept-Language", c.fp.AcceptLang)
	req.Header.Set("Sec-CH-UA", c.fp.SecChUA)
	req.Header.Set("Sec-CH-UA-Platform", c.fp.SecChUAPlat)
	req.Header.Set("Sec-CH-UA-Mobile", "?0")
	req.Header.Set("makers-conversation-id", convID)
}

func rpcErrMsg(r *RPCResponse) string {
	if r.Result.Error == nil {
		return "unknown error"
	}
	if r.Result.Error.Message != "" {
		return r.Result.Error.Message
	}
	return "code " + r.Result.Error.Code
}

func genRpcID() string {
	return fmt.Sprintf("rpc-%d-%s", time.Now().UnixMilli(), randHex(8))
}

func randHex(n int) string {
	b := make([]byte, (n+1)/2)
	if _, err := rand.Read(b); err != nil {
		// fallback to timestamp-based entropy
		b = []byte(fmt.Sprintf("%d", time.Now().UnixNano()))
	}
	return hex.EncodeToString(b)[:n]
}

// AssistantChunk is a parsed text/reasoning/tool-call delta from an assistant/chunk event
type AssistantChunk struct {
	Text         string
	Reasoning    string
	ToolCall     *AssistantToolCall
	IsDone       bool
	FinishReason string // "stop" | "tool_calls"
}

// AssistantToolCall represents a tool call from the model
type AssistantToolCall struct {
	Index          int
	ID             string
	Name           string
	Arguments      string // accumulated arguments
	ArgumentsDelta string // current delta
	IsComplete     bool   // true for block-end (full arguments available)
}

// ChatResult holds the final outcome of a chat turn
type ChatResult struct {
	Text         string
	Reasoning    string // accumulated reasoning-delta text (thinking trace)
	FinishReason string // "stop" | "tool_calls"
	ToolCalls    []AssistantToolCall
}

// ChatStream holds an active chat session: SSE stream + prompt context
type ChatStream struct {
	envCh     <-chan SSEEnvelope
	cancelSSE func()
}

// StartChat opens the event stream and submits the prompt (steer mode).
// If the prompt is rejected (e.g. quota/rate limit), it returns an error and
// the caller may retry with a fresh session. On success, call StreamEvents.
func (c *Client) StartChat(ctx context.Context, sessionID, convID string, items []ContentItem) (*ChatStream, error) {
	streamCtx, cancelStream := context.WithCancel(ctx)

	envCh, cancelSSE, err := c.OpenEventStream(streamCtx, convID)
	if err != nil {
		cancelStream()
		return nil, fmt.Errorf("open event stream: %w", err)
	}

	// send prompt after the SSE stream is established
	if err := c.SendPrompt(ctx, sessionID, convID, items); err != nil {
		cancelSSE()
		cancelStream()
		return nil, err
	}

	return &ChatStream{
		envCh:     envCh,
		cancelSSE: func() { cancelSSE(); cancelStream() },
	}, nil
}

func (c *Client) InitSession(ctx context.Context, sessionID, convID string) error {
	cs, err := c.StartChat(ctx, sessionID, convID, InitPrompt())
	if err != nil {
		return err
	}
	defer cs.Cancel()
	_, err = c.StreamEvents(ctx, cs, nil, false, 0)
	return err
}

// Cancel aborts the chat stream early
func (cs *ChatStream) Cancel() {
	if cs != nil && cs.cancelSSE != nil {
		cs.cancelSSE()
	}
}

// StreamEvents consumes the SSE event stream, yielding deltas via onDelta,
// until the turn ends. Returns the accumulated result.
//
// When textOnly is true (kuku2api-style plain-text proxy mode), tool-call
// events are folded away: native tool blocks are dropped, the finish reason
// is normalized to "stop", and a tool-driven turn does not terminate the
// round — the stream keeps being consumed so any follow-up text produced by
// the upstream agent loop can still flow through. The client therefore never
// sees a bare tool_calls chunk or a blank agent-loop reply.
//
// idleTimeout guards against a dead upstream: if no SSE event arrives for
// that long, the stream is aborted with an error.  It resets on every event,
// so a streaming model that keeps emitting is never cut off; 0 disables it.
// There is deliberately no overall deadline here — the caller's ctx (client
// disconnect, non-streaming timeout) is the only other stop condition.
//
// ctx can be used for timeout; nil means no deadline.
func (c *Client) StreamEvents(ctx context.Context, cs *ChatStream, onDelta func(AssistantChunk), textOnly bool, idleTimeout time.Duration) (ChatResult, error) {
	var sb strings.Builder
	var reasoningSb strings.Builder
	var turn int
	finishReason := "stop"
	toolCalls := make(map[int]*AssistantToolCall)
	var toolCallOrder []int // preserves the order tool calls first appear
	droppedTools := false   // textOnly: a tool-call was folded this turn

	var idleTimer *time.Timer
	var idleC <-chan time.Time
	if idleTimeout > 0 {
		idleTimer = time.NewTimer(idleTimeout)
		idleC = idleTimer.C
		defer idleTimer.Stop()
	}
	resetIdle := func() {
		if idleTimer == nil {
			return
		}
		if !idleTimer.Stop() {
			select {
			case <-idleTimer.C:
			default:
			}
		}
		idleTimer.Reset(idleTimeout)
	}

	result := func() ChatResult {
		ordered := make([]AssistantToolCall, 0, len(toolCallOrder))
		for _, idx := range toolCallOrder {
			if tc, ok := toolCalls[idx]; ok && tc.Name != "" {
				ordered = append(ordered, *tc)
			}
		}
		return ChatResult{Text: sb.String(), Reasoning: reasoningSb.String(), FinishReason: finishReason, ToolCalls: ordered}
	}

	for {
		select {
		case <-ctx.Done():
			return result(), ctx.Err()
		case <-idleC:
			return result(), fmt.Errorf("stream idle timeout: no upstream event for %s", idleTimeout)
		case env, ok := <-cs.envCh:
			if !ok {
				return result(), nil
			}
			// Any SSE event counts as liveness: a model mid-thought or
			// mid-stream keeps the connection alive and is never cut off.
			resetIdle()
			if env.Type != "server-request" || env.Method != "session/event" {
				continue
			}
			se := SessionEvent{}
			if err := json.Unmarshal(env.Payload, &se); err != nil {
				continue
			}
			switch se.Event.Type {
			case "turn/start":
				var d struct {
					Turn int `json:"turn"`
				}
				json.Unmarshal(se.Event.Data, &d)
				turn = d.Turn
				droppedTools = false

			case "assistant/chunk":
				if turn == 0 {
					continue
				}
				var d struct {
					Turn  int             `json:"turn"`
					Step  int             `json:"step"`
					Chunk json.RawMessage `json:"chunk"`
				}
				if err := json.Unmarshal(se.Event.Data, &d); err != nil || d.Turn != turn {
					continue
				}
				var typeOnly struct {
					Type string `json:"type"`
				}
				json.Unmarshal(d.Chunk, &typeOnly)
				switch typeOnly.Type {
				case "text-delta":
					var cd struct {
						Text string `json:"text"`
					}
					json.Unmarshal(d.Chunk, &cd)
					if cd.Text != "" {
						sb.WriteString(cd.Text)
						if onDelta != nil {
							onDelta(AssistantChunk{Text: cd.Text})
						}
					}
				case "reasoning-delta":
					var cd struct {
						Text string `json:"text"`
					}
					json.Unmarshal(d.Chunk, &cd)
					if cd.Text != "" {
						reasoningSb.WriteString(cd.Text)
						if onDelta != nil {
							onDelta(AssistantChunk{Reasoning: cd.Text})
						}
					}
				case "block-start":
					var cd struct {
						BlockType string `json:"blockType"`
					}
					json.Unmarshal(d.Chunk, &cd)
					if cd.BlockType == "tool-call" {
						if textOnly {
							droppedTools = true
							log.Printf("[STREAM] textOnly: dropping tool-call block")
							continue
						}
						if onDelta != nil {
							onDelta(AssistantChunk{ToolCall: &AssistantToolCall{}})
						}
					}
				case "tool-call-delta":
					var cd struct {
						Index          int    `json:"index"`
						ID             string `json:"id"`
						Name           string `json:"name"`
						ArgumentsDelta string `json:"argumentsDelta"`
					}
					json.Unmarshal(d.Chunk, &cd)
					if textOnly {
						droppedTools = true
						continue
					}
					tc := toolCalls[cd.Index]
					if tc == nil {
						tc = &AssistantToolCall{Index: cd.Index}
						toolCalls[cd.Index] = tc
						toolCallOrder = append(toolCallOrder, cd.Index)
					}
					if cd.ID != "" {
						tc.ID = cd.ID
					}
					if cd.Name != "" {
						tc.Name = cd.Name
					}
					if cd.ArgumentsDelta != "" {
						tc.ArgumentsDelta = cd.ArgumentsDelta
						tc.Arguments += cd.ArgumentsDelta
					}
					finishReason = "tool_calls"
					if onDelta != nil {
						onDelta(AssistantChunk{
							ToolCall: &AssistantToolCall{
								Index:          cd.Index,
								ID:             tc.ID,
								Name:           tc.Name,
								ArgumentsDelta: cd.ArgumentsDelta,
								Arguments:      tc.Arguments,
							},
						})
					}
				case "block-end":
					var cd struct {
						Index int `json:"index"`
						Block *struct {
							Type      string `json:"type"`
							ID        string `json:"id"`
							Name      string `json:"name"`
							Arguments string `json:"arguments"`
						} `json:"block"`
					}
					json.Unmarshal(d.Chunk, &cd)
					if textOnly {
						if cd.Block != nil && cd.Block.Type == "tool-call" {
							droppedTools = true
						}
						continue
					}
					if cd.Block != nil && cd.Block.Type == "tool-call" {
						if tc, ok := toolCalls[cd.Index]; ok && cd.Block.Arguments != "" {
							tc.Arguments = cd.Block.Arguments
							tc.ID = cd.Block.ID
							tc.Name = cd.Block.Name
						}
						if onDelta != nil {
							onDelta(AssistantChunk{
								ToolCall: &AssistantToolCall{
									Index:      cd.Index,
									ID:         cd.Block.ID,
									Name:       cd.Block.Name,
									Arguments:  cd.Block.Arguments,
									IsComplete: true,
								},
							})
						}
					}
				case "finish":
					var cd struct {
						Reason struct {
							Kind string `json:"kind"`
						} `json:"reason"`
					}
					json.Unmarshal(d.Chunk, &cd)
					if cd.Reason.Kind == "tool-calls" && !textOnly {
						finishReason = "tool_calls"
					}
				}

			case "turn/end":
				var d struct {
					Turn int `json:"turn"`
				}
				json.Unmarshal(se.Event.Data, &d)
				if turn > 0 && d.Turn == turn {
					if textOnly && droppedTools {
						continue // tool-driven turn: keep consuming follow-up turns
					}
					if onDelta != nil {
						onDelta(AssistantChunk{IsDone: true, FinishReason: finishReason})
					}
					return result(), nil
				}

			case "stream/error":
				var d struct {
					Error struct {
						Message string `json:"message"`
					} `json:"error"`
				}
				json.Unmarshal(se.Event.Data, &d)
				return result(), fmt.Errorf("stream error: %s", d.Error.Message)
			}
		}
	}
}

// ensure io is referenced (reserved for future body streaming helpers)
var _ io.Reader

// IsQuotaError checks if an error from the upstream indicates a quota/rate-limit
// condition that should trigger session rotation.
func IsQuotaError(err error) bool {
	if err == nil {
		return false
	}
	msg := strings.ToLower(err.Error())
	for _, pat := range []string{"rate limit", "quota", "429", "too many", "capacity exceeded", "timed out"} {
		if strings.Contains(msg, pat) {
			return true
		}
	}
	return false
}

// IsSessionNotFound reports whether the upstream error means the underlying
// session was destroyed server-side (e.g. the free session aged past the
// upstream's lifetime and became a zombie).  Callers should reacquire a fresh
// session and retry once rather than surface a 502.
func IsSessionNotFound(err error) bool {
	if err == nil {
		return false
	}
	return strings.Contains(strings.ToLower(err.Error()), "not found")
}

// IsUnsupportedReasoningEffort reports whether the upstream refused the model
// switch because the requested reasoning effort does not apply to that model
// (kimi / hy3 / minimax expose no reasoning knob and reject any value,
// including "off").  Callers should retry the switch with no effort rather
// than leave the session on its previous model.
func IsUnsupportedReasoningEffort(err error) bool {
	if err == nil {
		return false
	}
	return strings.Contains(strings.ToLower(err.Error()), "does not support reasoning effort")
}
