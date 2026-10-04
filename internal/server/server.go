package server

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"log"
	mathrand "math/rand"
	"net/http"
	"strings"
	"sync"
	"time"

	"edgeone2api/internal/auth"
	"edgeone2api/internal/config"
	"edgeone2api/internal/upstream"
)

// normalizeReasoningEffort maps client-supplied reasoning strength values to
// the upstream's accepted set ("off", "high", "max").  OpenAI-standard
// values (low/medium/high) and common aliases are normalized so downstream
// clients can send reasoning_effort without hitting an upstream rejection:
//
//	low / medium / high  -> high   (lowest upstream on-state)
//	max / extreme        -> max
//	off / none / false   -> off
//	anything else        -> ""    (unset: upstream default)
func normalizeReasoningEffort(s string) string {
	switch strings.ToLower(strings.TrimSpace(s)) {
	case "":
		return ""
	case "off", "none", "false", "0":
		return "off"
	case "low", "medium", "high":
		return "high"
	case "max", "extreme", "high-max":
		return "max"
	default:
		return ""
	}
}

// Server is an OpenAI-compatible API server backed by DeepSeek Harness sessions
type Server struct {
	pool     *auth.Pool
	apiKey   string
	models   []string
	timeout  time.Duration // non-streaming overall timeout
	streamIdle time.Duration // streaming: no event for this long = dead (0 = disabled)
	defaultReasoningEffort string // fallback when client & model_map don't specify
	modelMap map[string]config.ModelMapping
	jitterMs int // pseudo-concurrency: random pre-send delay [0, jitterMs)
	rng      *mathrand.Rand
	rngMu    sync.Mutex
	sem      chan struct{} // in-flight upstream request cap (nil = unlimited)
}

// New creates a new server
func New(pool *auth.Pool, apiKey string, models []string, timeout, streamIdle time.Duration, modelMap map[string]config.ModelMapping, defaultReasoningEffort string, jitterMs, maxConcurrent int) *Server {
	if len(models) == 0 {
		models = []string{"@makers/deepseek-v4-flash", "@makers/deepseek-v4-pro"}
	}
	if modelMap == nil {
		modelMap = map[string]config.ModelMapping{}
	}
	s := &Server{
		pool:       pool,
		apiKey:     apiKey,
		models:     models,
		timeout:    timeout,
		streamIdle: streamIdle,
		defaultReasoningEffort: defaultReasoningEffort,
		modelMap:   modelMap,
		jitterMs:   jitterMs,
		rng:        mathrand.New(mathrand.NewSource(time.Now().UnixNano())),
	}
	if maxConcurrent > 0 {
		s.sem = make(chan struct{}, maxConcurrent)
	}
	return s
}

// Handler returns the HTTP handler
func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/models", s.handleModels)
	mux.HandleFunc("/v1/chat/completions", s.handleChat)
	mux.HandleFunc("/healthz", s.handleHealth)
	mux.HandleFunc("/pool", s.handlePool)
	return mux
}

func (s *Server) auth(r *http.Request) bool {
	if s.apiKey == "" {
		return true
	}
	got := strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer ")
	return strings.TrimSpace(got) == s.apiKey
}

// --- Models ---

func (s *Server) handleModels(w http.ResponseWriter, r *http.Request) {
	if !s.auth(r) {
		writeJSON(w, http.StatusUnauthorized, map[string]any{"error": map[string]any{"message": "invalid api key", "type": "auth_error"}})
		return
	}
	data := make([]map[string]any, 0, len(s.models))
	for _, m := range s.models {
		data = append(data, map[string]any{"id": m, "object": "model", "created": time.Now().Unix(), "owned_by": "deepseek-harness"})
	}
	writeJSON(w, http.StatusOK, map[string]any{"object": "list", "data": data})
}

// --- Request types ---

type openaiChatRequest struct {
	Model           string          `json:"model"`
	Messages        []openaiMessage `json:"messages"`
	Stream          bool            `json:"stream"`
	MaxTokens       int             `json:"max_tokens"`
	Temperature     float64         `json:"temperature"`
	ReasoningEffort string          `json:"reasoning_effort"`
	Tools           []openaiTool    `json:"tools"`
}

type openaiTool struct {
	Type     string         `json:"type"`
	Function openaiToolFunc `json:"function"`
}

type openaiToolFunc struct {
	Name        string          `json:"name"`
	Description string          `json:"description"`
	Parameters  json.RawMessage `json:"parameters"`
}

// openaiToolCall mirrors the OpenAI tool_calls message field (parsed from
// client history and returned to the client for execution).
type openaiToolCall struct {
	ID       string           `json:"id"`
	Type     string           `json:"type"`
	Function openaiToolCallFn `json:"function"`
}

type openaiToolCallFn struct {
	Name      string `json:"name"`
	Arguments string `json:"arguments"`
}

type openaiMessage struct {
	Role       string           `json:"role"`
	Content    json.RawMessage  `json:"content"`
	ToolCalls  []openaiToolCall `json:"tool_calls,omitempty"`
	ToolCallID string           `json:"tool_call_id,omitempty"`
}

// --- Chat Completions ---

// resolveSessionKey picks the sticky key for a chat request: an explicit
// X-Session-Key header wins (backward compatible); otherwise the first
// conversation key found in the request body (transparent stickiness, ported
// from workbuddy2api).  Empty means no stickiness — stateless free-pool
// session.
func resolveSessionKey(headerKey string, body []byte) string {
	if headerKey != "" {
		return headerKey
	}
	return auth.ExtractKey(body)
}

func (s *Server) handleChat(w http.ResponseWriter, r *http.Request) {
	if !s.auth(r) {
		writeJSON(w, http.StatusUnauthorized, map[string]any{"error": map[string]any{"message": "invalid api key", "type": "auth_error"}})
		return
	}
	if r.Method != http.MethodPost {
		writeJSON(w, http.StatusMethodNotAllowed, map[string]any{"error": "method not allowed"})
		return
	}

	body, err := io.ReadAll(io.LimitReader(r.Body, 64<<20))
	if err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]any{"error": "read body: " + err.Error()})
		return
	}

	var req openaiChatRequest
	if err := json.Unmarshal(body, &req); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]any{"error": "invalid json: " + err.Error()})
		return
	}
	if len(req.Messages) == 0 {
		writeJSON(w, http.StatusBadRequest, map[string]any{"error": "messages is required"})
		return
	}

	model := req.Model
	if model == "" {
		model = s.models[0]
	}

	log.Printf("[CHAT] model=%s stream=%v msgs=%d", model, req.Stream, len(req.Messages))

	// Pseudo-concurrency: spread concurrent requests with a random delay so
	// they don't hit the upstream in the same instant (its rate limiter trips
	// on simultaneous session.prompt bursts and returns 502/timeouts).
	// Applied before session acquisition so the delay doesn't hold a session.
	if s.jitterMs > 0 {
		s.rngMu.Lock()
		d := time.Duration(s.rng.Intn(s.jitterMs)) * time.Millisecond
		s.rngMu.Unlock()
		if d > 0 {
			time.Sleep(d)
		}
	}

	// Concurrency gate: cap in-flight upstream requests so a burst of client
	// concurrency never overloads the upstream (it degrades to 502/timeouts
	// under sustained parallel load).  Excess requests queue here; from the
	// client's perspective the requests still complete, just slightly later.
	// Waits on the client connection context, not the 180s upstream timeout,
	// so queueing time does not eat into the per-request upstream budget.
	if s.sem != nil {
		select {
		case s.sem <- struct{}{}:
			defer func() { <-s.sem }()
		case <-r.Context().Done():
			writeJSON(w, http.StatusServiceUnavailable, map[string]any{"error": map[string]any{"message": "server busy, retry later", "type": "server_error"}})
			return
		}
	}

	// Session affinity: a client that sends X-Session-Key gets a bound,
	// stateful session for continuity.  Anonymous requests (no header) fall
	// back to a transparent body-derived key (metadata.conversation_id /
	// conversationId / user_id, workbuddy2api-style ExtractKey) so the same
	// conversation sticks to the same upstream session without client
	// cooperation.  Requests with neither use a stateless free-pool session
	// and release it afterwards, so they can not exhaust the pool.
	headerKey := r.Header.Get("X-Session-Key")
	sessionKey := resolveSessionKey(headerKey, body)
	bound := sessionKey != ""
	// Echo the binding only when the client opted in with a header; a
	// body-derived key is the client's own conversation id, no need to invent
	// a header it did not ask for.
	if headerKey != "" {
		w.Header().Set("X-Session-Key", headerKey)
	}

	// Timeout semantics differ by mode:
	//   - Non-streaming: overall budget (s.timeout) covers the whole round-trip.
	//   - Streaming: NO overall deadline — an active upstream that keeps
	//     emitting events must never be cut off mid-stream.  The client's own
	//     disconnect cancels r.Context() naturally; a dead upstream is caught
	//     by the per-event idle timeout inside StreamEvents.  The same ctx is
	//     handed to StartChat, whose internal SSE/RPC setup has its own guards
	//     (RPC client timeout + SSE response-header timeout).
	ctx := r.Context()
	if !req.Stream && s.timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(r.Context(), s.timeout)
		defer cancel()
	}

	var session *auth.Session
	var release func(*auth.Session, bool)
	// reacquire grabs a replacement session for the same affinity: a bound key
	// rebinds, an anonymous request takes any free session.  Used whenever the
	// current session turns out to be dead (upstream reaps idle sessions well
	// before our own FreeTTL) so the request rotates transparently.
	reacquire := func(ctx context.Context) (*auth.Session, error) {
		if bound {
			return s.pool.Bind(ctx, sessionKey)
		}
		return s.pool.Acquire(ctx)
	}
	if bound {
		session, err = s.pool.Bind(ctx, sessionKey)
		if err != nil {
			writeJSON(w, http.StatusServiceUnavailable, map[string]any{"error": map[string]any{"message": "no available session: " + err.Error(), "type": "server_error"}})
			return
		}
		release = func(sess *auth.Session, success bool) { s.pool.ReleaseBind(sessionKey, sess, success) }
	} else {
		session, err = s.pool.Acquire(ctx)
		if err != nil {
			writeJSON(w, http.StatusServiceUnavailable, map[string]any{"error": map[string]any{"message": "no available session: " + err.Error(), "type": "server_error"}})
			return
		}
		release = func(sess *auth.Session, success bool) { s.pool.Release(sess, success) }
	}

	// Resolve model mapping and apply selectModel if needed.
	mm, ok := s.modelMap[model]
	if !ok {
		// Fallback: treat the model string as-is with edgeone-makers provider
		mm = config.ModelMapping{Provider: "edgeone-makers", Model: model}
	}
	// Reasoning effort precedence: client request > model_map mapping >
	// global default_reasoning_effort (config) > unset (upstream default).
	re := normalizeReasoningEffort(req.ReasoningEffort)
	if re == "" {
		re = normalizeReasoningEffort(mm.ReasoningEffort)
	}
	if re == "" {
		re = normalizeReasoningEffort(s.defaultReasoningEffort)
	}
	selKey := mm.Provider + "/" + mm.Model + "/" + re
	if session.SelectedModel != selKey {
		err := session.Client.SelectModel(ctx, session.SessionID, session.ConversationID, mm.Provider, mm.Model, re)
		if err != nil && upstream.IsSessionNotFound(err) {
			// The harness reaps idle sessions server-side well before our own
			// FreeTTL window, so this is normal lifecycle — not a quota event.
			// Rebind a fresh session and select the model once more instead of
			// proceeding with a dead session (the direct source of the 502).
			log.Printf("[SELECTMODEL] %s: session gone, reacquiring: %v", selKey, err)
			session.MarkGone()
			release(session, false)
			session, err = reacquire(ctx)
			if err != nil {
				writeJSON(w, http.StatusServiceUnavailable, map[string]any{"error": map[string]any{"message": "no available session: " + err.Error(), "type": "server_error"}})
				return
			}
			err = session.Client.SelectModel(ctx, session.SessionID, session.ConversationID, mm.Provider, mm.Model, re)
		}
		if err != nil && upstream.IsUnsupportedReasoningEffort(err) {
			// Models with no reasoning knob (kimi / hy3 / minimax) reject every
			// effort value, "off" included, because the parameter simply does not
			// apply to them.  Retry without it so the switch actually lands:
			// otherwise SelectedModel is never set, the session stays on its
			// previous model, and the caller silently gets a model they never
			// asked for (ask for kimi, get answered by deepseek-v4-flash).
			log.Printf("[SELECTMODEL] %s: model has no reasoning support upstream, retrying without effort: %v", selKey, err)
			re = ""
			selKey = mm.Provider + "/" + mm.Model + "/" + re
			err = session.Client.SelectModel(ctx, session.SessionID, session.ConversationID, mm.Provider, mm.Model, re)
		}
		if err != nil {
			log.Printf("[SELECTMODEL] %s: %v", selKey, err)
		} else {
			session.SelectedModel = selKey
			log.Printf("[SELECTMODEL] session=%s model=%s", session.SessionID, selKey)
		}
	}

	chatID := "chatcmpl-" + randHex(24)
	created := time.Now().Unix()

	// Dual mode (workbuddy2api-style tool aggregation):
	//   - No tools declared -> kuku2api-style plain text (content + reasoning,
	//     no tool_calls ever).
	//   - Tools declared    -> the model declares tool_calls via the
	//     ToolForge JSON-text protocol; the gateway parses them and returns
	//     standard OpenAI tool_calls to the client for execution.  The EdgeOne
	//     sandbox is never invoked (directive forbids native tools / agent
	//     loop, and the SSE stream is cancelled as soon as the turn ends).
	//     Tool results come back as follow-up role=tool messages.
	toolsJSON := ""
	textOnly := len(req.Tools) == 0
	filter := newToolFilter(req.Tools)
	if len(req.Tools) > 0 {
		if b, err := json.Marshal(req.Tools); err == nil {
			toolsJSON = string(b)
		}
	}

	if req.Stream {
		s.streamChat(w, ctx, session, chatID, model, created, req.Messages, sessionKey, release, reacquire, toolsJSON, textOnly, filter)
	} else {
		s.nonStreamChat(w, ctx, session, chatID, model, created, req.Messages, sessionKey, release, reacquire, toolsJSON, textOnly, filter)
	}
}

func (s *Server) streamChat(w http.ResponseWriter, ctx context.Context, session *auth.Session, chatID, model string, created int64, msgs []openaiMessage, sessionKey string, release func(*auth.Session, bool), reacquire func(context.Context) (*auth.Session, error), toolsJSON string, textOnly bool, filter *toolFilter) {
	flusher, ok := w.(http.Flusher)
	if !ok {
		release(session, false)
		writeJSON(w, http.StatusInternalServerError, map[string]any{"error": "streaming unsupported"})
		return
	}

	userText := lastUserMessage(msgs)

	// Try once with the current session; if quota error, retry with a fresh one.
	// lastErr carries the final StartChat failure so the fallback 502 below
	// reports the real cause (a zombie session is *not* a quota event).
	var lastErr error
	for attempt := 0; attempt < 2; attempt++ {
		items := s.buildItems(session, sessionKey, msgs, toolsJSON)
		cs, err := session.Client.StartChat(ctx, session.SessionID, session.ConversationID, items)
		if err != nil {
			if upstream.IsQuotaError(err) || upstream.IsSessionNotFound(err) {
				// Quota / session-destroyed: rope the session and retry once with a
				// fresh one (transparent rotation instead of failed response).
				// A vanished session is upstream lifecycle, not a quota event: mark
				// it gone so it is recycled *without* burning the browser
				// fingerprint for 24h.
				if upstream.IsQuotaError(err) {
					session.MarkQuotaExceeded()
				} else {
					session.MarkGone()
				}
				release(session, false)
				session, err = reacquire(ctx)
				if err != nil {
					writeJSON(w, http.StatusServiceUnavailable, map[string]any{"error": map[string]any{"message": "no available session: " + err.Error(), "type": "server_error"}})
					return
				}
				continue // retry
			}
			lastErr = err
			release(session, false)
			log.Printf("[CHAT] start chat failed: %v", err)
			writeJSON(w, http.StatusBadGateway, map[string]any{"error": map[string]any{"message": "upstream error: " + err.Error(), "type": "upstream_error"}})
			return
		}

		// Prompt accepted — now flush the 200 and start streaming
		w.Header().Set("Content-Type", "text/event-stream")
		w.Header().Set("Cache-Control", "no-cache")
		w.Header().Set("Connection", "keep-alive")
		w.Header().Set("X-Accel-Buffering", "no")
		w.WriteHeader(http.StatusOK)
		flusher.Flush()

		// Role chunk
		emitSSE(w, chatID, model, created, map[string]any{"role": "assistant"})
		flusher.Flush()

		success := false

		defer func() {
			release(session, success)
		}()

		var sb strings.Builder
		var result upstream.ChatResult
		var streamErr error
		toolSeen := make(map[int]bool)     // tool-call index -> first (name/id) delta already sent
		toolDropped := make(map[int]bool) // tool-call index -> suppressed by name filter
		emittedToolCall := false          // any tool-call delta actually sent to the client

		// Cancel the SSE stream as soon as this turn ends.  The upstream agent
		// loop (EdgeOne sandbox tool execution) only starts after turn/end; a
		// cancelled stream cuts it off — the sandbox never runs.  This is the
		// enforcement half of the "tools run on the client, not in the sandbox"
		// design; the directive (BuildDirective) is the prevention half.
		defer cs.Cancel()

		result, streamErr = session.Client.StreamEvents(ctx, cs, func(chunk upstream.AssistantChunk) {
			if chunk.IsDone {
				return // finish chunk is emitted after StreamEvents returns
			}

			delta := map[string]any{}
			if chunk.Text != "" {
				sb.WriteString(chunk.Text)
				delta["content"] = chunk.Text
			}
			if chunk.Reasoning != "" {
				delta["reasoning_content"] = chunk.Reasoning
			}
			if chunk.ToolCall != nil && !textOnly {
				tc := chunk.ToolCall
				if toolDropped[tc.Index] {
					return // this call's name failed the declared-tool filter; suppress all its deltas
				}
				if tc.Name != "" && !toolSeen[tc.Index] {
					// First delta for this tool call: id/type/name + any argument text.
					// Validate the name against the client's declared set (alias
					// table included); unlisted names are dropped and logged instead
					// of leaking "Tool not found" errors to the client.
					name := filter.filterStreamCall(tc.Name)
					if name == "" {
						toolDropped[tc.Index] = true
						return
					}
					toolSeen[tc.Index] = true
					emittedToolCall = true
					delta["role"] = "assistant"
					delta["content"] = nil
					delta["tool_calls"] = []any{map[string]any{
						"index":    tc.Index,
						"id":       tc.ID,
						"type":     "function",
						"function": map[string]any{"name": name, "arguments": tc.ArgumentsDelta},
					}}
				} else if tc.ArgumentsDelta != "" {
					// Streaming argument fragment for an already-announced call.
					delta["tool_calls"] = []any{map[string]any{
						"index":    tc.Index,
						"function": map[string]any{"arguments": tc.ArgumentsDelta},
					}}
				} else if tc.IsComplete && tc.Arguments != "" {
					// Final block-end: full arguments (authoritative replacement).
					delta["tool_calls"] = []any{map[string]any{
						"index":    tc.Index,
						"function": map[string]any{"arguments": tc.Arguments},
					}}
				}
			}
			if len(delta) > 0 {
				emitSSE(w, chatID, model, created, delta)
				flusher.Flush()
			}
		}, textOnly, s.streamIdle)
		logTools(sessionKey, result.ToolCalls)
		finishReason := result.FinishReason
		if finishReason == "" {
			finishReason = "stop"
		}
		// If every native tool call was dropped by the declared-name filter,
		// the upstream finish reason may still say tool_calls; normalize to
		// stop so the client never sees finish_reason=tool_calls with no calls.
		if !textOnly && finishReason == "tool_calls" && !emittedToolCall {
			finishReason = "stop"
		}
		// Streaming fallback for the ToolForge JSON-text protocol: when the
		// model answered with a tool_calls JSON as plain text (no native
		// tool-call blocks), the JSON already streamed out as content.  Emit
		// the parsed tool_calls as a final delta so OpenAI clients see a
		// structured tool_calls turn and finish_reason=tool_calls, matching
		// the non-streaming aggregation.  Names are filtered against the
		// client's declared set like the native path.
		if !textOnly && finishReason != "tool_calls" {
			if calls := filter.filterCalls(parseToolCalls(sb.String())); len(calls) > 0 {
				emitToolCallsSSE(w, chatID, model, created, calls)
				flusher.Flush()
				finishReason = "tool_calls"
			}
		}
		if streamErr != nil {
			log.Printf("[STREAM] error: %v", streamErr)
			success = false
		} else if isEmptyTurn(sb.String(), result.ToolCalls) {
			// 上游返回空产出（无文本、无 tool calls、无错误）：session 已死，
			// 但 StreamEvents 不会报错。判失败让池回收该 session，避免僵尸
			// session 持续返回空响应（observed: 池里 5 个 free session 全死，
			// 每个请求都空输出，客户端无限重试）。
			log.Printf("[STREAM] empty turn: session=%s model=%s — zombie session, marking failed", session.SessionID, model)
			success = false
			session.MarkGone()
		} else {
			success = true
			if sessionKey != "" {
				s.pool.AppendHistory(sessionKey, session.SessionID, userText, sb.String())
			}
		}

		emitFinish(w, chatID, model, created, finishReason)
		flusher.Flush()
		return
	}
	// Both attempts failed with quota/session-gone errors: report the real
	// cause instead of a blanket "quota exceeded". A recycled zombie session is
	// upstream lifecycle, not a quota event, and the old wording sent people
	// hunting in the wrong direction.
	release(session, false)
	log.Printf("[CHAT] both attempts failed: %v", lastErr)
	if upstream.IsQuotaError(lastErr) {
		writeJSON(w, http.StatusBadGateway, map[string]any{"error": map[string]any{"message": "upstream quota exceeded, retry later: " + lastErr.Error(), "type": "upstream_error"}})
	} else {
		writeJSON(w, http.StatusBadGateway, map[string]any{"error": map[string]any{"message": "upstream session unavailable after rotation: " + lastErr.Error(), "type": "upstream_error"}})
	}
}

func (s *Server) nonStreamChat(w http.ResponseWriter, ctx context.Context, session *auth.Session, chatID, model string, created int64, msgs []openaiMessage, sessionKey string, release func(*auth.Session, bool), reacquire func(context.Context) (*auth.Session, error), toolsJSON string, textOnly bool, filter *toolFilter) {
	var result upstream.ChatResult
	var err error

	userText := lastUserMessage(msgs)

	for attempt := 0; attempt < 2; attempt++ {
		var cs *upstream.ChatStream
		items := s.buildItems(session, sessionKey, msgs, toolsJSON)
		cs, err = session.Client.StartChat(ctx, session.SessionID, session.ConversationID, items)
		if err != nil {
			if upstream.IsQuotaError(err) || upstream.IsSessionNotFound(err) {
				if upstream.IsQuotaError(err) {
					session.MarkQuotaExceeded()
				} else {
					session.MarkGone()
				}
				release(session, false)
				session, err = reacquire(ctx)
				if err != nil {
					writeJSON(w, http.StatusServiceUnavailable, map[string]any{"error": map[string]any{"message": "no available session: " + err.Error(), "type": "server_error"}})
					return
				}
				continue
			}
			release(session, false)
			writeJSON(w, http.StatusBadGateway, map[string]any{"error": map[string]any{"message": "upstream error: " + err.Error(), "type": "upstream_error"}})
			return
		}
		// Cancel the SSE stream as soon as the turn ends: the upstream agent
		// loop (EdgeOne sandbox execution) would only run after turn/end, and
		// a cancelled stream cuts it off.  Tools execute on the client, never
		// in the sandbox.
		defer cs.Cancel()
		result, err = session.Client.StreamEvents(ctx, cs, nil, textOnly, 0)
		logTools(sessionKey, result.ToolCalls)
		if err != nil {
			log.Printf("[CHAT] stream error: %v", err)
		}
		// 空产出 = 僵尸 session：标记失败并透明重试一次（同 session-not-found 路径）。
		if err == nil && isEmptyTurn(result.Text, result.ToolCalls) {
			log.Printf("[CHAT] empty turn: session=%s model=%s — zombie session, reacquiring", session.SessionID, model)
			session.MarkGone()
			release(session, false)
			if attempt == 0 {
				session, err = reacquire(ctx)
				if err != nil {
					writeJSON(w, http.StatusServiceUnavailable, map[string]any{"error": map[string]any{"message": "no available session: " + err.Error(), "type": "server_error"}})
					return
				}
				continue
			}
		}
		break
	}
	if err != nil {
		release(session, false)
		writeJSON(w, http.StatusBadGateway, map[string]any{"error": map[string]any{"message": "upstream error: " + err.Error(), "type": "upstream_error"}})
		return
	}

	success := true
	if isEmptyTurn(result.Text, result.ToolCalls) {
		// 同 streamChat：空产出 = 僵尸 session，判失败让池回收。
		log.Printf("[CHAT] empty turn: session=%s model=%s — zombie session, marking failed", session.SessionID, model)
		success = false
		session.MarkGone()
	} else if sessionKey != "" {
		s.pool.AppendHistory(sessionKey, session.SessionID, userText, result.Text)
	}

	msg := map[string]any{"role": "assistant", "content": result.Text}
	if result.Reasoning != "" {
		msg["reasoning_content"] = result.Reasoning
	}
	finish := "stop"
	if !textOnly {
		// workbuddy2api-style tool aggregation: prefer upstream-native
		// tool-call events (real tool-call blocks), falling back to the
		// ToolForge JSON-text protocol (the model declares a tool_calls JSON
		// as plain text).  Names are validated against the client's declared
		// tool set (with a small alias table); unlisted names are dropped and
		// logged instead of leaking "Tool not found" errors to the client.
		// The calls are returned to the client for execution; the EdgeOne
		// sandbox never runs them.
		if calls := filter.filterCalls(toOpenAIToolCalls(result.ToolCalls)); len(calls) > 0 {
			msg["content"] = nil
			msg["tool_calls"] = calls
			finish = "tool_calls"
		} else if calls := filter.filterCalls(parseToolCalls(result.Text)); len(calls) > 0 {
			msg["content"] = nil
			msg["tool_calls"] = calls
			finish = "tool_calls"
		}
	}

	resp := map[string]any{
		"id":      chatID,
		"object":  "chat.completion",
		"created": created,
		"model":   model,
		"choices": []map[string]any{
			{
				"index":         0,
				"message":       msg,
				"finish_reason": finish,
			},
		},
		"usage": map[string]any{
			"prompt_tokens":     0,
			"completion_tokens": 0,
			"total_tokens":      0,
		},
	}

	release(session, success)
	writeJSON(w, http.StatusOK, resp)
}

// --- Message conversion ---

// buildItems assembles the prompt for a session: the system directive
// (plain-text posture, or ToolForge tool-declaration protocol when the client
// declared tools), an optional cache-hit replay of the previous dialog
// history, then the request messages.  Replay is recomputed per session so a
// fresh session created by a quota retry inherits the cached context
// automatically.
func (s *Server) buildItems(session *auth.Session, sessionKey string, msgs []openaiMessage, toolsJSON string) []upstream.ContentItem {
	items := []upstream.ContentItem{{Type: "text", Text: upstream.BuildDirective(toolsJSON)}}
	if sessionKey != "" {
		if replay := s.pool.ReplayHistory(sessionKey, session.SessionID); len(replay) > 0 {
			items = append(items, upstream.ContentItem{Type: "text", Text: formatReplay(replay)})
		}
	}
	return append(items, convertMessages(msgs)...)
}

func formatReplay(turns []auth.DialogTurn) string {
	var sb strings.Builder
	sb.WriteString("[Cache Hit - Previous Conversation Context (already happened, for reference only)]\n")
	for _, t := range turns {
		if t.Role == "assistant" {
			sb.WriteString("assistant: " + t.Text + "\n")
		} else {
			sb.WriteString("user: " + t.Text + "\n")
		}
	}
	sb.WriteString("[End of Previous Context - answer the latest user message only]\n")
	return sb.String()
}

func convertMessages(msgs []openaiMessage) []upstream.ContentItem {
	var items []upstream.ContentItem

	for _, msg := range msgs {
		switch msg.Role {
		case "system":
			text := extractContent(msg.Content)
			if text != "" {
				items = append(items, upstream.ContentItem{Type: "text", Text: "[System] " + text})
			}
		case "user":
			text := extractContent(msg.Content)
			if text != "" {
				items = append(items, upstream.ContentItem{Type: "text", Text: text})
			}
		case "assistant":
			text := extractContent(msg.Content)
			if text != "" {
				items = append(items, upstream.ContentItem{Type: "text", Text: text})
			}
			// Preserve a previous tool_calls turn as fenced text so the
			// upstream model sees which tool-call was issued (with its id),
			// enabling it to match a later tool result and continue.
			if len(msg.ToolCalls) > 0 {
				items = append(items, upstream.ContentItem{Type: "text", Text: formatAssistantToolCalls(msg.ToolCalls)})
			}
		case "tool":
			text := extractContent(msg.Content)
			if text == "" {
				continue
			}
			// Tool result echoed by the client.  The upstream session.prompt
			// only accepts plain {type,text} content items — it rejects
			// structured "tool" items — so we fold the result into a labeled
			// text turn, keeping tool_call_id so the upstream agent can
			// correlate it with the assistant's earlier tool call and continue.
			label := "[Tool Result]"
			if msg.ToolCallID != "" {
				label += " (tool_call_id=" + msg.ToolCallID + ")"
			}
			items = append(items, upstream.ContentItem{Type: "text", Text: label + " " + text})
		}
	}
	return items
}

// toOpenAIToolCalls converts upstream-native assistant tool calls into the
// OpenAI tool_calls message shape returned to the client.  Names pass
// through untouched (workbuddy2api style — no translation layer).
func toOpenAIToolCalls(tcs []upstream.AssistantToolCall) []openaiToolCall {
	if len(tcs) == 0 {
		return nil
	}
	calls := make([]openaiToolCall, 0, len(tcs))
	for i, tc := range tcs {
		id := tc.ID
		if id == "" {
			id = fmt.Sprintf("call_%s_%d", randHex(6), i)
		}
		calls = append(calls, openaiToolCall{
			ID:   id,
			Type: "function",
			Function: openaiToolCallFn{
				Name:      tc.Name,
				Arguments: tc.Arguments,
			},
		})
	}
	return calls
}

func extractContent(raw json.RawMessage) string {
	if len(raw) == 0 || string(raw) == "null" {
		return ""
	}
	var s string
	if err := json.Unmarshal(raw, &s); err == nil {
		return s
	}
	var parts []struct {
		Type string `json:"type"`
		Text string `json:"text"`
	}
	if err := json.Unmarshal(raw, &parts); err == nil {
		var sb strings.Builder
		for _, p := range parts {
			if p.Type == "text" && p.Text != "" {
				sb.WriteString(p.Text)
			}
		}
		return sb.String()
	}
	return string(bytes.Trim(raw, "\""))
}

func lastUserMessage(msgs []openaiMessage) string {
	for i := len(msgs) - 1; i >= 0; i-- {
		if msgs[i].Role == "user" {
			if t := extractContent(msgs[i].Content); t != "" {
				return t
			}
		}
	}
	return ""
}

// isEmptyTurn reports whether the upstream produced absolutely nothing
// (no text, no reasoning, no tool calls) without returning an error. This is
// the signature of a zombie session: the harness accepted the request but the
// session is dead server-side, so the SSE stream completes with zero content.
// StreamEvents does not error in this case, so without this check the session
// is released as "success" and stays in the pool forever, returning empty to
// every subsequent request.
func isEmptyTurn(text string, calls []upstream.AssistantToolCall) bool {
	return text == "" && len(calls) == 0
}

// logTools records any tool calls the upstream model actually emitted for a
// request.  The expected posture is zero native tool-call events: the
// directive makes the model declare tool calls as JSON text in the first
// turn instead of entering the platform agent loop.
func logTools(sessionKey string, calls []upstream.AssistantToolCall) {
	if len(calls) == 0 {
		log.Printf("[TOOLS] key=%s tool_calls=none", sessionKey)
		return
	}
	for _, tc := range calls {
		args := tc.Arguments
		if len(args) > 160 {
			args = args[:160] + "..."
		}
		log.Printf("[TOOLS] key=%s tool_call name=%s args=%s", sessionKey, tc.Name, args)
	}
}

// formatAssistantToolCalls renders a previous assistant tool_calls turn as a
// stable text fragment (id + name + arguments) that the upstream model can
// parse and correlate with the subsequent tool result.
func formatAssistantToolCalls(calls []openaiToolCall) string {
	var sb strings.Builder
	sb.WriteString("[Assistant Tool Calls - issued earlier]\n")
	for _, c := range calls {
		sb.WriteString("  id=" + c.ID + " name=" + c.Function.Name)
		if c.Function.Arguments != "" {
			sb.WriteString(" arguments=" + c.Function.Arguments)
		}
		sb.WriteString("\n")
	}
	return sb.String()
}

// parseToolCalls tries to interpret the model's text output as a tool_calls
// JSON (the ToolForge declared-tool protocol).  It tolerates a surrounding
// code fence.  Returns nil when the text is not a tool_calls JSON, meaning
// the model answered directly.
func parseToolCalls(text string) []openaiToolCall {
	t := strings.TrimSpace(text)
	t = strings.TrimPrefix(t, "```json")
	t = strings.TrimPrefix(t, "```")
	t = strings.TrimSuffix(t, "```")
	t = strings.TrimSpace(t)
	var parsed struct {
		ToolCalls []struct {
			ID       string `json:"id"`
			Type     string `json:"type"`
			Function struct {
				Name      string `json:"name"`
				Arguments string `json:"arguments"`
			} `json:"function"`
		} `json:"tool_calls"`
	}
	if err := json.Unmarshal([]byte(t), &parsed); err != nil {
		return nil
	}
	if len(parsed.ToolCalls) == 0 {
		return nil
	}
	calls := make([]openaiToolCall, 0, len(parsed.ToolCalls))
	for i, c := range parsed.ToolCalls {
		typ := c.Type
		if typ == "" {
			typ = "function"
		}
		id := c.ID
		if id == "" {
			id = fmt.Sprintf("call_%s_%d", randHex(6), i)
		}
		calls = append(calls, openaiToolCall{
			ID:   id,
			Type: typ,
			Function: openaiToolCallFn{
				Name:      c.Function.Name,
				Arguments: c.Function.Arguments,
			},
		})
	}
	return calls
}

// --- SSE helpers ---

func emitSSE(w http.ResponseWriter, chatID, model string, created int64, delta map[string]any) {
	c := map[string]any{
		"index":         0,
		"delta":         map[string]any{},
		"finish_reason": nil,
	}
	if delta != nil {
		c["delta"] = delta
	}

	out := map[string]any{
		"id":      chatID,
		"model":   model,
		"created": created,
		"object":  "chat.completion.chunk",
		"choices": []any{c},
	}
	d, _ := json.Marshal(out)
	w.Write([]byte("data: " + string(d) + "\n\n"))
}

// emitToolCallsSSE sends the complete tool_calls payload as a streaming chunk.
// Used by the streaming fallback when the model declared tool_calls via the
// ToolForge JSON-text protocol instead of native tool-call blocks.
func emitToolCallsSSE(w http.ResponseWriter, chatID, model string, created int64, calls []openaiToolCall) {
	delta := map[string]any{
		"role":       "assistant",
		"content":    nil,
		"tool_calls": calls,
	}
	out := map[string]any{
		"id":      chatID,
		"model":   model,
		"created": created,
		"object":  "chat.completion.chunk",
		"choices": []any{map[string]any{"index": 0, "delta": delta, "finish_reason": "tool_calls"}},
	}
	d, _ := json.Marshal(out)
	w.Write([]byte("data: " + string(d) + "\n\n"))
}

func emitFinish(w http.ResponseWriter, chatID, model string, created int64, finish string) {
	out := map[string]any{
		"id":      chatID,
		"model":   model,
		"created": created,
		"object":  "chat.completion.chunk",
		"choices": []any{map[string]any{"index": 0, "delta": map[string]any{}, "finish_reason": finish}},
	}
	d, _ := json.Marshal(out)
	w.Write([]byte("data: " + string(d) + "\n\n"))
	w.Write([]byte("data: [DONE]\n\n"))
}

// --- Health / Pool ---

func (s *Server) handleHealth(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]any{"status": "ok", "poolSize": s.pool.Count()})
}

func (s *Server) handlePool(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]any{"pool": s.pool.Stats()})
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	if err := json.NewEncoder(w).Encode(v); err != nil {
		log.Printf("write json: %v", err)
	}
}

func randHex(n int) string {
	b := make([]byte, (n+1)/2)
	if _, err := rand.Read(b); err != nil {
		b = []byte(time.Now().String())
	}
	return hex.EncodeToString(b)[:n]
}
