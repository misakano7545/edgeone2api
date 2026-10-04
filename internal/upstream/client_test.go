package upstream

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
)

func env(eventType string, data map[string]any) SSEEnvelope {
	se := map[string]any{
		"sessionId": "s1",
		"event": map[string]any{
			"type": eventType,
			"seq":  0,
			"time": 0,
			"data": data,
		},
	}
	raw, _ := json.Marshal(se)
	return SSEEnvelope{Type: "server-request", RpcID: "rpc-1", Method: "session/event", Payload: raw}
}

func chunkEnv(turn int, chunk map[string]any) SSEEnvelope {
	return env("assistant/chunk", map[string]any{
		"turn":  turn,
		"step":  0,
		"chunk": chunk,
	})
}

func runEvents(t *testing.T, chunks []SSEEnvelope, textOnly bool) ChatResult {
	t.Helper()
	envCh := make(chan SSEEnvelope, 32)
	for _, e := range chunks {
		envCh <- e
	}
	close(envCh)
	c := &Client{}
	cs := &ChatStream{envCh: envCh}
	res, err := c.StreamEvents(context.Background(), cs, nil, textOnly, 0)
	if err != nil {
		t.Fatalf("StreamEvents: %v", err)
	}
	return res
}

func TestStreamEventsTextOnlyFoldsToolCallsAcrossTurns(t *testing.T) {
	chunks := []SSEEnvelope{
		env("turn/start", map[string]any{"turn": 1}),
		chunkEnv(1, map[string]any{"type": "text-delta", "text": "Let me check."}),
		chunkEnv(1, map[string]any{"type": "block-start", "blockType": "tool-call"}),
		chunkEnv(1, map[string]any{"type": "tool-call-delta", "index": 0, "id": "t_1", "name": "read", "argumentsDelta": `{"path":"/x"}`}),
		chunkEnv(1, map[string]any{"type": "finish", "reason": map[string]any{"kind": "tool-calls"}}),
		env("turn/end", map[string]any{"turn": 1}),
		env("turn/start", map[string]any{"turn": 2}),
		chunkEnv(2, map[string]any{"type": "text-delta", "text": "Final answer."}),
		env("turn/end", map[string]any{"turn": 2}),
	}
	res := runEvents(t, chunks, true)
	if res.Text != "Let me check.Final answer." {
		t.Fatalf("textOnly must keep consuming follow-up turns, got %q", res.Text)
	}
	if res.FinishReason != "stop" {
		t.Fatalf("textOnly must normalize finish reason to stop, got %q", res.FinishReason)
	}
	if len(res.ToolCalls) != 0 {
		t.Fatalf("textOnly must fold tool calls, got %+v", res.ToolCalls)
	}
}

func TestStreamEventsTextOnlyCleanTurnStopsImmediately(t *testing.T) {
	chunks := []SSEEnvelope{
		env("turn/start", map[string]any{"turn": 1}),
		chunkEnv(1, map[string]any{"type": "text-delta", "text": "Direct answer"}),
		env("turn/end", map[string]any{"turn": 1}),
		env("turn/start", map[string]any{"turn": 2}),
		chunkEnv(2, map[string]any{"type": "text-delta", "text": "should-not-appear"}),
		env("turn/end", map[string]any{"turn": 2}),
	}
	res := runEvents(t, chunks, true)
	if res.Text != "Direct answer" {
		t.Fatalf("a clean turn must return at turn/end, got %q", res.Text)
	}
}

func TestStreamEventsDefaultCapturesToolCalls(t *testing.T) {
	chunks := []SSEEnvelope{
		env("turn/start", map[string]any{"turn": 1}),
		chunkEnv(1, map[string]any{"type": "text-delta", "text": "Let me check."}),
		chunkEnv(1, map[string]any{"type": "block-start", "blockType": "tool-call"}),
		chunkEnv(1, map[string]any{"type": "tool-call-delta", "index": 0, "id": "t_1", "name": "read", "argumentsDelta": `{"path":"/x"}`}),
		chunkEnv(1, map[string]any{"type": "finish", "reason": map[string]any{"kind": "tool-calls"}}),
		env("turn/end", map[string]any{"turn": 1}),
	}
	res := runEvents(t, chunks, false)
	if res.FinishReason != "tool_calls" {
		t.Fatalf("default mode must keep tool_calls finish, got %q", res.FinishReason)
	}
	if len(res.ToolCalls) != 1 || res.ToolCalls[0].Name != "read" || res.ToolCalls[0].Arguments != `{"path":"/x"}` {
		t.Fatalf("expected one read tool call, got %+v", res.ToolCalls)
	}
	if res.Text != "Let me check." {
		t.Fatalf("text must be preserved, got %q", res.Text)
	}
}

func TestBuildDirectiveNoToolsSuppressesAgentLoop(t *testing.T) {
	d := BuildDirective("")
	for _, want := range []string{"NEVER emit tool calls", "single turn only", `finish_reason "tool_calls"`} {
		if !strings.Contains(d, want) {
			t.Fatalf("directive missing %q:\n%s", want, d)
		}
	}
}

func TestBuildDirectiveWithToolsKeepsProtocol(t *testing.T) {
	d := BuildDirective(`[{"type":"function","function":{"name":"read_file","description":"read a file","parameters":{"type":"object","properties":{"path":{"type":"string"}},"required":["path"]}}}]`)
	if !strings.Contains(d, "read_file") {
		t.Fatalf("tools directive must list declared tool names:\n%s", d)
	}
	if strings.Contains(d, "NEVER emit tool calls") {
		t.Fatalf("tools directive must enable the tool protocol, not forbid it:\n%s", d)
	}
}

// The retry-without-effort path in the server hinges on recognising this exact
// upstream rejection.  Pin it to the real message so a reword upstream surfaces
// here instead of silently reintroducing "ask for kimi, get deepseek".
func TestIsUnsupportedReasoningEffort(t *testing.T) {
	cases := []struct {
		name string
		err  error
		want bool
	}{
		{"real upstream rejection",
			errors.New(`session.selectModel failed: provider "edgeone-makers" model "@makers/kimi-k2.6" does not support reasoning effort "off"`), true},
		{"nil", nil, false},
		{"session gone (must not be swallowed here)",
			errors.New(`session.selectModel failed: session "session-1" not found`), false},
		{"transient network error",
			errors.New(`session.selectModel: status 502`), false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := IsUnsupportedReasoningEffort(c.err); got != c.want {
				t.Errorf("IsUnsupportedReasoningEffort(%v) = %v, want %v", c.err, got, c.want)
			}
		})
	}
}
