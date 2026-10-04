package config

import (
	"encoding/json"
	"os"
	"strconv"
)

type Config struct {
	Listen           string                  `json:"listen"`
	APIKey           string                  `json:"api_key"`
	Models           []string                `json:"models"`
	PoolMin          int                     `json:"pool_min"`
	PoolMax          int                     `json:"pool_max"`
	TTLMin           int                     `json:"ttl_minutes"`
	FreeTTLMin       int                     `json:"free_ttl_minutes"`
	BindTTLMin       int                     `json:"bind_ttl_minutes"`
	MaxReqPerSession int                     `json:"max_req_per_session"`
	UpstreamURL      string                  `json:"upstream_url"`
	AgentPreset      string                  `json:"agent_preset"`
	RequestJitterMs  int                     `json:"request_jitter_ms"`  // pseudo-concurrency: random delay [0,N)ms before sending to upstream
	MaxConcurrent    int                     `json:"max_concurrent"`      // cap of in-flight upstream requests (0 = unlimited)
	RequestTimeout   int                     `json:"request_timeout_seconds"` // non-streaming overall timeout (streaming has no total deadline)
	StreamIdle       int                     `json:"stream_idle_seconds"`     // streaming: break when no upstream event for N seconds (0 = disabled)
	DefaultReasoningEffort string            `json:"default_reasoning_effort"` // global fallback reasoning strength ("off"/"high"/"max"/"") when client & model_map don't specify
	ModelMap         map[string]ModelMapping `json:"model_map"`
}

// ModelMapping maps a client-facing model name to the upstream provider/model
// and optional reasoning effort ("off", "high", "max").
type ModelMapping struct {
	Provider        string `json:"provider"`
	Model           string `json:"model"`
	ReasoningEffort string `json:"reasoning_effort"`
}

// defaultModelMap covers models discovered via session.models on the minimal
// preset.  Only the edgeone-makers provider is usable without a EDGEONE_API_KEY;
// deepseek-official models require the credentials service and are excluded.
func defaultModelMap() map[string]ModelMapping {
	return map[string]ModelMapping{
		"@makers/hy3":               {Provider: "edgeone-makers", Model: "@makers/hy3"},
		"@makers/hy3-preview":       {Provider: "edgeone-makers", Model: "@makers/hy3-preview"},
		"@makers/deepseek-v4-pro":   {Provider: "edgeone-makers", Model: "@makers/deepseek-v4-pro"},
		"@makers/deepseek-v4-flash": {Provider: "edgeone-makers", Model: "@makers/deepseek-v4-flash"},
		"@makers/minimax-m3":        {Provider: "edgeone-makers", Model: "@makers/minimax-m3"},
		"@makers/minimax-m2.7":      {Provider: "edgeone-makers", Model: "@makers/minimax-m2.7"},
		"@makers/kimi-k2.6":         {Provider: "edgeone-makers", Model: "@makers/kimi-k2.6"},
	}
}

func defaultConfig() Config {
	return Config{
		Listen:           ":7863",
		APIKey:           "",
		Models:           []string{"@makers/deepseek-v4-flash", "@makers/deepseek-v4-pro"},
		PoolMin:          4,
		PoolMax:          32,
		TTLMin:           0,  // 0 = bound/free 会话无统一生命周期上限（free 会话另由 FreeTTL 约束）
		FreeTTLMin:       90, // free 会话年龄上限：上游 session 约 1.5-2h 后会被销毁，free 会话超过此时长即回收重建，避免僵尸会话导致 session not found
		BindTTLMin:       30,
		MaxReqPerSession: 200,
		UpstreamURL:      "https://deepseek-harness.edgeone.cool",
		AgentPreset:      "minimal",
		RequestJitterMs:  0,
		MaxConcurrent:    0,
		RequestTimeout:   600, // non-streaming: 10min overall budget
		StreamIdle:       180, // streaming: 3min without any upstream event = dead
		DefaultReasoningEffort: "off", // default: no reasoning, so the full 16k completion budget goes to content
		ModelMap:         defaultModelMap(),
	}
}

func Load(path string) (Config, error) {
	cfg := defaultConfig()
	if path != "" {
		data, err := os.ReadFile(path)
		if err != nil {
			return cfg, err
		}
		if err := json.Unmarshal(data, &cfg); err != nil {
			return cfg, err
		}
	}
	// Environment variable overrides
	if v := os.Getenv("EDGEONE_API_LISTEN"); v != "" {
		cfg.Listen = v
	} else if v := os.Getenv("PORT"); v != "" {
		// PaaS platforms (Zeabur / Railway / Fly / Heroku) inject the port to
		// bind and route traffic to, and never expand a value we set ourselves,
		// so honour $PORT directly.  ":N" binds every interface, which is what
		// those platforms probe.
		cfg.Listen = ":" + v
	}
	if v := os.Getenv("EDGEONE_API_KEY"); v != "" {
		cfg.APIKey = v
	}
	if v := os.Getenv("EDGEONE_API_MODELS"); v != "" {
		cfg.Models = splitAndTrim(v, ",")
	}
	if v := os.Getenv("EDGEONE_API_POOL_MIN"); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			cfg.PoolMin = n
		}
	}
	if v := os.Getenv("EDGEONE_API_POOL_MAX"); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			cfg.PoolMax = n
		}
	}
	if v := os.Getenv("EDGEONE_API_TTL_MINUTES"); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			cfg.TTLMin = n
		}
	}
	if v := os.Getenv("EDGEONE_API_BIND_TTL_MINUTES"); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			cfg.BindTTLMin = n
		}
	}
	if v := os.Getenv("EDGEONE_API_MAX_REQ_PER_SESSION"); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			cfg.MaxReqPerSession = n
		}
	}
	if v := os.Getenv("EDGEONE_API_UPSTREAM"); v != "" {
		cfg.UpstreamURL = v
	}
	if v := os.Getenv("EDGEONE_API_JITTER_MS"); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			cfg.RequestJitterMs = n
		}
	}
	if v := os.Getenv("EDGEONE_API_MAX_CONCURRENT"); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			cfg.MaxConcurrent = n
		}
	}
	if v := os.Getenv("EDGEONE_API_REQUEST_TIMEOUT"); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			cfg.RequestTimeout = n
		}
	}
	if v := os.Getenv("EDGEONE_API_STREAM_IDLE"); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			cfg.StreamIdle = n
		}
	}
	if v := os.Getenv("EDGEONE_API_DEFAULT_REASONING_EFFORT"); v != "" {
		cfg.DefaultReasoningEffort = v
	}
	return cfg, nil
}

func splitAndTrim(s, sep string) []string {
	if s == "" {
		return nil
	}
	result := []string{}
	start := 0
	for i := 0; i < len(s); i++ {
		if string(s[i]) == sep {
			if start < i {
				result = append(result, s[start:i])
			}
			start = i + 1
		}
	}
	if start < len(s) {
		result = append(result, s[start:])
	}
	for i := range result {
		result[i] = trim(result[i])
	}
	return result
}

func trim(s string) string {
	start := 0
	end := len(s)
	for start < end && (s[start] == ' ' || s[start] == '\t') {
		start++
	}
	for end > start && (s[end-1] == ' ' || s[end-1] == '\t') {
		end--
	}
	return s[start:end]
}
