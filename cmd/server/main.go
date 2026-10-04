package main

import (
	"flag"
	"log"
	"net/http"
	"time"

	"edgeone2api/internal/auth"
	"edgeone2api/internal/config"
	"edgeone2api/internal/server"
)

func main() {
	configPath := flag.String("config", "", "config file path (JSON)")
	flag.Parse()

	cfg, err := config.Load(*configPath)
	if err != nil {
		log.Fatalf("load config: %v", err)
	}

	log.Printf("edgeone2api starting: listen=%s models=%v pool=[%d,%d] ttl=%dm bind_ttl=%dm max_req=%d upstream=%s agent_preset=%s",
		cfg.Listen, cfg.Models, cfg.PoolMin, cfg.PoolMax, cfg.TTLMin, cfg.BindTTLMin, cfg.MaxReqPerSession, cfg.UpstreamURL, cfg.AgentPreset)

	if cfg.APIKey == "" {
		log.Printf("WARNING: api_key is empty — this service is UNPROTECTED. Anyone who can reach %s consumes the upstream. Set EDGEONE_API_KEY before exposing it on a public host.", cfg.Listen)
	}

	poolCfg := auth.PoolConfig{
		MinSize:          cfg.PoolMin,
		MaxSize:          cfg.PoolMax,
		TTL:              time.Duration(cfg.TTLMin) * time.Minute,
		FreeTTL:          time.Duration(cfg.FreeTTLMin) * time.Minute,
		BindTTL:          time.Duration(cfg.BindTTLMin) * time.Minute,
		MaxReqPerSession: cfg.MaxReqPerSession,
		UpstreamURL:      cfg.UpstreamURL,
		AgentPreset:      cfg.AgentPreset,
	}
	pool := auth.NewPool(poolCfg)
	defer pool.Close()

	// Wait for initial sessions
	time.Sleep(2 * time.Second)
	log.Printf("session pool ready: %d session(s)", pool.Count())

	srv := server.New(pool, cfg.APIKey, cfg.Models, time.Duration(cfg.RequestTimeout)*time.Second, time.Duration(cfg.StreamIdle)*time.Second, cfg.ModelMap, cfg.DefaultReasoningEffort, cfg.RequestJitterMs, cfg.MaxConcurrent)
	httpServer := &http.Server{
		Addr:              cfg.Listen,
		Handler:           srv.Handler(),
		ReadHeaderTimeout: 10 * time.Second,
	}

	log.Printf("listening on %s", cfg.Listen)
	if err := httpServer.ListenAndServe(); err != nil {
		log.Fatalf("server: %v", err)
	}
}
