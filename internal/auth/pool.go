package auth

import (
	"context"
	"fmt"
	"log"
	"math"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"edgeone2api/internal/upstream"
)

// DialogTurn is one cached message in the per-key dialog history used to
// rebuild context after session rotation (cache-hit replay).
type DialogTurn struct {
	Role string // "user" | "assistant"
	Text string
}

const (
	fingerprintCooldown = 24 * time.Hour // how long an exhausted fingerprint stays blacklisted
	dialogCacheTTL      = 24 * time.Hour // idle time before a key's dialog cache is dropped
	maxReplayTurns      = 10             // max turns replayed into a fresh session
	maxCacheTurns       = 20             // max turns kept per key before trimming oldest
)

// Session represents a DeepSeek Harness session
type Session struct {
	ConversationID string
	SessionID      string
	CreatedAt      time.Time
	LastUsed       time.Time
	FailedCount    int
	ReqCount       int // total requests served by this session (tool-less turns)
	Client         *upstream.Client

	mu            sync.Mutex  // locks the session for exclusive use
	active        atomic.Bool // set under mu by Lock; read atomically by Unlock/Stats
	bound         bool        // true if this session is bound to a key (guarded by mu)
	quotaExceeded bool        // set by MarkQuotaExceeded; fingerprint exhaustion is recorded on release

	// SelectedModel caches the last selectModel payload so repeated requests
	// with the same model skip the redundant RPC (guarded by mu).
	SelectedModel string
}

// Lock locks the session for exclusive use
func (s *Session) Lock() {
	s.mu.Lock()
	s.active.Store(true)
	s.LastUsed = time.Now()
}

// tryLock acquires the session for exclusive use without blocking, marking it
// active so the matching Unlock/Release can clear the flag.  Every checkout
// path (Acquire/Bind) must go through here: Unlock() gates on the active
// atomic, so acquiring with a bare s.mu.TryLock() would leave the flag false
// and the mutex permanently leaked on release.  Maintenance probes that only
// peek at availability keep using s.mu.TryLock() directly and pair it with a
// raw s.mu.Unlock().
func (s *Session) tryLock() bool {
	if !s.mu.TryLock() {
		return false
	}
	s.active.Store(true)
	s.LastUsed = time.Now()
	return true
}

// Unlock unlocks the session.  Idempotent: calling Unlock on an already
// unlocked session is a no-op.  This guards against double-release when a
// request's release closure is invoked more than once (e.g. an internal
// error path plus the deferred release), which previously crashed with
// "sync: unlock of unlocked mutex".
//
// The caller is expected to hold s.mu (from Lock/Bind/Acquire), so the
// idempotency check uses an atomic flag instead of re-locking the mutex —
// re-locking here would self-deadlock on the held mutex.
func (s *Session) Unlock() {
	if !s.active.CompareAndSwap(true, false) {
		return // already released
	}
	s.mu.Unlock()
}

// MarkQuotaExceeded flags the session for immediate recycling on the next
// Release/ReleaseBind call.  Call before releasing when a quota/rate-limit
// error is detected.  Caller must hold the session lock (from Bind/Acquire).
func (s *Session) MarkQuotaExceeded() {
	s.ReqCount = 999999
	s.quotaExceeded = true
}

// MarkGone flags the session for immediate recycling on the next
// Release/ReleaseBind call *without* exhausting its browser fingerprint.
//
// Used when the upstream reports the session no longer exists.  The harness
// reaps idle sessions server-side well before our own FreeTTL window, so a
// plain "session not found" is normal lifecycle, not a quota event: burning
// the fingerprint for 24h over it would slowly poison the pool with cooling
// entries for sessions that never hit any limit.
func (s *Session) MarkGone() {
	s.ReqCount = 999999
	s.quotaExceeded = false
}

// PoolConfig configures the session pool
type PoolConfig struct {
	MinSize          int
	MaxSize          int
	TTL              time.Duration // session max lifetime
	FreeTTL          time.Duration // max age for free (unbound/anonymous) sessions, independent of TTL
	BindTTL          time.Duration // idle bound-session lifetime
	MaxReqPerSession int           // max requests before session is recycled (0 = unlimited)
	UpstreamURL      string
	AgentPreset      string // "makers" (default), "minimal", "standard", "code", "cordis"
}

// boundEntry is a session bound to a client key
type boundEntry struct {
	session  *Session
	lastUsed time.Time
}

// Pool manages a pool of sessions shared across clients, plus
// per-key bound sessions for conversation continuity.
type Pool struct {
	mu     sync.RWMutex           // guards free/binds
	free   []*Session             // sessions available for generic assignment
	binds  map[string]*boundEntry // key → bound session
	config PoolConfig

	cacheMu      sync.Mutex
	cache        map[string][]DialogTurn // key → dialog history (cache-hit source)
	lastSrv      map[string]string       // key → sessionID that most recently served it
	cacheLast    map[string]time.Time    // key → last activity, for cache expiry
	replays      int64                   // total cache-hit replays delivered
	replayHits   int64                   // total dialog-cache hits (key had history)
	replayMisses int64                   // total dialog-cache misses (key had no history)

	fpMu        sync.Mutex
	exhaustedFP map[string]time.Time // fingerprint sig → exhausted-at (24h cooldown)

	warmingMu   sync.Mutex
	warming     int   // number of background session creations currently in flight
	lastWarmErr error // last background creation error (consumed by waiters)

	// maxConcurrentWarm caps how many background session creations may run in
	// parallel.  Each creation draws a fresh random fingerprint and performs a
	// slow upstream RPC, so running too many at once risks tripping the
	// upstream's per-fingerprint rate limits.  4 balances burst-scaling speed
	// against upstream tolerance.
	maxConcurrentWarm int
}

// NewPool creates a new session pool and eagerly creates min sessions
func NewPool(cfg PoolConfig) *Pool {
	if cfg.BindTTL == 0 {
		cfg.BindTTL = 30 * time.Minute
	}
	// FreeTTL caps the age of free (anonymous) sessions so upstream-destroyed
	// zombie sessions are rotated out even when TTL is 0.  Default 90m matches
	// the observed upstream session lifetime (~1.5-2h).  0 explicitly disables it.
	if cfg.FreeTTL == 0 {
		cfg.FreeTTL = 90 * time.Minute
	}
	// TTL 0 means no max lifetime for bound sessions — they are only recycled
	// on failure or when MaxReqPerSession is exceeded.  Free sessions are
	// independently bounded by FreeTTL above.
	if cfg.MaxReqPerSession == 0 {
		cfg.MaxReqPerSession = 200
	}
	pool := &Pool{
		config:            cfg,
		binds:             make(map[string]*boundEntry),
		cache:             make(map[string][]DialogTurn),
		lastSrv:           make(map[string]string),
		cacheLast:         make(map[string]time.Time),
		exhaustedFP:       make(map[string]time.Time),
		maxConcurrentWarm: 4,
	}
	// Warm the pool in the background.  Each session costs a full RPC
	// round-trip (CreateSession → InitSession → first model reply, ~20-44s),
	// so blocking here would hold the listener unbound past the ~30s window
	// PaaS platforms (Zeabur-style TCP probes) allow.
	//
	// warm() is the shared creation path — maintain() and Acquire() use it
	// too — and it self-caps at maxConcurrentWarm and appends under p.mu, so
	// this needs no semaphore of its own and cannot race a second creator
	// against the shared cap.  Calls are staggered because a burst of
	// simultaneous session.create trips the upstream rate limiter.
	go func() {
		for i := 0; i < cfg.MinSize; i++ {
			pool.warm()
			if i < cfg.MinSize-1 {
				time.Sleep(time.Duration(rand.Intn(5000)) * time.Millisecond)
			}
		}
	}()
	// Start maintenance goroutine
	go pool.maintain()
	return pool
}

func (p *Pool) createSession() (*Session, error) {
	for attempt := 0; attempt < 5; attempt++ {
		// Each session gets its own client with a random browser fingerprint,
		// so upstream sees each session as a separate browser.  This bounds the
		// damage of rate limiting to one session instead of the whole pool.
		client := upstream.NewClient(p.config.UpstreamURL)
		sig := client.FingerprintSignature()
		if p.fingerprintCooling(sig) {
			continue // this fingerprint is in the 24h cooldown — draw another
		}

		ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
		convID, sessID, err := client.CreateSession(ctx, p.config.AgentPreset)
		if err != nil {
			cancel()
			if upstream.IsQuotaError(err) {
				p.exhaustFingerprint(sig)
				continue
			}
			return nil, err
		}
		if err := client.InitSession(ctx, sessID, convID); err != nil {
			cancel()
			if upstream.IsQuotaError(err) {
				p.exhaustFingerprint(sig)
				continue
			}
			return nil, fmt.Errorf("init session: %w", err)
		}
		cancel()
		return &Session{
			ConversationID: convID,
			SessionID:      sessID,
			CreatedAt:      time.Now(),
			LastUsed:       time.Now(),
			FailedCount:    0,
			Client:         client,
		}, nil
	}
	return nil, fmt.Errorf("no fresh fingerprint available: all fingerprints exhausted or cooling down")
}

// warm starts one background session creation when the pool is under pressure
// (all free sessions busy), so concurrent waiters share in-flight creates
// instead of each blocking on a slow serialized RPC.  Up to
// maxConcurrentWarm creations run in parallel, so a burst of waiters scales
// the pool up quickly instead of one session per RPC round-trip.  Freshly
// created sessions are appended to the free list; waiters pick them up on
// their next poll.  warm is a no-op while the in-flight cap is reached.
func (p *Pool) warm() {
	p.warmingMu.Lock()
	if p.warming >= p.maxConcurrentWarm {
		p.warmingMu.Unlock()
		return
	}
	p.warming++
	p.lastWarmErr = nil
	p.warmingMu.Unlock()

	go func() {
		sess, err := p.createSession()
		p.warmingMu.Lock()
		p.warming--
		if err != nil {
			p.lastWarmErr = err
		}
		p.warmingMu.Unlock()
		if err != nil {
			log.Printf("pool: background warm create failed: %v", err)
			return
		}
		p.mu.Lock()
		if p.totalLocked() >= p.config.MaxSize {
			p.mu.Unlock()
			return // pool already at capacity; drop the extra session
		}
		p.free = append(p.free, sess)
		p.mu.Unlock()
		log.Printf("pool: background warm created session %s", sess.SessionID)
	}()
}

// warmError returns the most recent background creation error, if any.
func (p *Pool) warmError() error {
	p.warmingMu.Lock()
	defer p.warmingMu.Unlock()
	return p.lastWarmErr
}

// freeStale reports whether a free-list candidate is too old to lend out.
// Applies the explicit TTL when set, and always the FreeTTL (age cap for free
// sessions) so upstream-destroyed zombies are rotated even when TTL is 0.
func (p *Pool) freeStale(now time.Time, s *Session) bool {
	if p.config.FreeTTL > 0 && now.Sub(s.CreatedAt) > p.config.FreeTTL {
		return true
	}
	if p.config.TTL > 0 && now.Sub(s.CreatedAt) > p.config.TTL {
		return true
	}
	return false
}

// Acquire gets an available free session, locking it for exclusive use.
// Caller must call Release() when done.
// When all free sessions are busy, a background session creation is started
// (shared across waiters) instead of blocking each request on a slow
// serialized RPC; waiters poll until a session becomes available, the
// background creation fails, or the context is done.
func (p *Pool) Acquire(ctx context.Context) (*Session, error) {
	for {
		p.mu.Lock()
		now := time.Now()
		for _, s := range p.free {
			if p.freeStale(now, s) {
				continue // expired
			}
			if s.tryLock() {
				p.mu.Unlock()
				return s, nil
			}
		}
		canCreate := p.totalLocked() < p.config.MaxSize
		p.mu.Unlock()
		if canCreate {
			p.warm()
			if err := p.warmError(); err != nil {
				// Background creation failed.  Only surface it when no
				// session is available at all and no other creation is still
				// in flight (a just-released session or another warm may
				// still satisfy this request on the next poll).
				p.warmingMu.Lock()
				inFlight := p.warming
				p.warmingMu.Unlock()
				p.mu.Lock()
				hasAny := len(p.free) > 0
				p.mu.Unlock()
				if !hasAny && inFlight == 0 {
					return nil, fmt.Errorf("create session: %w", err)
				}
			}
		}

		// All sessions busy — wait briefly then retry
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(300 * time.Millisecond):
		}
	}
}

// Bind returns the session locked for key, creating or reusing the binding.
// Caller must call ReleaseBind(key, session, ok) when done.
// If the bound session is busy, waits for it (up to ctx deadline).
func (p *Pool) Bind(ctx context.Context, key string) (*Session, error) {
	for {
		p.mu.Lock()

		if be, ok := p.binds[key]; ok {
			s := be.session
			if s.tryLock() {
				be.lastUsed = time.Now()
				p.mu.Unlock()
				return s, nil
			}
			// Bound session is busy — wait for it, then retry.
			p.mu.Unlock()
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-time.After(100 * time.Millisecond):
			}
			continue
		}

		// Need a session for this key: reuse a free one or create new
		now := time.Now()
		for i, s := range p.free {
			if p.freeStale(now, s) {
				continue
			}
			if s.tryLock() {
				p.free = append(p.free[:i], p.free[i+1:]...)
				s.bound = true
				p.binds[key] = &boundEntry{session: s, lastUsed: time.Now()}
				p.mu.Unlock()
				return s, nil
			}
		}
		atMax := len(p.free)+len(p.binds) >= p.config.MaxSize
		p.mu.Unlock()
		if atMax {
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-time.After(300 * time.Millisecond):
			}
			continue
		}

		// No reusable free session for this key: start a background creation
		// (shared across waiters) and poll.  The created session lands in the
		// free list and is bound on the next poll.  When creation fails and no
		// session is available at all, surface the error instead of spinning.
		p.warm()
		if err := p.warmError(); err != nil {
			p.warmingMu.Lock()
			inFlight := p.warming
			p.warmingMu.Unlock()
			p.mu.Lock()
			hasAny := len(p.free) > 0
			p.mu.Unlock()
			if !hasAny && inFlight == 0 {
				return nil, fmt.Errorf("create session: %w", err)
			}
		}
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(300 * time.Millisecond):
		}
		continue
	}
}

// Release releases a free-list session after use.
// The caller holds the session lock (from Acquire); do not lock again here.
func (p *Pool) Release(s *Session, success bool) {
	s.ReqCount++
	if success {
		s.FailedCount = 0
	} else {
		s.FailedCount++
	}
	exceeded := p.config.MaxReqPerSession > 0 && s.ReqCount >= p.config.MaxReqPerSession
	failed := s.FailedCount >= 3

	s.Unlock() // release the lock held since Acquire
	if failed || exceeded {
		if exceeded {
			if s.quotaExceeded {
				p.exhaustFingerprint(s.Client.FingerprintSignature())
			}
			log.Printf("pool: session %s recycled after %d requests", s.SessionID, s.ReqCount)
		}
		p.removeSession(s)
	}
}

// ReleaseBind releases a bound session after use.
// On success the binding is kept for continuity; on failure it is removed.
// Sessions that hit request/quota limits are recycled and replaced by a fresh
// one on the next Bind — this is the "incognito" rotation that bypasses the
// per-browser-state daily usage cap.
// The caller holds the session lock (from Bind); do not lock again here.
func (p *Pool) ReleaseBind(key string, s *Session, success bool) {
	s.ReqCount++
	if success {
		s.FailedCount = 0
	} else {
		s.FailedCount++
	}
	exceeded := p.config.MaxReqPerSession > 0 && s.ReqCount >= p.config.MaxReqPerSession
	failed := s.FailedCount >= 3

	if failed || exceeded {
		// Direct removal: a failed or quota-exceeded session is dropped
		// entirely (binding + pool structures), never left as an orphan.
		// The next request with this key creates a brand-new session
		// (new conversation id + fresh browser fingerprint).
		if exceeded && s.quotaExceeded {
			p.exhaustFingerprint(s.Client.FingerprintSignature())
		}
		p.removeSession(s)
		if exceeded {
			log.Printf("pool: removed session %s for key %s after %d requests", s.SessionID, key, s.ReqCount)
		} else {
			log.Printf("pool: removed failed session %s for key %s", s.SessionID, key)
		}
		s.Unlock()
		return
	}
	s.Unlock()
}

// removeSession drops a failed session entirely
func (p *Pool) removeSession(s *Session) {
	p.mu.Lock()
	if s.bound {
		for k, be := range p.binds {
			if be.session == s {
				delete(p.binds, k)
			}
		}
	} else {
		for i, x := range p.free {
			if x == s {
				p.free = append(p.free[:i], p.free[i+1:]...)
				break
			}
		}
	}
	p.mu.Unlock()
}

// fingerprintCooling reports whether a fingerprint is inside its 24h cooldown
// after being marked exhausted by the upstream.
func (p *Pool) fingerprintCooling(sig string) bool {
	p.fpMu.Lock()
	defer p.fpMu.Unlock()
	exp, ok := p.exhaustedFP[sig]
	return ok && time.Since(exp) < fingerprintCooldown
}

// exhaustFingerprint marks a fingerprint as exhausted for 24h so the pool
// stops drawing new sessions with it ("彻底失效移除" until cooldown expires).
func (p *Pool) exhaustFingerprint(sig string) {
	if sig == "" {
		return
	}
	p.fpMu.Lock()
	defer p.fpMu.Unlock()
	if _, ok := p.exhaustedFP[sig]; !ok {
		p.exhaustedFP[sig] = time.Now()
		log.Printf("pool: fingerprint exhausted, cooling down 24h: %.40s", sig)
	}
}

// cleanupExhausted forgets fingerprints whose 24h cooldown has expired.
func (p *Pool) cleanupExhausted() {
	p.fpMu.Lock()
	defer p.fpMu.Unlock()
	for sig, exp := range p.exhaustedFP {
		if time.Since(exp) >= fingerprintCooldown {
			delete(p.exhaustedFP, sig)
			log.Printf("pool: fingerprint cooldown expired, reactivated: %.40s", sig)
		}
	}
}

// ReplayHistory returns the cached dialog turns to replay into sessionID.
// If sessionID is the same session that most recently served the key, the
// upstream conversation already carries the context (cache hit in-session)
// and nothing is replayed.  Otherwise the full cached history is returned so
// a fresh session can rebuild the conversation ("缓存命中").
//
// Hit-rate accounting:
//   - hits: the key already has cached dialog history (len(turns)>0), regardless
//     of whether we can short-circuit by using the same live session.
//   - misses: the key has no cached history, so nothing can be reused.
//   - replays: the subset of hits where we actually replay history into a
//     fresh session (i.e. lastSrv != current session).
func (p *Pool) ReplayHistory(key, sessionID string) []DialogTurn {
	p.cacheMu.Lock()
	defer p.cacheMu.Unlock()
	turns := p.cache[key]
	if len(turns) == 0 {
		p.replayMisses++
		return nil
	}
	p.replayHits++
	if p.lastSrv[key] == sessionID {
		return nil
	}
	p.replays++
	if len(turns) > maxReplayTurns {
		turns = turns[len(turns)-maxReplayTurns:]
	}
	out := make([]DialogTurn, len(turns))
	copy(out, turns)
	return out
}

// AppendHistory records one completed user→assistant turn for a key and marks
// sessionID as the session that now carries the live context.
func (p *Pool) AppendHistory(key, sessionID, userText, assistantText string) {
	p.cacheMu.Lock()
	defer p.cacheMu.Unlock()
	p.lastSrv[key] = sessionID
	p.cacheLast[key] = time.Now()
	if userText == "" {
		return
	}
	p.cache[key] = append(p.cache[key], DialogTurn{Role: "user", Text: userText})
	p.cache[key] = append(p.cache[key], DialogTurn{Role: "assistant", Text: assistantText})
	if n := len(p.cache[key]); n > maxCacheTurns {
		p.cache[key] = p.cache[key][n-maxCacheTurns:]
	}
}

// cleanupCache drops dialog caches that have been idle past dialogCacheTTL.
func (p *Pool) cleanupCache() {
	p.cacheMu.Lock()
	defer p.cacheMu.Unlock()
	now := time.Now()
	for k, last := range p.cacheLast {
		if now.Sub(last) > dialogCacheTTL {
			delete(p.cache, k)
			delete(p.cacheLast, k)
			delete(p.lastSrv, k)
		}
	}
}

// totalLocked returns the total number of sessions managed (free + bound).
// Caller must hold p.mu.
func (p *Pool) totalLocked() int {
	return len(p.free) + len(p.binds)
}

// Count returns the total number of sessions managed
func (p *Pool) Count() int {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.totalLocked()
}

// Stats returns pool statistics
func (p *Pool) Stats() map[string]interface{} {
	p.mu.RLock()
	sessions := make([]*Session, 0, p.totalLocked())
	sessions = append(sessions, p.free...)
	for _, be := range p.binds {
		sessions = append(sessions, be.session)
	}
	freeN, boundN := len(p.free), len(p.binds)
	p.mu.RUnlock()

	active, failed := 0, 0
	for _, s := range sessions {
		s.mu.Lock()
		if s.active.Load() {
			active++
		}
		if s.FailedCount > 0 {
			failed++
		}
		s.mu.Unlock()
	}

	p.cacheMu.Lock()
	dialogKeys := len(p.cache)
	dialogReplays := p.replays
	dialogHits := p.replayHits
	dialogMisses := p.replayMisses
	dialogTotal := dialogHits + dialogMisses
	var dialogHitRate float64
	if dialogTotal > 0 {
		dialogHitRate = float64(dialogHits) / float64(dialogTotal)
	}
	p.cacheMu.Unlock()

	p.fpMu.Lock()
	exhaustedFPs := len(p.exhaustedFP)
	p.fpMu.Unlock()

	stats := map[string]interface{}{
		"free":            freeN,
		"bound":           boundN,
		"total":           freeN + boundN,
		"min":             p.config.MinSize,
		"max":             p.config.MaxSize,
		"ttl_s":           p.config.TTL.Seconds(),
		"free_ttl_s":      p.config.FreeTTL.Seconds(),
		"bind_ttl_s":      p.config.BindTTL.Seconds(),
		"active":          active,
		"failed":          failed,
		"dialog_keys":     dialogKeys,
		"dialog_replays":  dialogReplays,
		"dialog_hits":     dialogHits,
		"dialog_misses":   dialogMisses,
		"dialog_hit_rate": math.Round(dialogHitRate*10000) / 10000,
		"exhausted_fps":   exhaustedFPs,
	}
	return stats
}

// maintain periodically cleans up expired sessions and idle bindings, and
// pre-warms the pool when no free session is available (under pressure) so a
// burst of requests is not held up by a slow serialized session creation.
func (p *Pool) maintain() {
	ticker := time.NewTicker(15 * time.Second)
	defer ticker.Stop()

	for range ticker.C {
		now := time.Now()
		p.mu.Lock()

		// Drop expired free sessions (FreeTTL age cap + explicit TTL)
		kept := make([]*Session, 0, len(p.free))
		for _, s := range p.free {
			if p.freeStale(now, s) {
				log.Printf("pool: reaped stale free session %s (age %s)", s.SessionID, now.Sub(s.CreatedAt).Round(time.Second))
				continue
			}
			kept = append(kept, s)
		}
		p.free = kept

		// Reap idle bound sessions
		for k, be := range p.binds {
			if now.Sub(be.lastUsed) > p.config.BindTTL || (p.config.TTL > 0 && now.Sub(be.session.CreatedAt) > p.config.TTL) {
				if be.session.mu.TryLock() {
					delete(p.binds, k)
					be.session.bound = false
					be.session.mu.Unlock()
					p.free = append(p.free, be.session)
					log.Printf("pool: reaped idle binding %s", k)
				}
			}
		}

		// Count genuinely available free sessions (not expired, not busy).
		freeAvail := 0
		for _, s := range p.free {
			if p.freeStale(now, s) {
				continue
			}
			if s.mu.TryLock() {
				s.mu.Unlock()
				freeAvail++
			}
		}

		need := p.config.MinSize - freeAvail
		if freeAvail == 0 && p.totalLocked() < p.config.MaxSize && need < 1 {
			need = 1 // under pressure: pre-warm one extra session
		}
		if p.totalLocked() >= p.config.MaxSize {
			need = 0
		}
		p.mu.Unlock()

		p.cleanupCache()
		p.cleanupExhausted()

		// Refill through the async warm path (capped at maxConcurrentWarm
		// parallel creates) so this tick never blocks on a slow upstream RPC
		// and the pool still scales under a burst.  Any shortfall is topped
		// up on subsequent ticks.
		for i := 0; i < need; i++ {
			p.warm()
		}

		p.mu.Lock()
		count := p.totalLocked()
		freeCount := len(p.free)
		bound := len(p.binds)
		p.mu.Unlock()
		log.Printf("pool maintenance: %d sessions (free=%d, bound=%d)", count, freeCount, bound)
	}
}

// Close closes the pool
func (p *Pool) Close() {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.free = nil
	p.binds = make(map[string]*boundEntry)
}
