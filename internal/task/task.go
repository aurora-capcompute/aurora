// Package task owns durable approval: when a brain yields a capability that
// needs out-of-band confirmation, this package turns the yield into a persisted
// task record, mints an HMAC-derived token the caller resolves against, and on
// approval replays the original call back through the wrapped dispatcher. A
// task's token hash is the only secret-derived value the store persists out of
// band; the record itself omits it from JSON.
//
// It owns the task lifecycle and token scheme, not the capability behind the
// task — the underlying dispatcher and the durable store are injected.
package task

import (
	"capcompute/dispatcher"
	"capcompute/dispatcher/replay/tape/journaled"
	"context"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"time"

	resolutionpkg "aurora-dispatchers/resolution"
)

type Scope struct {
	TenantID string
	ThreadID string
	RunID    string
	Revision uint64
}

type State = resolutionpkg.Decision

const (
	StatePending   State = "pending"
	StateApproved        = resolutionpkg.Approved
	StateCompleted       = resolutionpkg.Completed
	StateFailed          = resolutionpkg.Failed
	StateDenied          = resolutionpkg.Denied
	StateCancelled       = resolutionpkg.Cancelled
	StateExpired   State = "expired"
	StateExecuted  State = "executed"
)

type Resolution = resolutionpkg.Resolution

type Record struct {
	Scope           Scope           `json:"scope"`
	ID              string          `json:"id"`
	JournalPosition int             `json:"journal_position"`
	CallHash        string          `json:"call_hash"`
	Call            dispatcher.Call `json:"call"`
	Summary         string          `json:"summary"`
	State           State           `json:"state"`
	TokenHash       []byte          `json:"-"`
	Resolution      Resolution      `json:"resolution,omitempty"`
	CreatedAt       time.Time       `json:"created_at"`
	ExpiresAt       *time.Time      `json:"expires_at,omitempty"`
	ResolvedAt      *time.Time      `json:"resolved_at,omitempty"`
}

type Store interface {
	Find(context.Context, Scope, int, string) (Record, bool, error)
	Create(context.Context, Record) error
	Get(context.Context, string, string) (Record, error)
	List(context.Context, string, string) ([]Record, error)
	Resolve(context.Context, string, string, []byte, Resolution, time.Time) (Record, error)
	MarkExecuted(context.Context, string, string, time.Time) error
}

var (
	ErrNotFound     = errors.New("task not found")
	ErrConflict     = errors.New("task resolution conflict")
	ErrGone         = errors.New("task is no longer resolvable")
	ErrUnauthorized = errors.New("invalid task token")
)

func WithResolution(ctx context.Context, resolution Resolution) context.Context {
	return resolutionpkg.WithContext(ctx, resolution)
}

func ResolutionFromContext(ctx context.Context) (Resolution, bool) {
	return resolutionpkg.FromContext(ctx)
}

type Dispatcher[K any] struct {
	Next          dispatcher.Dispatcher[K]
	Store         Store
	Journal       journaled.Journal
	Scope         func(K) Scope
	Now           func() time.Time
	TokenSecret   []byte
	TaskTTL       time.Duration
	OnTaskCreated func(Record)
}

func (d *Dispatcher[K]) Dispatch(ctx context.Context, key K, call dispatcher.Call) (dispatcher.Outcome, error) {
	if d.Next == nil || d.Store == nil || d.Journal == nil || d.Scope == nil {
		return dispatcher.Outcome{}, errors.New("task dispatcher is not configured")
	}
	scope := d.Scope(key)
	position := d.Journal.Length()
	callHash := HashCall(call)
	record, found, err := d.Store.Find(ctx, scope, position, callHash)
	if err != nil {
		return dispatcher.Outcome{}, err
	}
	if found {
		return d.resume(ctx, key, record)
	}

	outcome, err := d.Next.Dispatch(ctx, key, call)
	if err != nil || outcome.Kind() != dispatcher.OutcomeYield {
		return outcome, err
	}
	now := d.now()
	taskID, err := randomID()
	if err != nil {
		return dispatcher.Outcome{}, err
	}
	record = Record{
		Scope:           scope,
		ID:              taskID,
		JournalPosition: position,
		CallHash:        callHash,
		Call:            call.Copy(),
		Summary:         outcome.Message(),
		State:           StatePending,
		CreatedAt:       now,
	}
	if d.TaskTTL > 0 {
		expires := now.Add(d.TaskTTL)
		record.ExpiresAt = &expires
	}
	token := Token(d.TokenSecret, scope.TenantID, record.ID)
	sum := sha256.Sum256([]byte(token))
	record.TokenHash = sum[:]
	if err := d.Store.Create(ctx, record); err != nil {
		return dispatcher.Outcome{}, err
	}
	if d.OnTaskCreated != nil {
		d.OnTaskCreated(record)
	}
	return dispatcher.Yield(record.ID), nil
}

func (d *Dispatcher[K]) resume(ctx context.Context, key K, record Record) (dispatcher.Outcome, error) {
	if record.ExpiresAt != nil && !d.now().Before(*record.ExpiresAt) && record.State == StatePending {
		return dispatcher.Failed("external task expired"), nil
	}
	switch record.State {
	case StatePending:
		return dispatcher.Yield(record.ID), nil
	case StateApproved:
		outcome, err := d.Next.Dispatch(WithResolution(ctx, record.Resolution), key, record.Call)
		if err == nil && outcome.Kind() != dispatcher.OutcomeYield {
			_ = d.Store.MarkExecuted(ctx, record.Scope.TenantID, record.ID, d.now())
		}
		return outcome, err
	case StateCompleted:
		return dispatcher.Result(record.Resolution.Data), nil
	case StateDenied:
		return dispatcher.Failed(nonempty(record.Resolution.Reason, "external task denied")), nil
	case StateFailed:
		return dispatcher.Failed(nonempty(record.Resolution.Reason, "external task failed")), nil
	case StateCancelled:
		return dispatcher.Failed("external task cancelled"), nil
	case StateExpired:
		return dispatcher.Failed("external task expired"), nil
	case StateExecuted:
		return d.Next.Dispatch(WithResolution(ctx, record.Resolution), key, record.Call)
	default:
		return dispatcher.Outcome{}, fmt.Errorf("unsupported task state %q", record.State)
	}
}

func (d *Dispatcher[K]) Capabilities() []dispatcher.Capability {
	return dispatcher.Capabilities(d.Next)
}

func HashCall(call dispatcher.Call) string {
	sum := sha256.New()
	_, _ = sum.Write([]byte(call.Name))
	_, _ = sum.Write([]byte{0})
	_, _ = sum.Write(call.Args)
	return hex.EncodeToString(sum.Sum(nil))
}

func Token(secret []byte, tenantID, taskID string) string {
	mac := hmac.New(sha256.New, secret)
	_, _ = mac.Write([]byte(tenantID))
	_, _ = mac.Write([]byte{0})
	_, _ = mac.Write([]byte(taskID))
	return hex.EncodeToString(mac.Sum(nil))
}

func VerifyToken(expectedHash []byte, token string) bool {
	sum := sha256.Sum256([]byte(token))
	return hmac.Equal(expectedHash, sum[:])
}

func (d *Dispatcher[K]) now() time.Time {
	if d.Now != nil {
		return d.Now().UTC()
	}
	return time.Now().UTC()
}

func randomID() (string, error) {
	var raw [16]byte
	if _, err := rand.Read(raw[:]); err != nil {
		return "", err
	}
	return "task_" + hex.EncodeToString(raw[:]), nil
}

func nonempty(value, fallback string) string {
	if value != "" {
		return value
	}
	return fallback
}
