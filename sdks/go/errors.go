package exspeed

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"
)

// Error codes the server returns (HTTP-like). See [ServerError].
const (
	// CodeBadRequest: malformed request, invalid name or filter, invalid config.
	CodeBadRequest = 400
	// CodeUnauthorized: not authenticated (bad or missing token).
	CodeUnauthorized = 401
	// CodeForbidden: the credential lacks the needed action.
	CodeForbidden = 403
	// CodeNotFound: stream, consumer, bucket or key not found, or a core
	// request had no responders.
	CodeNotFound = 404
	// CodeQueryTimeout: a query timed out.
	CodeQueryTimeout = 408
	// CodeConflict: exists with different settings, stream still has
	// consumers, a msg id reused with a different body, or a KV key not at
	// the expected revision.
	CodeConflict = 409
	// CodeQueryTooLarge: a query exceeded the server's query memory limit.
	CodeQueryTooLarge = 422
	// CodeTooManyRequests: retry later (dedup map full, the stream is full
	// with discard "new", too many concurrent waiting requests).
	CodeTooManyRequests = 429
	// CodeInternal: internal server error.
	CodeInternal = 500
	// CodeUnavailable: not the leader (see [ServerError.LeaderHint]), still
	// starting, or not enough in-sync replicas.
	CodeUnavailable = 503
	// CodeInsufficientStorage: the server's disk is full; nothing was written.
	CodeInsufficientStorage = 507
)

// ServerError is an Error reply from the server.
//
// Code is HTTP-like (see the Code* constants). Detail is the optional
// machine-readable JSON the server attached, as sent (snake_case keys), for
// example {"leader":"host:5933"} (503), {"stored_offset":7} (409) or
// {"retry_after_secs":30} (429).
//
// Compare codes with errors.Is against the Err* sentinels:
//
//	if errors.Is(err, exspeed.ErrNotFound) { ... }
type ServerError struct {
	Code    int
	Message string
	Detail  json.RawMessage
}

func (e *ServerError) Error() string {
	return fmt.Sprintf("exspeed: server error %d: %s", e.Code, e.Message)
}

// Is reports whether target is a code sentinel (a *ServerError with an
// empty message, such as [ErrNotFound]) with the same code.
func (e *ServerError) Is(target error) bool {
	t, ok := target.(*ServerError)
	return ok && t.Message == "" && t.Detail == nil && t.Code == e.Code
}

// DecodeDetail unmarshals Detail into v. It returns an error when there is
// no detail.
func (e *ServerError) DecodeDetail(v any) error {
	if len(e.Detail) == 0 {
		return errors.New("exspeed: error has no detail")
	}
	return json.Unmarshal(e.Detail, v)
}

func (e *ServerError) detailField(name string) (json.RawMessage, bool) {
	var m map[string]json.RawMessage
	if len(e.Detail) == 0 || json.Unmarshal(e.Detail, &m) != nil {
		return nil, false
	}
	v, ok := m[name]
	return v, ok
}

func (e *ServerError) detailUint(name string) (uint64, bool) {
	raw, ok := e.detailField(name)
	if !ok {
		return 0, false
	}
	var v uint64
	if json.Unmarshal(raw, &v) != nil {
		return 0, false
	}
	return v, true
}

// LeaderHint is detail.leader of a 503 "not the leader" error: the leader's
// client address, or "" when unknown.
func (e *ServerError) LeaderHint() string {
	raw, ok := e.detailField("leader")
	if !ok {
		return ""
	}
	var s string
	if json.Unmarshal(raw, &s) != nil {
		return ""
	}
	return s
}

// StoredOffset is detail.stored_offset of a 409 for a msg id reused with a
// different body.
func (e *ServerError) StoredOffset() (uint64, bool) { return e.detailUint("stored_offset") }

// CurrentRevision is detail.current_revision of a 409 from a KV
// compare-and-set.
func (e *ServerError) CurrentRevision() (uint64, bool) { return e.detailUint("current_revision") }

// RetryAfter is detail.retry_after_secs of a 429.
func (e *ServerError) RetryAfter() (time.Duration, bool) {
	s, ok := e.detailUint("retry_after_secs")
	return time.Duration(s) * time.Second, ok
}

// Sentinels for errors.Is on a [*ServerError]'s code.
var (
	ErrBadRequest          = &ServerError{Code: CodeBadRequest}
	ErrUnauthorized        = &ServerError{Code: CodeUnauthorized}
	ErrForbidden           = &ServerError{Code: CodeForbidden}
	ErrNotFound            = &ServerError{Code: CodeNotFound}
	ErrConflict            = &ServerError{Code: CodeConflict}
	ErrTooManyRequests     = &ServerError{Code: CodeTooManyRequests}
	ErrInternal            = &ServerError{Code: CodeInternal}
	ErrUnavailable         = &ServerError{Code: CodeUnavailable}
	ErrInsufficientStorage = &ServerError{Code: CodeInsufficientStorage}
)

// ErrClosed is wrapped by the [*ConnectionError] of calls made after
// [Client.Close], and of calls still pending when it was called.
var ErrClosed = errors.New("exspeed: client closed")

// ErrInvalidArgument is wrapped by errors about arguments that can't be
// sent (for example a priority above 9, or a subject longer than 65,535
// bytes).
var ErrInvalidArgument = errors.New("exspeed: invalid argument")

// ConnectionError reports that the connection is closed, was lost, or is
// being re-established. Requests pending when the connection drops fail
// with it; they are not retried.
type ConnectionError struct {
	Msg string
	Err error
}

func (e *ConnectionError) Error() string {
	if e.Err != nil && e.Msg == "" {
		return "exspeed: connection error: " + e.Err.Error()
	}
	if e.Err != nil {
		return "exspeed: " + e.Msg + ": " + e.Err.Error()
	}
	return "exspeed: " + e.Msg
}

func (e *ConnectionError) Unwrap() error { return e.Err }

// TimeoutError reports that no response arrived within the client's
// request timeout (on top of a pull's or read's own wait), or that a core
// request got no answer in time. It matches context.DeadlineExceeded with
// errors.Is. When the caller's context expires first, calls return the
// context's error instead.
type TimeoutError struct{ Msg string }

func (e *TimeoutError) Error() string { return "exspeed: " + e.Msg }

// Timeout reports true (like net.Error).
func (e *TimeoutError) Timeout() bool { return true }

// Is matches context.DeadlineExceeded.
func (e *TimeoutError) Is(target error) bool { return target == context.DeadlineExceeded }

// ProtocolError reports bytes this client cannot decode, or a reply of the
// wrong type.
type ProtocolError struct{ Msg string }

func (e *ProtocolError) Error() string { return "exspeed: protocol error: " + e.Msg }

// ErrSubscriptionEnded is matched (errors.Is) by the [*SubscriptionEndedError]
// that Next returns once a subscription has ended and its buffer is drained.
var ErrSubscriptionEnded = errors.New("exspeed: subscription ended")

// SubscriptionEndedError says why a subscription ended.
//
// Code 404: the consumer or its stream was deleted. 503: the node lost
// leadership, or the connection was lost and not re-established. 0: ended
// locally (Unsubscribe or Client.Close). Other codes come from a failed
// re-subscribe after a reconnect.
type SubscriptionEndedError struct {
	Code    int
	Message string
}

func (e *SubscriptionEndedError) Error() string {
	return fmt.Sprintf("exspeed: subscription ended (%d): %s", e.Code, e.Message)
}

// Is matches [ErrSubscriptionEnded].
func (e *SubscriptionEndedError) Is(target error) bool { return target == ErrSubscriptionEnded }

// ErrWatchStopped is returned by [KVWatch.Next] after [KVWatch.Stop].
var ErrWatchStopped = errors.New("exspeed: watch stopped")

// ErrPublisherClosed is returned by [Publisher.Publish] after [Publisher.Close].
var ErrPublisherClosed = errors.New("exspeed: publisher closed")

func invalidf(format string, args ...any) error {
	return fmt.Errorf("%w: "+format, append([]any{ErrInvalidArgument}, args...)...)
}
