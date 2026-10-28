package storine

import (
	"errors"
	"fmt"
)

// Error kinds. Test with errors.Is(err, storine.ErrConflict).
var (
	// ErrNotFound: the document does not exist (e.g. Update of a missing key).
	ErrNotFound = errors.New("storine: not found")
	// ErrConflict: a condition did not hold (already exists, does not exist, or changed). Re-read and decide again.
	ErrConflict = errors.New("storine: conflict")
	// ErrInvalid: the request was rejected as invalid (bad value, type mismatch, bound violated). Do not retry.
	ErrInvalid = errors.New("storine: invalid")
	// ErrUnauthorized: authentication failed or the token's role does not allow this operation.
	ErrUnauthorized = errors.New("storine: unauthorized")
	// ErrOverloaded: the server is temporarily overloaded (ErrRateLimited is one too). Retry later.
	ErrOverloaded = errors.New("storine: overloaded")
	// ErrRateLimited: this connection exceeded its rate limit. Retry later.
	ErrRateLimited = errors.New("storine: rate limited")
	// ErrUnavailable: no server (or no cluster leader) could be reached in time. Retry later.
	ErrUnavailable = errors.New("storine: unavailable")
	// ErrServer: the server failed internally; the message has the details.
	ErrServer = errors.New("storine: server error")
)

// Error is every error returned by this package. Kind is one of the Err* values above.
type Error struct {
	Kind    error
	Message string
	Status  int // wire status, 0 when the error was detected locally
	// WriteIndex is the position of the failing write in the request (-1 if unknown).
	WriteIndex int
	// CurrentVersion is the document's version now (Conflict only); 0 when it does not exist.
	CurrentVersion uint64
}

func (e *Error) Error() string { return fmt.Sprintf("%v: %s", e.Kind, e.Message) }

// Is makes errors.Is(err, storine.ErrConflict) work; a rate limit also counts as overload.
func (e *Error) Is(target error) bool {
	return target == e.Kind || (e.Kind == ErrRateLimited && target == ErrOverloaded)
}

func newError(kind error, status int, msg string) *Error {
	return &Error{Kind: kind, Message: msg, Status: status, WriteIndex: -1}
}

func invalidf(format string, args ...any) *Error {
	return newError(ErrInvalid, 0, fmt.Sprintf(format, args...))
}
