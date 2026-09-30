package line

import (
	"context"
	"errors"
	"net"
	"net/url"
	"regexp"
	"strings"
)

// Class is what a LINE API error says about the failure. Classify only uses
// error shapes this package already recognises (the IsXxx helpers and the
// error strings produced by callRPC and the OBS client). Anything else is
// ClassUnknown; callers must not guess.
type Class string

const (
	ClassUnknown Class = ""
	// ClassSessionInvalid: the access token is rejected or the session was
	// logged out (IsAuthError: code 119, V3_TOKEN_CLIENT_LOGGED_OUT, invalid
	// sender key, REQUEST_NEED_LOGIN, HTTP/OBS 401 or 403).
	ClassSessionInvalid Class = "session_invalid"
	// ClassNotAMember: TalkException code 10 "not a member" (IsNotAMemberError).
	ClassNotAMember Class = "not_a_member"
	// ClassGroupKeyNotRegistered: sendMessage code 99 "group key is not
	// registered" (IsGroupKeyNotRegisteredError).
	ClassGroupKeyNotRegistered Class = "group_key_not_registered"
	// ClassTalkNotFound: TalkException code 5 "not found". What was not found
	// depends on the call (IsTalkExceptionNotFound), so callers interpret it.
	ClassTalkNotFound Class = "talk_not_found"
	// ClassTimeout: the request hit a deadline or a network timeout.
	ClassTimeout Class = "timeout"
	// ClassNetwork: the request failed before LINE answered (transport error).
	ClassNetwork Class = "network"
	// ClassServerError: LINE answered with an HTTP 5xx.
	ClassServerError Class = "server_error"
)

var serverErrorPattern = regexp.MustCompile(`(?i)(api error 5\d\d|http 5\d\d|obs upload failed \(5\d\d\))`)

// Classify returns the Class of err, or ClassUnknown.
func Classify(err error) Class {
	if err == nil {
		return ClassUnknown
	}
	// Session and protocol errors come first: they are answers from LINE, even
	// when a later retry of the same call failed differently.
	switch {
	case IsAuthError(err):
		return ClassSessionInvalid
	case IsNotAMemberError(err):
		return ClassNotAMember
	case IsGroupKeyNotRegisteredError(err):
		return ClassGroupKeyNotRegistered
	case IsTalkExceptionNotFound(err):
		return ClassTalkNotFound
	}
	if isTimeout(err) {
		return ClassTimeout
	}
	if serverErrorPattern.MatchString(err.Error()) {
		return ClassServerError
	}
	if isNetworkError(err) {
		return ClassNetwork
	}
	return ClassUnknown
}

func isTimeout(err error) bool {
	if errors.Is(err, context.DeadlineExceeded) {
		return true
	}
	var netErr net.Error
	if errors.As(err, &netErr) && netErr.Timeout() {
		return true
	}
	msg := strings.ToLower(err.Error())
	return strings.Contains(msg, "context deadline exceeded") ||
		strings.Contains(msg, "client.timeout exceeded") ||
		strings.Contains(msg, "i/o timeout")
}

func isNetworkError(err error) bool {
	var urlErr *url.Error
	if errors.As(err, &urlErr) {
		return true
	}
	var netErr net.Error
	if errors.As(err, &netErr) {
		return true
	}
	// callRPC and the OBS client wrap transport failures with these prefixes.
	msg := strings.ToLower(err.Error())
	return strings.Contains(msg, "request failed: ") &&
		!strings.Contains(msg, "api error")
}
