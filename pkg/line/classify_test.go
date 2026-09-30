package line

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/url"
	"testing"
)

type fakeNetError struct{ timeout bool }

func (e fakeNetError) Error() string   { return "fake net error" }
func (e fakeNetError) Timeout() bool   { return e.timeout }
func (e fakeNetError) Temporary() bool { return false }

var _ net.Error = fakeNetError{}

// Error strings below are the ones already used by errors_test.go and the
// formats produced by callRPC and the OBS client.
func TestClassify(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want Class
	}{
		{"nil", nil, ClassUnknown},

		// session / auth
		{"refresh required 119", errors.New(`API error 400: {"code":10051,"message":"RESPONSE_ERROR","data":{"name":"TalkException","code":119,"reason":"Access token refresh required"}}`), ClassSessionInvalid},
		{"logged out", errors.New("V3_TOKEN_CLIENT_LOGGED_OUT"), ClassSessionInvalid},
		{"invalid sender key 83", errors.New(`API error 400: {"code":10051,"message":"RESPONSE_ERROR","data":{"name":"TalkException","message":"TalkException","code":83,"reason":"invalid sender key","parameterMap":null}}`), ClassSessionInvalid},
		{"request need login", errors.New(`SSE error: 401: {"code":10004,"message":"REQUEST_NEED_LOGIN"}`), ClassSessionInvalid},
		{"api 401", errors.New("API error 401: unauthorized"), ClassSessionInvalid},
		{"obs upload 403", errors.New("OBS upload failed (403): forbidden"), ClassSessionInvalid},
		{"recovery failed keeps auth cause", fmt.Errorf("failed to recover token after LINE auth error (%w): %w",
			errors.New("V3_TOKEN_CLIENT_LOGGED_OUT"), errors.New("login failed")), ClassSessionInvalid},

		// talk exceptions
		{"not a member 10", errors.New(`API error 400: {"code":10051,"message":"RESPONSE_ERROR","data":{"name":"TalkException","message":"TalkException","code":10,"reason":"not a member","parameterMap":null}}`), ClassNotAMember},
		{"group key not registered 99", errors.New(`API error 400: {"code":10051,"message":"RESPONSE_ERROR","data":{"name":"TalkException","code":99,"reason":"group key is not registered"}}`), ClassGroupKeyNotRegistered},
		{"not found 5", errors.New(`API error 400: {"code":10051,"message":"RESPONSE_ERROR","data":{"name":"TalkException","message":"TalkException","code":5,"reason":"not found","parameterMap":null}}`), ClassTalkNotFound},
		{"code 5 different reason", errors.New(`API error 400: {"code":10051,"message":"RESPONSE_ERROR","data":{"name":"TalkException","message":"TalkException","code":5,"reason":"different","parameterMap":null}}`), ClassUnknown},

		// timeout
		{"context deadline", fmt.Errorf("request failed: %w", context.DeadlineExceeded), ClassTimeout},
		{"url error timeout", fmt.Errorf("request failed: %w", &url.Error{Op: "Post", URL: "https://line", Err: fakeNetError{timeout: true}}), ClassTimeout},
		{"dial i/o timeout text", errors.New("request failed: dial tcp: i/o timeout"), ClassTimeout},
		{"obs deadline text", errors.New("OBS download request failed: context deadline exceeded"), ClassTimeout},

		// network
		{"connection reset", errors.New("OBS upload request failed: connection reset by peer"), ClassNetwork},
		{"url error", fmt.Errorf("request failed: %w", &url.Error{Op: "Post", URL: "https://line", Err: errors.New("connection refused")}), ClassNetwork},

		// server
		{"api 502", errors.New("API error 502: bad gateway"), ClassServerError},
		{"obs upload 503", errors.New("failed to upload image to OBS: OBS upload failed (503): unavailable"), ClassServerError},

		// unknown: no guessing
		{"api 404", errors.New("HTTP 404: not found"), ClassUnknown},
		{"sendMessage code != 0", errors.New("sendMessage failed: Extension does not support file upload"), ClassUnknown},
		{"plain error", errors.New("something else"), ClassUnknown},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := Classify(tt.err); got != tt.want {
				t.Fatalf("Classify(%v) = %q, want %q", tt.err, got, tt.want)
			}
		})
	}
}
