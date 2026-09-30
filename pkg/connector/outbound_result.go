package connector

import (
	"context"
	"errors"

	"github.com/highesttt/matrix-line-messenger/pkg/e2ee"
	"github.com/highesttt/matrix-line-messenger/pkg/line"
)

// Transport-level outcome of one outbound send, for the future DIVA outbound
// endpoint (LINE-1A PR 4). Nothing here changes how a message is sent; it only
// describes what sendLineOutbound did or why it failed.

// outboundErrorCode is the transport error taxonomy. Only codes backed by an
// existing error shape are produced by classifyOutboundError; the others are
// reserved for the endpoint layer and are never guessed from LINE errors.
type outboundErrorCode string

const (
	outboundInvalidRequest         outboundErrorCode = "INVALID_REQUEST"
	outboundUnauthorized           outboundErrorCode = "UNAUTHORIZED" // reserved: endpoint auth (PR 4)
	outboundUnsupportedMessageType outboundErrorCode = "UNSUPPORTED_MESSAGE_TYPE"
	outboundNoActiveLogin          outboundErrorCode = "NO_ACTIVE_LOGIN" // reserved: endpoint login selection (PR 4)
	outboundLineSessionInvalid     outboundErrorCode = "LINE_SESSION_INVALID"
	outboundTargetNotFound         outboundErrorCode = "TARGET_NOT_FOUND"
	outboundBlocked                outboundErrorCode = "BLOCKED"
	outboundE2EEKeyUnavailable     outboundErrorCode = "E2EE_KEY_UNAVAILABLE"
	outboundReplyTargetNotFound    outboundErrorCode = "REPLY_TARGET_NOT_FOUND"
	outboundUploadFailed           outboundErrorCode = "UPLOAD_FAILED"
	outboundLineTransient          outboundErrorCode = "LINE_TRANSIENT"
	outboundTimeout                outboundErrorCode = "TIMEOUT"
	outboundInternal               outboundErrorCode = "INTERNAL"
)

// outboundDelivery says whether the message reached LINE.
type outboundDelivery string

const (
	// deliveryNotSent: no sendMessage request was made, or LINE answered it
	// with an error. Retrying cannot create a duplicate.
	deliveryNotSent outboundDelivery = "not_sent"
	// deliverySent: LINE accepted the message; a later step failed.
	deliverySent outboundDelivery = "sent"
	// deliveryUnknown: a sendMessage request was made but no answer came back
	// (timeout, network, 5xx). The message may be in the chat. Retry only with
	// the same request identity, or hand over to a human.
	deliveryUnknown outboundDelivery = "unknown"
)

// outboundFallback names a transport-level degradation the caller should know about.
type outboundFallback string

const (
	// fallbackPlaintextNoE2EE: the account can do E2EE, but this message went
	// out in plaintext (Letter Sealing off, no usable group key, E2EE setup
	// failure, or LINE selected the plain media flow).
	fallbackPlaintextNoE2EE outboundFallback = "plaintext_no_e2ee"
	// fallbackReplyRelationDropped: LINE could not find the reply target and
	// the message was re-sent without the quote.
	fallbackReplyRelationDropped outboundFallback = "reply_relation_dropped"
	// fallbackZipWrapped: LINE rejected the file and it was re-sent in a ZIP.
	fallbackZipWrapped outboundFallback = "zip_wrapped"
)

// outboundResult is the result model for one outbound send.
type outboundResult struct {
	OK      bool                   `json:"ok"`
	Message *outboundResultMessage `json:"message,omitempty"`
	// Fallbacks is never null.
	Fallbacks []outboundFallback `json:"fallbacks"`
	// Deduplicated is reserved for request_id dedupe (PR 4); always false here.
	Deduplicated bool                 `json:"deduplicated"`
	Error        *outboundResultError `json:"error,omitempty"`
}

type outboundResultMessage struct {
	// ID is the LINE server-assigned message ID.
	ID     string `json:"id"`
	ChatID string `json:"chat_id"`
	// CreatedAt is LINE's createdTime in epoch ms when LINE returned one,
	// otherwise the local send time.
	CreatedAt int64 `json:"created_at"`
	E2EE      bool  `json:"e2ee"`
}

type outboundResultError struct {
	Code      outboundErrorCode `json:"code"`
	Retryable bool              `json:"retryable"`
	Delivery  outboundDelivery  `json:"delivery"`
	Detail    string            `json:"detail"`
}

// ---------------------------------------------------------------------------
// failure markers
// ---------------------------------------------------------------------------

type outboundFailureKind int

const (
	failInvalidRequest outboundFailureKind = iota + 1
	failUnsupportedType
	// failSendAttempt marks an error returned by a sendMessage call.
	failSendAttempt
	// failUpload marks an OBS upload made before sendMessage.
	failUpload
	// failPostSend marks a failure after LINE accepted the message.
	failPostSend
	// failOwnE2EEKey marks a missing own E2EE key on the 1:1 path.
	failOwnE2EEKey
	// failReplyTargetMissing marks a reply-target-not-found failure when the
	// caller did not allow the reply fallback.
	failReplyTargetMissing
)

// outboundFailure tags an error with where in the send flow it happened. It
// keeps the wrapped error's text, so Matrix sees exactly the same message.
type outboundFailure struct {
	kind outboundFailureKind
	err  error
	// result is set for failPostSend: the message LINE accepted.
	result *lineOutboundResult
}

func (f *outboundFailure) Error() string { return f.err.Error() }
func (f *outboundFailure) Unwrap() error { return f.err }

func markOutboundFailure(kind outboundFailureKind, err error) error {
	if err == nil {
		return nil
	}
	return &outboundFailure{kind: kind, err: err}
}

// findOutboundFailure returns the outermost failure marker of the given kind.
func findOutboundFailure(err error, kind outboundFailureKind) *outboundFailure {
	for err != nil {
		if f, ok := err.(*outboundFailure); ok && f.kind == kind {
			return f
		}
		err = errors.Unwrap(err)
	}
	return nil
}

// ---------------------------------------------------------------------------
// classification
// ---------------------------------------------------------------------------

const outboundErrorDetailMax = 500

// classifyOutboundError maps an error from sendLineOutbound to the transport
// taxonomy and decides whether the message may already be in the chat.
func classifyOutboundError(err error) outboundResultError {
	result := outboundResultError{
		Code:     outboundInternal,
		Delivery: outboundDeliveryFor(err),
		Detail:   err.Error(),
	}
	if len(result.Detail) > outboundErrorDetailMax {
		result.Detail = result.Detail[:outboundErrorDetailMax]
	}

	class := line.Classify(err)
	switch {
	case findOutboundFailure(err, failInvalidRequest) != nil:
		result.Code = outboundInvalidRequest
	case findOutboundFailure(err, failUnsupportedType) != nil:
		result.Code = outboundUnsupportedMessageType
	case errors.Is(err, errLineOutboundBlocked):
		result.Code = outboundBlocked
	case findOutboundFailure(err, failReplyTargetMissing) != nil:
		result.Code = outboundReplyTargetNotFound
	case errors.Is(err, errLineSessionInvalidated),
		errors.Is(err, errLineClientSuperseded),
		class == line.ClassSessionInvalid:
		result.Code = outboundLineSessionInvalid
	case errors.Is(err, e2ee.ErrMissingOwnPrivateKey),
		errors.Is(err, e2ee.ErrGroupKeyNotLoaded),
		findOutboundFailure(err, failOwnE2EEKey) != nil,
		class == line.ClassGroupKeyNotRegistered:
		result.Code = outboundE2EEKeyUnavailable
	case class == line.ClassNotAMember:
		result.Code = outboundTargetNotFound
	case class == line.ClassTimeout:
		result.Code = outboundTimeout
		result.Retryable = true
	case findOutboundFailure(err, failUpload) != nil,
		findOutboundFailure(err, failPostSend) != nil:
		result.Code = outboundUploadFailed
		result.Retryable = true
	case class == line.ClassNetwork, class == line.ClassServerError:
		result.Code = outboundLineTransient
		result.Retryable = true
	}
	return result
}

// outboundDeliveryFor decides delivery from where the failure happened:
// after LINE accepted the message it was sent; on a sendMessage call that got
// no definitive answer it is unknown; anything else was not sent.
func outboundDeliveryFor(err error) outboundDelivery {
	if findOutboundFailure(err, failPostSend) != nil {
		return deliverySent
	}
	if attempt := findOutboundFailure(err, failSendAttempt); attempt != nil {
		switch line.Classify(attempt.err) {
		case line.ClassTimeout, line.ClassNetwork, line.ClassServerError:
			return deliveryUnknown
		}
		if errors.Is(attempt.err, context.Canceled) {
			return deliveryUnknown
		}
	}
	return deliveryNotSent
}

// buildOutboundResult turns the outcome of sendLineOutbound into the result model.
func (lc *LineClient) buildOutboundResult(chatMID string, sent *lineOutboundResult, err error) outboundResult {
	out := outboundResult{OK: err == nil, Fallbacks: []outboundFallback{}}
	if err != nil {
		classified := classifyOutboundError(err)
		out.Error = &classified
		if post := findOutboundFailure(err, failPostSend); post != nil {
			sent = post.result
		} else {
			sent = nil
		}
	}
	if sent == nil || sent.Sent == nil {
		return out
	}

	createdAt := sent.SentAt.UnixMilli()
	if serverTime, errTime := sent.Sent.CreatedTime.Int64(); errTime == nil && serverTime > 0 {
		createdAt = serverTime
	}
	out.Message = &outboundResultMessage{
		ID:        sent.Sent.ID,
		ChatID:    chatMID,
		CreatedAt: createdAt,
		E2EE:      !sent.SentPlaintext,
	}
	if sent.SentPlaintext && lc.E2EE != nil {
		out.Fallbacks = append(out.Fallbacks, fallbackPlaintextNoE2EE)
	}
	if sent.ReplyDropped {
		out.Fallbacks = append(out.Fallbacks, fallbackReplyRelationDropped)
	}
	if sent.ZipWrapped {
		out.Fallbacks = append(out.Fallbacks, fallbackZipWrapped)
	}
	return out
}
