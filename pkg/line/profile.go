package line

// Server A OWNER P2: the logged-in account's own profile only.
//
// Display name / status message use the Chrome gateway's sparse
// TalkService/updateProfileAttributes request (one attribute per call). The
// profile picture uses the dedicated personal OBS path talk/p/{selfMid}; chat
// media (talk/m) is never reused for it. These helpers never retry a write,
// never re-login and never accept a caller supplied URL, attribute or target.

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"image"
	"image/color"
	"image/draw"
	"image/jpeg"
	"image/png"
	"io"
	"net/http"
	"net/url"
	"regexp"
	"strconv"
	"strings"
	"unicode"
	"unicode/utf16"
	"unicode/utf8"
)

const (
	ProfileAttributeDisplayName   = 2
	ProfileAttributeStatusMessage = 16

	// Product limits chosen by DIVA. They are conservative safety caps, not a
	// verified LINE limit: LINE may still reject a value inside them.
	ProfileDisplayNameMaxUnits   = 20
	ProfileStatusMessageMaxUnits = 500

	// ProfileImageChannelID is the LINE channel used by CHRLINE's personal
	// profile image upload. The same channel token is already issued through
	// ChannelService by the existing album preview download.
	ProfileImageChannelID     = albumPreviewChannelID
	ProfileImageMaxInputBytes = 10 << 20
	ProfileImageMaxSide       = 4096
	ProfileImageMaxPixels     = 16 << 20
	profileImageMaxOutput     = 10 << 20
	profileImageJPEGQuality   = 90

	profileResponseLimit = 1 << 20
)

var (
	profileSelfMIDPattern = regexp.MustCompile(`^[uU][0-9A-Za-z_-]+$`)
	profileOBSValue       = regexp.MustCompile(`^[0-9A-Za-z._-]{1,256}$`)

	ErrProfileValueInvalid = errors.New("profile value invalid")
	ErrProfileImageInvalid = errors.New("profile image invalid")
)

// ProfileWriteOutcome tells the caller whether LINE may have applied a write.
type ProfileWriteOutcome string

const (
	// ProfileWriteNotSent: nothing reached LINE (validation, token, signer).
	ProfileWriteNotSent ProfileWriteOutcome = "not_sent"
	// ProfileWriteRejected: LINE answered with a definite rejection.
	ProfileWriteRejected ProfileWriteOutcome = "rejected"
	// ProfileWriteUnknown: the request may have been applied (timeout, network,
	// server error or a response that does not follow the contract).
	ProfileWriteUnknown ProfileWriteOutcome = "unknown"
)

// ProfileWriteError is a classified failure. Code is a fixed identifier that
// is safe to log; Err may contain LINE text and must not be logged verbatim.
type ProfileWriteError struct {
	Outcome ProfileWriteOutcome
	Code    string
	Err     error
}

func (e *ProfileWriteError) Error() string {
	if e.Err == nil {
		return fmt.Sprintf("profile write %s: %s", e.Outcome, e.Code)
	}
	return fmt.Sprintf("profile write %s: %s: %v", e.Outcome, e.Code, e.Err)
}

func (e *ProfileWriteError) Unwrap() error { return e.Err }

func profileWriteError(outcome ProfileWriteOutcome, code string, err error) *ProfileWriteError {
	return &ProfileWriteError{Outcome: outcome, Code: code, Err: err}
}

// ProfileWriteOutcomeOf returns the classified outcome of err. An unclassified
// error is treated as unknown so callers never assume "not applied".
func ProfileWriteOutcomeOf(err error) (ProfileWriteOutcome, string) {
	var classified *ProfileWriteError
	if errors.As(err, &classified) {
		return classified.Outcome, classified.Code
	}
	return ProfileWriteUnknown, "PROFILE_WRITE_UNCLASSIFIED"
}

func utf16Units(value string) int {
	units := 0
	for _, r := range value {
		units += utf16.RuneLen(r)
	}
	return units
}

// ValidateProfileText applies DIVA's product checks without changing the value:
// no silent trim, truncation or normalization.
func ValidateProfileText(attribute int, value string) error {
	limit := 0
	switch attribute {
	case ProfileAttributeDisplayName:
		limit = ProfileDisplayNameMaxUnits
	case ProfileAttributeStatusMessage:
		limit = ProfileStatusMessageMaxUnits
	default:
		return fmt.Errorf("%w: attribute not allowed", ErrProfileValueInvalid)
	}
	if value == "" || !utf8.ValidString(value) || strings.TrimSpace(value) != value {
		return fmt.Errorf("%w: empty, invalid UTF-8 or surrounding whitespace", ErrProfileValueInvalid)
	}
	if utf16Units(value) > limit {
		return fmt.Errorf("%w: longer than %d UTF-16 units", ErrProfileValueInvalid, limit)
	}
	for _, r := range value {
		if r == '\n' && attribute == ProfileAttributeStatusMessage {
			continue
		}
		if unicode.Is(unicode.Cc, r) || r == utf8.RuneError || r == ' ' || r == ' ' ||
			(r >= '‪' && r <= '‮') || (r >= '⁦' && r <= '⁩') {
			return fmt.Errorf("%w: control or direction override character", ErrProfileValueInvalid)
		}
	}
	return nil
}

// UpdateProfileAttributeContext sends one sparse profile attribute update:
// [reqSeq, {"profileAttributes": {"<attr>": {"value": value, "meta": {}}}}].
// Only DisplayName (2) and StatusMessage (16) are accepted. One attempt only.
func (c *Client) UpdateProfileAttributeContext(ctx context.Context, reqSeq int64, attribute int, value string) error {
	if err := ValidateProfileText(attribute, value); err != nil {
		return profileWriteError(ProfileWriteNotSent, "PROFILE_VALUE_INVALID", err)
	}
	if reqSeq <= 0 {
		return profileWriteError(ProfileWriteNotSent, "PROFILE_REQSEQ_INVALID", nil)
	}
	request := map[string]any{
		"profileAttributes": map[string]any{
			strconv.Itoa(attribute): map[string]any{"value": value, "meta": map[string]string{}},
		},
	}
	body, err := json.Marshal([]any{reqSeq, request})
	if err != nil {
		return profileWriteError(ProfileWriteNotSent, "PROFILE_REQUEST_ENCODE_FAILED", err)
	}
	req, err := c.newSignedRPCRequest(ctx, BaseURL, "TalkService", "updateProfileAttributes", body)
	if err != nil {
		return profileWriteError(ProfileWriteNotSent, "PROFILE_REQUEST_SIGN_FAILED", err)
	}
	resp, err := c.HTTPClient.Do(req)
	if err != nil {
		// The request may have reached LINE before the connection failed.
		return profileWriteError(ProfileWriteUnknown, "PROFILE_TRANSPORT_UNKNOWN", err)
	}
	defer resp.Body.Close()
	raw, readErr := io.ReadAll(io.LimitReader(resp.Body, profileResponseLimit+1))
	if resp.StatusCode >= 400 && resp.StatusCode < 500 {
		return profileWriteError(ProfileWriteRejected, "PROFILE_HTTP_REJECTED", fmt.Errorf("HTTP %d", resp.StatusCode))
	}
	if resp.StatusCode != http.StatusOK {
		return profileWriteError(ProfileWriteUnknown, "PROFILE_HTTP_UNKNOWN", fmt.Errorf("HTTP %d", resp.StatusCode))
	}
	if readErr != nil || len(raw) > profileResponseLimit {
		return profileWriteError(ProfileWriteUnknown, "PROFILE_RESPONSE_UNREADABLE", readErr)
	}
	// A missing "code" must not decode to 0 and be mistaken for success.
	var wrapper struct {
		Code    *int            `json:"code"`
		Message string          `json:"message"`
		Data    json.RawMessage `json:"data"`
	}
	if err := json.Unmarshal(raw, &wrapper); err != nil || wrapper.Code == nil {
		return profileWriteError(ProfileWriteUnknown, "PROFILE_RESPONSE_INVALID", err)
	}
	if *wrapper.Code != 0 {
		// Keep TalkException details for auth classification by the caller.
		return profileWriteError(ProfileWriteRejected, "PROFILE_LINE_REJECTED",
			fmt.Errorf("updateProfileAttributes failed: code %d message %s data %s", *wrapper.Code, wrapper.Message, string(wrapper.Data)))
	}
	return nil
}

// ProfileImageInfo describes a validated, re-encoded profile image. It never
// contains image bytes.
type ProfileImageInfo struct {
	SourceFormat string
	Width        int
	Height       int
	InputBytes   int
	OutputBytes  int
}

// NormalizeProfileImage accepts only a static JPEG or PNG whose bytes, magic
// number and declared MIME agree. It checks dimensions before decoding, fully
// decodes the image and re-encodes it as a plain JPEG (no EXIF/metadata).
func NormalizeProfileImage(data []byte, declaredMIME string) ([]byte, ProfileImageInfo, error) {
	info := ProfileImageInfo{InputBytes: len(data)}
	invalid := func(reason string) ([]byte, ProfileImageInfo, error) {
		return nil, info, fmt.Errorf("%w: %s", ErrProfileImageInvalid, reason)
	}
	if len(data) == 0 || len(data) > ProfileImageMaxInputBytes {
		return invalid("size out of range")
	}
	var decodeConfig func(io.Reader) (image.Config, error)
	var decode func(io.Reader) (image.Image, error)
	switch {
	case bytes.HasPrefix(data, []byte{0xff, 0xd8, 0xff}):
		info.SourceFormat = "image/jpeg"
		decodeConfig, decode = jpeg.DecodeConfig, jpeg.Decode
	case bytes.HasPrefix(data, []byte("\x89PNG\r\n\x1a\n")):
		info.SourceFormat = "image/png"
		decodeConfig, decode = png.DecodeConfig, png.Decode
		if animated, ok := pngIsAnimated(data); !ok || animated {
			return invalid("animated or malformed PNG")
		}
	default:
		return invalid("not JPEG or PNG")
	}
	if declaredMIME != info.SourceFormat || http.DetectContentType(data) != info.SourceFormat {
		return invalid("declared MIME does not match content")
	}
	config, err := decodeConfig(bytes.NewReader(data))
	if err != nil {
		return invalid("header unreadable")
	}
	if config.Width <= 0 || config.Height <= 0 || config.Width > ProfileImageMaxSide || config.Height > ProfileImageMaxSide ||
		config.Width*config.Height > ProfileImageMaxPixels {
		return invalid("dimensions out of range")
	}
	img, err := decode(bytes.NewReader(data))
	if err != nil {
		return invalid("decode failed")
	}
	bounds := img.Bounds()
	if bounds.Dx() != config.Width || bounds.Dy() != config.Height {
		return invalid("decoded dimensions differ")
	}
	info.Width, info.Height = config.Width, config.Height
	// Flatten transparency onto white so the JPEG has no undefined pixels.
	canvas := image.NewRGBA(image.Rect(0, 0, bounds.Dx(), bounds.Dy()))
	draw.Draw(canvas, canvas.Bounds(), &image.Uniform{C: color.White}, image.Point{}, draw.Src)
	draw.Draw(canvas, canvas.Bounds(), img, bounds.Min, draw.Over)
	var out bytes.Buffer
	if err := jpeg.Encode(&out, canvas, &jpeg.Options{Quality: profileImageJPEGQuality}); err != nil {
		return invalid("encode failed")
	}
	if out.Len() == 0 || out.Len() > profileImageMaxOutput {
		return invalid("encoded size out of range")
	}
	info.OutputBytes = out.Len()
	return out.Bytes(), info, nil
}

// pngIsAnimated walks PNG chunks until IDAT. ok=false means malformed chunks.
func pngIsAnimated(data []byte) (animated bool, ok bool) {
	offset := 8
	for offset+8 <= len(data) {
		length := int(uint32(data[offset])<<24 | uint32(data[offset+1])<<16 | uint32(data[offset+2])<<8 | uint32(data[offset+3]))
		kind := string(data[offset+4 : offset+8])
		if length < 0 || offset+12+length > len(data) {
			return false, false
		}
		switch kind {
		case "acTL", "fcTL", "fdAT":
			return true, true
		case "IDAT":
			return false, true
		}
		offset += 12 + length
	}
	return false, false
}

// ProfileImageUploadResult carries the OBS identifiers returned by LINE.
type ProfileImageUploadResult struct {
	ObjectID string
	Hash     string
}

// UploadProfileImageContext uploads one already normalized JPEG to the
// account's own personal profile path talk/p/{selfMid}. One attempt only; the
// caller must verify the result by reading the profile back.
func (c *Client) UploadProfileImageContext(ctx context.Context, selfMid string, jpegData []byte) (*ProfileImageUploadResult, error) {
	if !profileSelfMIDPattern.MatchString(selfMid) {
		return nil, profileWriteError(ProfileWriteNotSent, "PROFILE_SELF_MID_INVALID", nil)
	}
	if len(jpegData) == 0 || len(jpegData) > profileImageMaxOutput || !bytes.HasPrefix(jpegData, []byte{0xff, 0xd8, 0xff}) {
		return nil, profileWriteError(ProfileWriteNotSent, "PROFILE_IMAGE_INVALID", nil)
	}
	obsToken, err := c.AcquireEncryptedAccessToken()
	if err != nil {
		return nil, profileWriteError(ProfileWriteNotSent, "PROFILE_OBS_TOKEN_UNAVAILABLE", err)
	}
	channelToken, err := c.AcquireChannelAccessToken(ProfileImageChannelID)
	if err != nil {
		return nil, profileWriteError(ProfileWriteNotSent, "PROFILE_CHANNEL_TOKEN_UNAVAILABLE", err)
	}
	params, _ := json.Marshal(map[string]string{"name": "profile.jpg", "type": "image", "ver": "2.0"})
	talkMeta, _ := json.Marshal(map[string]any{"profileContext": map[string]bool{"storyShare": false}})
	requestURL := fmt.Sprintf("%s/r/talk/p/%s", OBSBaseURL, url.PathEscape(selfMid))
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, requestURL, bytes.NewReader(jpegData))
	if err != nil {
		return nil, profileWriteError(ProfileWriteNotSent, "PROFILE_REQUEST_BUILD_FAILED", err)
	}
	req.Header.Set("User-Agent", UserAgent)
	req.Header.Set("x-line-application", lineApplicationHeader)
	req.Header.Set("x-lal", "en_US")
	req.Header.Set("Content-Type", "image/jpeg")
	req.Header.Set("X-Obs-Params", base64.StdEncoding.EncodeToString(params))
	req.Header.Set("X-Talk-Meta", base64.StdEncoding.EncodeToString(talkMeta))
	req.Header.Set("x-line-access", obsToken)
	req.Header.Set("X-Line-ChannelToken", channelToken)

	resp, err := c.obsHTTPClient().Do(req)
	if err != nil {
		return nil, profileWriteError(ProfileWriteUnknown, "PROFILE_OBS_TRANSPORT_UNKNOWN", err)
	}
	defer resp.Body.Close()
	_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 64<<10))
	switch {
	case resp.StatusCode == http.StatusOK || resp.StatusCode == http.StatusCreated:
	case resp.StatusCode >= 400 && resp.StatusCode < 500:
		return nil, profileWriteError(ProfileWriteRejected, "PROFILE_OBS_REJECTED", fmt.Errorf("HTTP %d", resp.StatusCode))
	default:
		return nil, profileWriteError(ProfileWriteUnknown, "PROFILE_OBS_HTTP_UNKNOWN", fmt.Errorf("HTTP %d", resp.StatusCode))
	}
	oid := resp.Header.Get("x-obs-oid")
	hash := resp.Header.Get("x-obs-hash")
	if !profileOBSValue.MatchString(oid) || !profileOBSValue.MatchString(hash) {
		// 2xx without the documented identifiers: LINE may have stored it.
		return nil, profileWriteError(ProfileWriteUnknown, "PROFILE_OBS_RESPONSE_INVALID", nil)
	}
	return &ProfileImageUploadResult{ObjectID: oid, Hash: hash}, nil
}
