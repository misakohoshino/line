package line

// Candidate port of DeachSword/CHRLINE @ 3ee320668e2da6fc04b50e2054b880e86d23434d:
// object.py (CHROMEOS headers, /oa/r uploads), timeline.py (cover association),
// helpers.py (result.coverObsInfo). Not a verified production Chrome contract.
// In particular the upstream uses a placeholder extension Origin and a legacy
// approval RPC. We do NOT invent an Origin, region, refresh or device login.
// The connector therefore exposes these operations as BLOCKED, not ready.

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"io"
	"net/http"
	"net/url"
	"strconv"
)

const profileLEGYBaseURL = "https://gwz.line.naver.jp"
const profileHomeBaseURL = "https://ga2.line.naver.jp"
const ProfileImageProtocolBlocked = "PROFILE_CHROME_IMAGE_PROTOCOL_UNVERIFIED"

// Cloning the client preserves test transports/timeouts, and prevents token
// leakage or additional writes on redirects. No retries and no token refresh.
func profileCandidateClient(client *http.Client) *http.Client {
	copyClient := *client
	copyClient.CheckRedirect = func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }
	return &copyClient
}

func setProfileCandidateHeaders(req *http.Request, token, mid, channel string, home bool) {
	req.Header.Set("User-Agent", UserAgent)
	req.Header.Set("X-Line-Application", lineApplicationHeader)
	req.Header.Set("X-Line-Access", token)
	req.Header.Set("X-Line-Mid", mid)
	req.Header.Set("X-Line-ChannelToken", channel)
	req.Header.Set("x-lal", "en_US")
	if home {
		req.Header.Set("X-LAP", "5")
		req.Header.Set("X-LPV", "1")
		req.Header.Set("x-line-bdbtemplateversion", "v1")
		req.Header.Set("x-line-global-config", "discover.enable=true; follow.enable=true")
	}
}

func classifyProfileHTTP(resp *http.Response, prefix string) error {
	status := resp.StatusCode
	if status == 200 || status == 201 {
		// CHRLINE checks the application status as well as the HTTP status.
		if raw := resp.Header.Get("x-lc"); raw != "" {
			code, err := strconv.Atoi(raw)
			if err != nil {
				return profileWriteError(ProfileWriteUnknown, prefix+"_RESPONSE_INVALID", nil)
			}
			if code != 200 && code != 201 {
				status = code
			} else {
				return nil
			}
		} else {
			return nil
		}
	}
	if status == 401 || status == 403 {
		return profileHTTPError(ProfileWriteRejected, prefix+"_UNAUTHORIZED", status)
	}
	if status >= 400 && status < 500 {
		return profileHTTPError(ProfileWriteRejected, prefix+"_REJECTED", status)
	}
	return profileHTTPError(ProfileWriteUnknown, prefix+"_HTTP_UNKNOWN", status)
}

// Unlike the old issueChannelToken path, the reference explicitly approves
// the channel. Use the existing Chrome signer; never copy binary Thrift or
// another device's token into a Chrome request. Gateway support is unverified.
func (c *Client) acquireProfileChannelTokenContext(ctx context.Context) (string, error) {
	if c.AccessToken == "" {
		return "", profileWriteError(ProfileWriteNotSent, "PROFILE_ACCESS_TOKEN_UNAVAILABLE", nil)
	}
	body, _ := json.Marshal([]string{ProfileImageChannelID})
	req, err := c.newSignedRPCRequest(ctx, BaseURL, "ChannelService", "approveChannelAndIssueChannelToken", body)
	if err != nil {
		return "", profileWriteError(ProfileWriteNotSent, "PROFILE_CHANNEL_REQUEST_UNAVAILABLE", nil)
	}
	resp, err := profileCandidateClient(c.HTTPClient).Do(req)
	if err != nil {
		return "", profileWriteError(ProfileWriteNotSent, "PROFILE_CHANNEL_TOKEN_UNAVAILABLE", nil)
	}
	defer resp.Body.Close()
	raw, err := io.ReadAll(io.LimitReader(resp.Body, profileResponseLimit+1))
	var wrapper struct {
		Code *int `json:"code"`
		Data struct {
			Token string `json:"channelAccessToken"`
		} `json:"data"`
	}
	if resp.StatusCode != 200 || err != nil || len(raw) > profileResponseLimit || json.Unmarshal(raw, &wrapper) != nil || wrapper.Code == nil || *wrapper.Code != 0 || wrapper.Data.Token == "" {
		return "", profileWriteError(ProfileWriteNotSent, "PROFILE_CHANNEL_TOKEN_UNAVAILABLE", nil)
	}
	return wrapper.Data.Token, nil
}

// The same bounded candidate is used for both association and fresh reads.
// GET is tunneled as POST with X-LHM=GET, matching CHRLINE (no HTTP cache).
func (c *Client) profileCoverRequest(ctx context.Context, mid, channel string, body []byte) ([]byte, error) {
	target := profileHomeBaseURL + "/hm/api/v1/home/cover.json"
	method := "POST"
	if body == nil {
		target += "?" + url.Values{"homeId": {mid}}.Encode()
		method = "GET"
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, target, bytes.NewReader(body))
	if err != nil {
		return nil, profileWriteError(ProfileWriteNotSent, "PROFILE_COVER_REQUEST_INVALID", nil)
	}
	setProfileCandidateHeaders(req, c.AccessToken, mid, channel, true)
	req.Header.Set("X-LHM", method)
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Cache-Control", "no-cache, no-store")
	resp, err := profileCandidateClient(c.obsHTTPClient()).Do(req)
	if err != nil {
		return nil, profileWriteError(ProfileWriteUnknown, "PROFILE_COVER_TRANSPORT_UNKNOWN", nil)
	}
	defer resp.Body.Close()
	if err := classifyProfileHTTP(resp, "PROFILE_COVER"); err != nil {
		return nil, err
	}
	raw, err := io.ReadAll(io.LimitReader(resp.Body, profileResponseLimit+1))
	if err != nil || len(raw) > profileResponseLimit {
		return nil, profileWriteError(ProfileWriteUnknown, "PROFILE_COVER_RESPONSE_UNREADABLE", nil)
	}
	return raw, nil
}

func parseProfileCover(raw []byte, mid string) (string, error) {
	var wrapper struct {
		Code   *int `json:"code"`
		Result struct {
			HomeID string `json:"homeId"`
			Cover  struct {
				Service   string `json:"serviceName"`
				Namespace string `json:"obsNamespace"`
				ObjectID  string `json:"objectId"`
			} `json:"coverObsInfo"`
		} `json:"result"`
	}
	if json.Unmarshal(raw, &wrapper) != nil || wrapper.Code == nil {
		return "", profileWriteError(ProfileWriteUnknown, "PROFILE_COVER_RESPONSE_INVALID", nil)
	}
	if *wrapper.Code != 0 {
		return "", profileWriteError(ProfileWriteRejected, "PROFILE_COVER_LINE_REJECTED", nil)
	}
	cover := wrapper.Result.Cover
	// A missing identity is not inferred. Tighten only on the basis of an
	// actual Chrome response contract, never by guessing undocumented keys.
	if wrapper.Result.HomeID != mid || cover.Service != "myhome" || cover.Namespace != "c" || !profileOBSValue.MatchString(cover.ObjectID) {
		return "", profileWriteError(ProfileWriteUnknown, "PROFILE_COVER_SELF_READBACK_UNVERIFIED", nil)
	}
	return cover.ObjectID, nil
}

func (c *Client) ReadProfileCoverContext(ctx context.Context, mid string) (string, error) {
	if !profileSelfMIDPattern.MatchString(mid) {
		return "", profileWriteError(ProfileWriteNotSent, "PROFILE_SELF_MID_INVALID", nil)
	}
	channel, err := c.acquireProfileChannelTokenContext(ctx)
	if err != nil {
		return "", err
	}
	raw, err := c.profileCoverRequest(ctx, mid, channel, nil)
	if err != nil {
		return "", err
	}
	return parseProfileCover(raw, mid)
}

func (c *Client) UploadProfileCoverContext(ctx context.Context, mid string, jpeg []byte) (*ProfileImageUploadResult, error) {
	var random [16]byte
	if _, err := rand.Read(random[:]); err != nil {
		return nil, profileWriteError(ProfileWriteNotSent, "PROFILE_COVER_OBJECT_ID_UNAVAILABLE", nil)
	}
	objectID := hex.EncodeToString(random[:])
	uploaded, err := c.uploadProfileObjectContext(ctx, mid, jpeg, "myhome/c/"+objectID, objectID+".jpg", false)
	if err != nil {
		return nil, err
	}
	// This identifier is for association, not proof of a changed cover.
	channel := uploaded.channelToken
	if channel == "" {
		return nil, profileWriteError(ProfileWriteUnknown, "PROFILE_COVER_ASSOCIATION_NOT_ATTEMPTED", nil)
	}
	body, _ := json.Marshal(map[string]any{"homeId": mid, "coverObjectId": uploaded.ObjectID, "storyShare": false, "meta": map[string]any{}})
	raw, err := c.profileCoverRequest(ctx, mid, channel, body)
	if err != nil {
		return nil, err
	}
	var wrapper struct {
		Code *int `json:"code"`
	}
	if json.Unmarshal(raw, &wrapper) != nil || wrapper.Code == nil {
		return nil, profileWriteError(ProfileWriteUnknown, "PROFILE_COVER_ASSOCIATION_UNCONFIRMED", nil)
	}
	if *wrapper.Code != 0 {
		return nil, profileWriteError(ProfileWriteRejected, "PROFILE_COVER_ASSOCIATION_REJECTED", nil)
	}
	return uploaded, nil // connector must freshly read the matching self cover.
}
