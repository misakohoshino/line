package line

// Chrome-extension-native candidate for personal profile cover changes.
// Source: NiceDayZc/OkLine @ 2e3437977368802edcc2219798b9d5333665ebca,
// extracted from LINE Chrome 3.7.2: OBS myhome/c upload, gateway timeline
// updateCover and getCover. This remains subject to real OWNER confirmation.
// No refresh, re-login, redirect following, write retry or token logging.

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"io"
	"net/http"
	"net/url"
	"regexp"

	gen "github.com/highesttt/matrix-line-messenger/pkg"
)

const (
	profileChromeGatewayBaseURL = "https://line-chrome-gw.line-apps.com"
	// Keep this for an unverified cover response contract. No environment
	// switch alone may bypass the connector's separate cover protocol gate.
	ProfileImageProtocolBlocked = "PROFILE_CHROME_IMAGE_PROTOCOL_UNVERIFIED"
)
var profileRegionPattern = regexp.MustCompile(`^[A-Z]{2}$`)

func profileCandidateClient(client *http.Client) *http.Client {
	if client == nil {
		client = &http.Client{}
	}
	copyClient := *client
	copyClient.CheckRedirect = func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }
	return &copyClient
}

// Chrome gateway defaults differ from the old LEGY/OBS headers: notably no
// X-Line-Application, X-Line-Mid or channel token on uploadProfile.
func setChromeProfileGatewayHeaders(req *http.Request, token, channel string) {
	req.Header.Set("User-Agent", UserAgent)
	req.Header.Set("X-Line-Chrome-Version", ExtensionVersion)
	req.Header.Set("X-LAL", "en_US")
	req.Header.Set("X-Line-Access", token)
	req.Header.Set("Accept", "application/json, text/plain, */*")
	if channel != "" {
		req.Header.Set("X-Line-ChannelToken", channel)
	}
}

// A gateway 2xx alone is never accepted as a write. It must have either the
// Chrome {"message":"OK","data":...} envelope or the existing code:0 variant.
// The final outcome still depends on the connector's fresh LINE readback.
func readChromeProfileGatewayResponse(resp *http.Response, prefix string) (json.RawMessage, error) {
	if err := classifyProfileHTTP(resp, prefix); err != nil {
		return nil, err
	}
	raw, readErr := io.ReadAll(io.LimitReader(resp.Body, profileResponseLimit+1))
	if readErr != nil || len(raw) == 0 || len(raw) > profileResponseLimit {
		return nil, profileWriteError(ProfileWriteUnknown, prefix+"_RESPONSE_UNREADABLE", nil)
	}
	var envelope struct {
		Message string          `json:"message"`
		Code    *int            `json:"code"`
		Data    json.RawMessage `json:"data"`
	}
	if json.Unmarshal(raw, &envelope) != nil {
		return nil, profileWriteError(ProfileWriteUnknown, prefix+"_RESPONSE_INVALID", nil)
	}
	if (envelope.Message == "OK" || envelope.Message == "" && envelope.Code != nil && *envelope.Code == 0) &&
		(envelope.Code == nil || *envelope.Code == 0) {
		return envelope.Data, nil
	}
	if envelope.Message == "" && envelope.Code == nil {
		return nil, profileWriteError(ProfileWriteUnknown, prefix+"_RESPONSE_UNVERIFIED", nil)
	}
	return nil, profileWriteError(ProfileWriteRejected, prefix+"_LINE_REJECTED", nil)
}

func classifyProfileHTTP(resp *http.Response, prefix string) error {
	switch {
	case resp.StatusCode == http.StatusOK || resp.StatusCode == http.StatusCreated:
		return nil
	case resp.StatusCode == http.StatusUnauthorized || resp.StatusCode == http.StatusForbidden:
		return profileHTTPError(ProfileWriteRejected, prefix+"_UNAUTHORIZED", resp.StatusCode)
	case resp.StatusCode >= 400 && resp.StatusCode < 500:
		return profileHTTPError(ProfileWriteRejected, prefix+"_REJECTED", resp.StatusCode)
	default:
		return profileHTTPError(ProfileWriteUnknown, prefix+"_HTTP_UNKNOWN", resp.StatusCode)
	}
}

// Only private, fixed /api/timeline/* paths are ever accepted. No untrusted
// URL or host may be supplied. Signing uses the existing Chrome HMAC runner.
func (c *Client) newChromeTimelineRequest(ctx context.Context, method, path string, body []byte, channel string) (*http.Request, error) {
	if c.AccessToken == "" || channel == "" {
		return nil, profileWriteError(ProfileWriteNotSent, "PROFILE_COVER_TOKEN_UNAVAILABLE", nil)
	}
	req, err := http.NewRequestWithContext(ctx, method, profileChromeGatewayBaseURL+path, bytes.NewReader(body))
	if err != nil {
		return nil, profileWriteError(ProfileWriteNotSent, "PROFILE_COVER_REQUEST_INVALID", nil)
	}
	setChromeProfileGatewayHeaders(req, c.AccessToken, channel)
	if method == http.MethodPost {
		req.Header.Set("Content-Type", "application/json")
	}
	req.Header.Set("Cache-Control", "no-cache, no-store")
	runner, err := gen.GetRunner()
	if err != nil {
		return nil, profileWriteError(ProfileWriteNotSent, "PROFILE_COVER_SIGN_UNAVAILABLE", nil)
	}
	signature, err := runner.GetSignature(path, string(body), c.AccessToken)
	if err != nil {
		return nil, profileWriteError(ProfileWriteNotSent, "PROFILE_COVER_SIGN_UNAVAILABLE", nil)
	}
	req.Header.Set("X-Hmac", signature)
	return req, nil
}

// Chrome issueChannelToken, not legacy approveChannelAndIssueChannelToken.
func (c *Client) acquireProfileChannelTokenContext(ctx context.Context) (string, error) {
	if c.AccessToken == "" {
		return "", profileWriteError(ProfileWriteNotSent, "PROFILE_ACCESS_TOKEN_UNAVAILABLE", nil)
	}
	body, _ := json.Marshal([]string{ProfileImageChannelID})
	req, err := c.newSignedRPCRequest(ctx, BaseURL, "ChannelService", "issueChannelToken", body)
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
		Message string `json:"message"`
		Data struct { Token string `json:"channelAccessToken"` } `json:"data"`
	}
	if resp.StatusCode != http.StatusOK || err != nil || len(raw) > profileResponseLimit ||
		json.Unmarshal(raw, &wrapper) != nil || wrapper.Data.Token == "" ||
		(wrapper.Code != nil && *wrapper.Code != 0) ||
		(wrapper.Code == nil && wrapper.Message != "OK") {
		return "", profileWriteError(ProfileWriteNotSent, "PROFILE_CHANNEL_TOKEN_UNAVAILABLE", nil)
	}
	return wrapper.Data.Token, nil
}

func (c *Client) profileCoverSelfRegion(ctx context.Context, mid string) (string, error) {
	if !profileSelfMIDPattern.MatchString(mid) {
		return "", profileWriteError(ProfileWriteNotSent, "PROFILE_SELF_MID_INVALID", nil)
	}
	profile, err := c.GetProfileContext(ctx)
	if err != nil || profile == nil || profile.Mid != mid || !profileRegionPattern.MatchString(profile.RegionCode) {
		return "", profileWriteError(ProfileWriteNotSent, "PROFILE_COVER_SELF_REGION_UNAVAILABLE", nil)
	}
	return profile.RegionCode, nil
}

// The read target and actor are always the same account, and region comes from
// a fresh authenticated self profile, never caller input.
func (c *Client) profileCoverRequest(ctx context.Context, mid, region, channel, objectID string) (json.RawMessage, error) {
	var (
		method = http.MethodGet
		path string
		body []byte
	)
	if objectID == "" {
		path = "/api/timeline/getCover?" + url.Values{
			"targetMid": {mid}, "myMid": {mid}, "myRegion": {region},
		}.Encode()
	} else {
		method = http.MethodPost
		path = "/api/timeline/updateCover"
		body, _ = json.Marshal(map[string]any{
			"myMid": mid, "myRegion": region, "coverObjectId": objectID, "storyShare": false,
		})
	}
	req, err := c.newChromeTimelineRequest(ctx, method, path, body, channel)
	if err != nil {
		return nil, err
	}
	resp, err := profileCandidateClient(c.HTTPClient).Do(req)
	if err != nil {
		return nil, profileWriteError(ProfileWriteUnknown, "PROFILE_COVER_TRANSPORT_UNKNOWN", nil)
	}
	defer resp.Body.Close()
	return readChromeProfileGatewayResponse(resp, "PROFILE_COVER")
}

// getCover gateway data is not guaranteed to echo homeId. Require a fresh
// authenticated self-only request; if the response does echo identity, it must
// agree exactly. Unknown schemas remain unconfirmed, never guessed.
func parseProfileCover(raw []byte, mid string) (string, error) {
	var obj struct {
		HomeID string `json:"homeId"`
		Cover struct {
			Service string `json:"serviceName"`
			Namespace string `json:"obsNamespace"`
			ObjectID string `json:"objectId"`
		} `json:"coverObsInfo"`
	}
	if json.Unmarshal(raw, &obj) != nil {
		return "", profileWriteError(ProfileWriteUnknown, "PROFILE_COVER_RESPONSE_INVALID", nil)
	}
	if obj.HomeID != "" && obj.HomeID != mid {
		return "", profileWriteError(ProfileWriteUnknown, "PROFILE_COVER_SELF_READBACK_UNVERIFIED", nil)
	}
	if obj.Cover.Service != "" && obj.Cover.Service != "myhome" ||
		obj.Cover.Namespace != "" && obj.Cover.Namespace != "c" ||
		!profileOBSValue.MatchString(obj.Cover.ObjectID) {
		return "", profileWriteError(ProfileWriteUnknown, "PROFILE_COVER_SELF_READBACK_UNVERIFIED", nil)
	}
	return obj.Cover.ObjectID, nil
}

func (c *Client) ReadProfileCoverContext(ctx context.Context, mid string) (string, error) {
	region, err := c.profileCoverSelfRegion(ctx, mid)
	if err != nil { return "", err }
	channel, err := c.acquireProfileChannelTokenContext(ctx)
	if err != nil { return "", err }
	raw, err := c.profileCoverRequest(ctx, mid, region, channel, "")
	if err != nil { return "", err }
	return parseProfileCover(raw, mid)
}

func (c *Client) UploadProfileCoverContext(ctx context.Context, mid string, jpeg []byte) (*ProfileImageUploadResult, error) {
	if len(jpeg) == 0 || len(jpeg) > profileImageMaxOutput || !bytes.HasPrefix(jpeg, []byte{0xff, 0xd8, 0xff}) {
		return nil, profileWriteError(ProfileWriteNotSent, "PROFILE_IMAGE_INVALID", nil)
	}
	region, err := c.profileCoverSelfRegion(ctx, mid)
	if err != nil { return nil, err }
	channel, err := c.acquireProfileChannelTokenContext(ctx)
	if err != nil { return nil, err }
	var random [16]byte
	if _, err = rand.Read(random[:]); err != nil {
		return nil, profileWriteError(ProfileWriteNotSent, "PROFILE_COVER_OBJECT_ID_UNAVAILABLE", nil)
	}
	objectID := hex.EncodeToString(random[:])
	params, _ := json.Marshal(map[string]string{"name": objectID+".jpg", "type": "image", "ver": "2.0"})
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, OBSBaseURL+"/r/myhome/c/"+objectID, bytes.NewReader(jpeg))
	if err != nil { return nil, profileWriteError(ProfileWriteNotSent, "PROFILE_COVER_UPLOAD_REQUEST_INVALID", nil) }
	req.Header.Set("User-Agent", UserAgent)
	req.Header.Set("X-Line-Chrome-Version", ExtensionVersion)
	req.Header.Set("Accept-Language", "en-US")
	req.Header.Set("X-Line-ChannelToken", channel)
	req.Header.Set("X-Obs-Params", base64.StdEncoding.EncodeToString(params))
	// Chrome's raw myhome OBS upload has no main access token or Content-Type.
	resp, err := profileCandidateClient(c.obsHTTPClient()).Do(req)
	if err != nil { return nil, profileWriteError(ProfileWriteUnknown, "PROFILE_COVER_UPLOAD_TRANSPORT_UNKNOWN", nil) }
	defer resp.Body.Close()
	_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, profileResponseLimit))
	if err := classifyProfileHTTP(resp, "PROFILE_COVER_UPLOAD"); err != nil { return nil, err }
	// A 2xx upload is not evidence of a changed cover. The ID was generated
	// locally and will only be confirmed after timeline association + readback.
	if _, err := c.profileCoverRequest(ctx, mid, region, channel, objectID); err != nil {
		return nil, err
	}
	return &ProfileImageUploadResult{ObjectID: objectID}, nil
}
