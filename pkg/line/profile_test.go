package line

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/binary"
	"errors"
	"hash/crc32"
	"image"
	"image/color"
	"image/gif"
	"image/jpeg"
	"image/png"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"
)

func profileRPCClient(t *testing.T, status int, response string, err error, body *string, calls *int) *Client {
	t.Helper()
	client := NewClient("test-token")
	client.HTTPClient = &http.Client{Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
		*calls++
		if req.URL.Path != "/api/talk/thrift/Talk/TalkService/updateProfileAttributes" || req.Header.Get("x-hmac") == "" {
			t.Fatalf("unexpected request %s hmac=%q", req.URL.Path, req.Header.Get("x-hmac"))
		}
		data, _ := io.ReadAll(req.Body)
		if body != nil {
			*body = string(data)
		}
		if err != nil {
			return nil, err
		}
		return &http.Response{StatusCode: status, Header: make(http.Header), Body: io.NopCloser(strings.NewReader(response))}, nil
	})}
	return client
}

func TestUpdateProfileAttributeSparseGoldenBodies(t *testing.T) {
	for _, tc := range []struct {
		attribute int
		value     string
		want      string
	}{
		{ProfileAttributeDisplayName, "新名字😀", `[42,{"profileAttributes":{"2":{"meta":{},"value":"新名字😀"}}}]`},
		{ProfileAttributeStatusMessage, "第一行\n第二行", `[43,{"profileAttributes":{"16":{"meta":{},"value":"第一行\n第二行"}}}]`},
	} {
		var body string
		calls := 0
		client := profileRPCClient(t, 200, `{"code":0,"message":"OK","data":{}}`, nil, &body, &calls)
		reqSeq := int64(42)
		if tc.attribute == ProfileAttributeStatusMessage {
			reqSeq = 43
		}
		if err := client.UpdateProfileAttributeContext(context.Background(), reqSeq, tc.attribute, tc.value); err != nil {
			t.Fatal(err)
		}
		if body != tc.want || calls != 1 {
			t.Fatalf("body=%s calls=%d want %s", body, calls, tc.want)
		}
	}
}

func TestUpdateProfileAttributeClassifiesResponses(t *testing.T) {
	for _, tc := range []struct {
		name     string
		status   int
		response string
		err      error
		outcome  ProfileWriteOutcome
	}{
		{"empty json is not success", 200, `{}`, nil, ProfileWriteUnknown},
		{"missing code", 200, `{"data":{}}`, nil, ProfileWriteUnknown},
		{"not json", 200, `<html>`, nil, ProfileWriteUnknown},
		{"empty body", 200, ``, nil, ProfileWriteUnknown},
		{"talk exception", 200, `{"code":20,"message":"RESPONSE_ERROR","data":{"name":"TalkException","code":20}}`, nil, ProfileWriteRejected},
		{"http 400", 400, `bad`, nil, ProfileWriteRejected},
		{"http 502", 502, `gateway`, nil, ProfileWriteUnknown},
		{"timeout", 0, ``, context.DeadlineExceeded, ProfileWriteUnknown},
	} {
		calls := 0
		client := profileRPCClient(t, tc.status, tc.response, tc.err, nil, &calls)
		err := client.UpdateProfileAttributeContext(context.Background(), 1, ProfileAttributeDisplayName, "名字")
		outcome, code := ProfileWriteOutcomeOf(err)
		if err == nil || outcome != tc.outcome || code == "" || calls != 1 {
			t.Fatalf("%s: err=%v outcome=%s code=%s calls=%d", tc.name, err, outcome, code, calls)
		}
	}
	calls := 0
	client := profileRPCClient(t, 200, `{"code":10051,"message":"RESPONSE_ERROR","data":{"name":"TalkException","code":119,"reason":"Access token refresh required"}}`, nil, nil, &calls)
	err := client.UpdateProfileAttributeContext(context.Background(), 1, ProfileAttributeDisplayName, "名字")
	if !IsAuthError(err) {
		t.Fatalf("auth details must stay classifiable: %v", err)
	}
}

func TestUpdateProfileAttributeRejectsBeforeSending(t *testing.T) {
	long := strings.Repeat("名", ProfileDisplayNameMaxUnits+1)
	emoji := strings.Repeat("😀", ProfileDisplayNameMaxUnits/2+1) // 2 UTF-16 units each
	for _, tc := range []struct {
		attribute int
		value     string
	}{
		{4, "picture"}, {8, "picture"}, {0, "x"},
		{ProfileAttributeDisplayName, ""}, {ProfileAttributeDisplayName, " 名字"}, {ProfileAttributeDisplayName, "名字　"},
		{ProfileAttributeDisplayName, "名\n字"}, {ProfileAttributeDisplayName, "名\x00字"}, {ProfileAttributeDisplayName, "名‮字"},
		{ProfileAttributeDisplayName, long}, {ProfileAttributeDisplayName, emoji}, {ProfileAttributeDisplayName, "bad\xff"},
		{ProfileAttributeStatusMessage, strings.Repeat("a", ProfileStatusMessageMaxUnits+1)}, {ProfileAttributeStatusMessage, "a\tb"},
		{ProfileAttributeStatusMessage, "a b"},
	} {
		calls := 0
		client := profileRPCClient(t, 200, `{"code":0}`, nil, nil, &calls)
		err := client.UpdateProfileAttributeContext(context.Background(), 1, tc.attribute, tc.value)
		if outcome, _ := ProfileWriteOutcomeOf(err); outcome != ProfileWriteNotSent || calls != 0 {
			t.Fatalf("attr=%d value=%q outcome=%s calls=%d", tc.attribute, tc.value, outcome, calls)
		}
	}
	if ValidateProfileText(ProfileAttributeDisplayName, strings.Repeat("名", ProfileDisplayNameMaxUnits)) != nil ||
		ValidateProfileText(ProfileAttributeStatusMessage, "多行\n簽名 😀") != nil {
		t.Fatal("valid boundary values rejected")
	}
	calls := 0
	client := profileRPCClient(t, 200, `{"code":0}`, nil, nil, &calls)
	if outcome, _ := ProfileWriteOutcomeOf(client.UpdateProfileAttributeContext(context.Background(), 0, 2, "名字")); outcome != ProfileWriteNotSent || calls != 0 {
		t.Fatal("invalid reqSeq was sent")
	}
}

func encodeTestImage(t *testing.T, format string, width, height int) []byte {
	t.Helper()
	img := image.NewNRGBA(image.Rect(0, 0, width, height))
	for y := 0; y < height; y++ {
		for x := 0; x < width; x++ {
			img.Set(x, y, color.NRGBA{R: uint8(x), G: uint8(y), B: 128, A: uint8(64 + x%128)})
		}
	}
	var out bytes.Buffer
	var err error
	switch format {
	case "png":
		err = png.Encode(&out, img)
	case "jpeg":
		err = jpeg.Encode(&out, img, nil)
	case "gif":
		err = gif.Encode(&out, img, nil)
	}
	if err != nil {
		t.Fatal(err)
	}
	return out.Bytes()
}

func pngChunk(kind string, data []byte) []byte {
	var out bytes.Buffer
	_ = binary.Write(&out, binary.BigEndian, uint32(len(data)))
	out.WriteString(kind)
	out.Write(data)
	crc := crc32.ChecksumIEEE(append([]byte(kind), data...))
	_ = binary.Write(&out, binary.BigEndian, crc)
	return out.Bytes()
}

func TestNormalizeProfileImageAcceptsStaticJPEGAndPNG(t *testing.T) {
	for format, mime := range map[string]string{"png": "image/png", "jpeg": "image/jpeg"} {
		input := encodeTestImage(t, format, 64, 48)
		output, info, err := NormalizeProfileImage(input, mime)
		if err != nil || info.Width != 64 || info.Height != 48 || info.SourceFormat != mime || info.OutputBytes != len(output) {
			t.Fatalf("%s: err=%v info=%+v", format, err, info)
		}
		decoded, err := jpeg.Decode(bytes.NewReader(output))
		if err != nil || decoded.Bounds().Dx() != 64 || bytes.Contains(output, []byte("Exif")) {
			t.Fatalf("%s: output is not a clean JPEG: %v", format, err)
		}
	}
}

func TestNormalizeProfileImageRejectsUnsafeInputs(t *testing.T) {
	pngData := encodeTestImage(t, "png", 16, 16)
	jpegData := encodeTestImage(t, "jpeg", 16, 16)
	// IHDR claims 5000x5000; the decoder must never allocate it.
	huge := append([]byte("\x89PNG\r\n\x1a\n"), pngChunk("IHDR", []byte{0, 0, 0x13, 0x88, 0, 0, 0x13, 0x88, 8, 6, 0, 0, 0})...)
	huge = append(huge, pngChunk("IDAT", []byte{0})...)
	tallThin := append([]byte("\x89PNG\r\n\x1a\n"), pngChunk("IHDR", []byte{0, 0, 0, 1, 0, 0, 0x20, 0x01, 8, 6, 0, 0, 0})...)
	tallThin = append(tallThin, pngChunk("IDAT", []byte{0})...)
	ihdrEnd := 8 + 25
	apng := append(append(append([]byte{}, pngData[:ihdrEnd]...), pngChunk("acTL", []byte{0, 0, 0, 2, 0, 0, 0, 0})...), pngData[ihdrEnd:]...)
	for name, tc := range map[string]struct {
		data []byte
		mime string
	}{
		"empty":           {nil, "image/png"},
		"too large":       {make([]byte, ProfileImageMaxInputBytes+1), "image/png"},
		"gif":             {encodeTestImage(t, "gif", 8, 8), "image/gif"},
		"fake png mime":   {jpegData, "image/png"},
		"fake jpeg mime":  {pngData, "image/jpeg"},
		"text as png":     {[]byte("\x89PNG\r\n\x1a\nnot really"), "image/png"},
		"truncated png":   {pngData[:len(pngData)/2], "image/png"},
		"truncated jpeg":  {jpegData[:len(jpegData)/2], "image/jpeg"},
		"huge pixels":     {huge, "image/png"},
		"over side limit": {tallThin, "image/png"},
		"animated png":    {apng, "image/png"},
		"html":            {[]byte("<html><img src=x></html>"), "image/png"},
	} {
		if _, _, err := NormalizeProfileImage(tc.data, tc.mime); !errors.Is(err, ErrProfileImageInvalid) {
			t.Fatalf("%s accepted: %v", name, err)
		}
	}
}

func installProfileChannelToken(client *Client) {
	client.channelTokenCache = map[string]cachedChannelAccessToken{
		ProfileImageChannelID: {token: "channel-token", expiresAt: time.Now().Add(time.Hour)},
	}
}

func TestUploadProfileImageUsesPersonalPathAndStrictResponse(t *testing.T) {
	installCachedOBSToken(t)
	jpegData := encodeTestImage(t, "jpeg", 8, 8)
	for _, tc := range []struct {
		name    string
		status  int
		headers map[string]string
		err     error
		outcome ProfileWriteOutcome
		ok      bool
	}{
		{"created", 201, map[string]string{"x-obs-oid": "oid_1", "x-obs-hash": "hash-1"}, nil, "", true},
		{"cached", 200, map[string]string{"x-obs-oid": "oid_1", "x-obs-hash": "hash-1"}, nil, "", true},
		{"200 without identifiers", 200, nil, nil, ProfileWriteUnknown, false},
		{"200 without hash", 200, map[string]string{"x-obs-oid": "oid_1"}, nil, ProfileWriteUnknown, false},
		{"bad identifier", 201, map[string]string{"x-obs-oid": "../x", "x-obs-hash": "h"}, nil, ProfileWriteUnknown, false},
		{"forbidden", 403, nil, nil, ProfileWriteRejected, false},
		{"server error", 500, nil, nil, ProfileWriteUnknown, false},
		{"timeout", 0, nil, context.DeadlineExceeded, ProfileWriteUnknown, false},
	} {
		requests := 0
		client := NewClient("main-token")
		installProfileChannelToken(client)
		client.OBSClient = &http.Client{Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
			requests++
			if req.Method != http.MethodPost || req.URL.Host != "obs.line-apps.com" || req.URL.Path != "/r/talk/p/UAccount_fake-90" {
				t.Fatalf("unexpected upload target %s %s", req.Method, req.URL)
			}
			params, _ := base64.StdEncoding.DecodeString(req.Header.Get("X-Obs-Params"))
			meta, _ := base64.StdEncoding.DecodeString(req.Header.Get("X-Talk-Meta"))
			if string(params) != `{"name":"profile.jpg","type":"image","ver":"2.0"}` ||
				string(meta) != `{"profileContext":{"storyShare":false}}` ||
				req.Header.Get("X-Line-ChannelToken") != "channel-token" || req.Header.Get("x-line-access") != "obs-token" ||
				req.Header.Get("Content-Type") != "image/jpeg" {
				t.Fatalf("unexpected profile upload headers: %v params=%s meta=%s", req.Header, params, meta)
			}
			body, _ := io.ReadAll(req.Body)
			if !bytes.Equal(body, jpegData) {
				t.Fatal("upload body changed")
			}
			if tc.err != nil {
				return nil, tc.err
			}
			resp := obsResponse(tc.status, "")
			for k, v := range tc.headers {
				resp.Header.Set(k, v)
			}
			return resp, nil
		})}
		result, err := client.UploadProfileImageContext(context.Background(), "UAccount_fake-90", jpegData)
		if requests != 1 {
			t.Fatalf("%s: requests=%d (no retries allowed)", tc.name, requests)
		}
		if tc.ok {
			if err != nil || result.ObjectID != "oid_1" || result.Hash != "hash-1" {
				t.Fatalf("%s: %v %+v", tc.name, err, result)
			}
			continue
		}
		if outcome, _ := ProfileWriteOutcomeOf(err); err == nil || outcome != tc.outcome {
			t.Fatalf("%s: err=%v outcome=%s", tc.name, err, outcome)
		}
	}
}

func TestUploadProfileImageRefusesOtherTargetsBeforeNetwork(t *testing.T) {
	installCachedOBSToken(t)
	client := NewClient("main-token")
	installProfileChannelToken(client)
	client.OBSClient = &http.Client{Transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
		t.Fatal("must not reach OBS")
		return nil, nil
	})}
	jpegData := encodeTestImage(t, "jpeg", 8, 8)
	for _, tc := range []struct {
		mid  string
		data []byte
	}{
		{"cGroup", jpegData}, {"u/../m/x", jpegData}, {"", jpegData},
		{"UAccount", encodeTestImage(t, "png", 8, 8)}, {"UAccount", nil},
	} {
		_, err := client.UploadProfileImageContext(context.Background(), tc.mid, tc.data)
		if outcome, _ := ProfileWriteOutcomeOf(err); outcome != ProfileWriteNotSent {
			t.Fatalf("mid=%q outcome=%s", tc.mid, outcome)
		}
	}
}
