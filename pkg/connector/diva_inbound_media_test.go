package connector

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/highesttt/matrix-line-messenger/pkg/connector/handlers"
	"github.com/highesttt/matrix-line-messenger/pkg/line"
)

type divaReceivedInbound struct {
	contentType string
	metadata    []byte
	media       []byte
	mediaType   string
	mediaName   string
}

func startDIVAInboundMediaWorker(t *testing.T) <-chan divaReceivedInbound {
	t.Helper()
	received := make(chan divaReceivedInbound, 4)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		got := divaReceivedInbound{contentType: r.Header.Get("Content-Type")}
		if strings.HasPrefix(got.contentType, "multipart/form-data") {
			mr, err := r.MultipartReader()
			if err != nil {
				t.Errorf("MultipartReader: %v", err)
				w.WriteHeader(http.StatusBadRequest)
				return
			}
			metaPart, err := mr.NextPart()
			if err != nil || metaPart.FormName() != "metadata" {
				t.Errorf("metadata part: part=%v err=%v", metaPart, err)
				w.WriteHeader(http.StatusBadRequest)
				return
			}
			got.metadata, _ = io.ReadAll(metaPart)
			_ = metaPart.Close()

			mediaPart, err := mr.NextPart()
			if err != nil || mediaPart.FormName() != "media" {
				t.Errorf("media part: part=%v err=%v", mediaPart, err)
				w.WriteHeader(http.StatusBadRequest)
				return
			}
			got.mediaType = mediaPart.Header.Get("Content-Type")
			got.mediaName = mediaPart.FileName()
			got.media, _ = io.ReadAll(mediaPart)
			_ = mediaPart.Close()
			if extra, err := mr.NextPart(); err == nil {
				_ = extra.Close()
				t.Error("unexpected third multipart part")
			}
		} else {
			got.metadata, _ = io.ReadAll(r.Body)
		}
		received <- got
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"ok":true}`))
	}))
	t.Cleanup(srv.Close)
	t.Setenv("DIVA_WEBHOOK_URL", srv.URL+"/line/inbound")
	t.Setenv("DIVA_CONTRACT_VERSION", "2")
	return received
}

func receiveDIVAInbound(t *testing.T, received <-chan divaReceivedInbound) divaReceivedInbound {
	t.Helper()
	select {
	case got := <-received:
		return got
	case <-time.After(3 * time.Second):
		t.Fatal("DIVA inbound request not received")
		return divaReceivedInbound{}
	}
}

func divaInboundImageMessage(id string) *line.Message {
	return &line.Message{
		ID: id, From: "usender", To: "cgroup", ToType: int(ToGroup),
		ContentType: int(ContentImage),
		ContentMetadata: map[string]string{
			"FILE_NAME": "car.png",
			"FILE_SIZE": "16",
			"ENC_KM":    "must-never-leak",
			"OID":       "must-never-leak-oid",
		},
	}
}

func TestDIVAInboundLiveImageUsesMultipart(t *testing.T) {
	received := startDIVAInboundMediaWorker(t)
	lc := newDIVAV2TestClient(io.Discard)
	image := []byte("\x89PNG\r\n\x1a\nimage")

	oldPrepare := divaPrepareInboundMedia
	divaPrepareInboundMedia = func(_ *LineClient, _ context.Context, kind handlers.MediaKind, _ line.Message, decryptedBody string) (*divaInboundMedia, error) {
		if !strings.Contains(decryptedBody, "keyMaterial") {
			t.Fatalf("decrypted body = %q", decryptedBody)
		}
		if kind != handlers.MediaKindImage {
			t.Fatalf("kind = %q, want image", kind)
		}
		return &divaInboundMedia{Data: image, MimeType: "image/png", FileName: "car.png"}, nil
	}
	t.Cleanup(func() { divaPrepareInboundMedia = oldPrepare })

	lc.handleDIVAInbound(
		divaInboundImageMessage("600000000000000201"),
		"cgroup",
		`{"keyMaterial":"secret-key"}`,
		false,
		int(OpReceiveMessage),
		divaOriginLive,
	)

	got := receiveDIVAInbound(t, received)
	if !strings.HasPrefix(got.contentType, "multipart/form-data") {
		t.Fatalf("Content-Type = %q", got.contentType)
	}
	if got.mediaType != "image/png" || !bytes.Equal(got.media, image) {
		t.Fatalf("media type=%q bytes=%q", got.mediaType, got.media)
	}
	var event map[string]any
	if err := json.Unmarshal(got.metadata, &event); err != nil {
		t.Fatalf("metadata JSON: %v", err)
	}
	content, _ := event["content"].(map[string]any)
	if content["type"] != "image" || event["origin"] != "live" {
		t.Fatalf("event = %v", event)
	}
	for _, forbidden := range []string{"secret-key", "keyMaterial", "must-never-leak", "must-never-leak-oid", "ENC_KM", "OID"} {
		if bytes.Contains(got.metadata, []byte(forbidden)) {
			t.Fatalf("metadata leaked %q: %s", forbidden, got.metadata)
		}
	}
}

func TestDIVAInboundBackfillImageStaysDescriptorOnly(t *testing.T) {
	received := startDIVAInboundMediaWorker(t)
	lc := newDIVAV2TestClient(io.Discard)
	var prepareCalls atomic.Int32

	oldPrepare := divaPrepareInboundMedia
	divaPrepareInboundMedia = func(_ *LineClient, _ context.Context, kind handlers.MediaKind, _ line.Message, _ string) (*divaInboundMedia, error) {
		prepareCalls.Add(1)
		return &divaInboundMedia{Data: []byte("should-not-happen"), MimeType: "image/png", FileName: "old.png"}, nil
	}
	t.Cleanup(func() { divaPrepareInboundMedia = oldPrepare })

	lc.handleDIVAInbound(
		divaInboundImageMessage("600000000000000202"),
		"cgroup",
		`{"keyMaterial":"secret-key"}`,
		false,
		int(OpReceiveMessage),
		divaOriginBackfill,
	)

	got := receiveDIVAInbound(t, received)
	if got.contentType != "application/json" || len(got.media) != 0 {
		t.Fatalf("backfill contentType=%q media=%d", got.contentType, len(got.media))
	}
	if prepareCalls.Load() != 0 {
		t.Fatalf("media prepare calls = %d, want 0", prepareCalls.Load())
	}
	var event map[string]any
	if err := json.Unmarshal(got.metadata, &event); err != nil || event["origin"] != "backfill" {
		t.Fatalf("backfill event=%v err=%v", event, err)
	}
}

func TestDIVAInboundImageMediaFailureFallsBackToJSONOnce(t *testing.T) {
	received := startDIVAInboundMediaWorker(t)
	logs := &divaSyncBuffer{}
	lc := newDIVAV2TestClient(logs)

	oldPrepare := divaPrepareInboundMedia
	divaPrepareInboundMedia = func(_ *LineClient, _ context.Context, kind handlers.MediaKind, _ line.Message, _ string) (*divaInboundMedia, error) {
		return nil, errors.New("OBS unavailable")
	}
	t.Cleanup(func() { divaPrepareInboundMedia = oldPrepare })

	lc.handleDIVAInbound(
		divaInboundImageMessage("600000000000000203"),
		"cgroup",
		`{"keyMaterial":"secret-key"}`,
		false,
		int(OpReceiveMessage),
		divaOriginLive,
	)

	got := receiveDIVAInbound(t, received)
	if got.contentType != "application/json" || len(got.media) != 0 {
		t.Fatalf("fallback contentType=%q media=%d", got.contentType, len(got.media))
	}
	select {
	case extra := <-received:
		t.Fatalf("unexpected duplicate DIVA request: %+v", extra)
	case <-time.After(100 * time.Millisecond):
	}
	if !strings.Contains(logs.String(), "sending descriptor only") {
		t.Fatalf("fallback not logged: %s", logs.String())
	}
}

func divaInboundTypedMediaMessage(id string, contentType ContentType, fileName string) *line.Message {
	meta := map[string]string{"FILE_SIZE": "128"}
	if fileName != "" {
		meta["FILE_NAME"] = fileName
	}
	return &line.Message{
		ID: id, From: "usender", To: "cgroup", ToType: int(ToGroup),
		ContentType: int(contentType), ContentMetadata: meta,
	}
}

func TestDIVAInboundLiveVideoAudioFileUseSameMultipart(t *testing.T) {
	cases := []struct {
		name        string
		contentType ContentType
		kind        handlers.MediaKind
		body        string
		mimeType    string
		fileName    string
		data        []byte
	}{
		{"video webm", ContentVideo, handlers.MediaKindVideo, `{"fileName":"clip.webm"}`, "video/webm", "clip.webm", []byte("video-bytes")},
		{"audio m4a", ContentAudio, handlers.MediaKindAudio, "", "audio/mp4", "audio.m4a", []byte("audio-bytes")},
		{"file pdf", ContentFile, handlers.MediaKindFile, `{"fileName":"report.pdf"}`, "application/pdf", "report.pdf", []byte("%PDF-file-bytes")},
	}

	for i, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			received := startDIVAInboundMediaWorker(t)
			lc := newDIVAV2TestClient(io.Discard)
			oldPrepare := divaPrepareInboundMedia
			divaPrepareInboundMedia = func(_ *LineClient, _ context.Context, kind handlers.MediaKind, _ line.Message, decryptedBody string) (*divaInboundMedia, error) {
				if kind != tc.kind {
					t.Fatalf("kind = %q, want %q", kind, tc.kind)
				}
				if decryptedBody != tc.body {
					t.Fatalf("decrypted body = %q, want %q", decryptedBody, tc.body)
				}
				return &divaInboundMedia{Data: tc.data, MimeType: tc.mimeType, FileName: tc.fileName}, nil
			}
			t.Cleanup(func() { divaPrepareInboundMedia = oldPrepare })

			msg := divaInboundTypedMediaMessage(fmt.Sprintf("6000000000000003%02d", i), tc.contentType, tc.fileName)
			lc.handleDIVAInbound(msg, "cgroup", tc.body, false, int(OpReceiveMessage), divaOriginLive)

			got := receiveDIVAInbound(t, received)
			if !strings.HasPrefix(got.contentType, "multipart/form-data") ||
				got.mediaType != tc.mimeType || got.mediaName != tc.fileName || !bytes.Equal(got.media, tc.data) {
				t.Fatalf("received = %+v", got)
			}
			var event map[string]any
			if err := json.Unmarshal(got.metadata, &event); err != nil {
				t.Fatal(err)
			}
			content, _ := event["content"].(map[string]any)
			if content["type"] != string(tc.kind) {
				t.Fatalf("content = %v, want type %q", content, tc.kind)
			}
		})
	}
}

func TestDIVAInboundBackfillVideoAudioFileNeverFetchesBinary(t *testing.T) {
	received := startDIVAInboundMediaWorker(t)
	lc := newDIVAV2TestClient(io.Discard)
	var prepareCalls atomic.Int32

	oldPrepare := divaPrepareInboundMedia
	divaPrepareInboundMedia = func(_ *LineClient, _ context.Context, _ handlers.MediaKind, _ line.Message, _ string) (*divaInboundMedia, error) {
		prepareCalls.Add(1)
		return nil, errors.New("should not run")
	}
	t.Cleanup(func() { divaPrepareInboundMedia = oldPrepare })

	for i, contentType := range []ContentType{ContentVideo, ContentAudio, ContentFile} {
		msg := divaInboundTypedMediaMessage(fmt.Sprintf("6000000000000004%02d", i), contentType, "")
		lc.handleDIVAInbound(msg, "cgroup", "", false, int(OpReceiveMessage), divaOriginBackfill)
	}
	for range 3 {
		got := receiveDIVAInbound(t, received)
		if got.contentType != "application/json" || len(got.media) != 0 {
			t.Fatalf("backfill request = %+v", got)
		}
	}
	if prepareCalls.Load() != 0 {
		t.Fatalf("prepare calls = %d, want 0", prepareCalls.Load())
	}
}

func TestDIVAMediaFileNameAndMIMEFollowExistingHandlerRules(t *testing.T) {
	video := line.Message{ContentMetadata: map[string]string{}}
	name, err := divaMediaFileName(handlers.MediaKindVideo, video, `{"fileName":"clip.webm"}`, "")
	if err != nil || name != "clip.webm" {
		t.Fatalf("video name=%q err=%v", name, err)
	}
	mimeType, err := divaMediaMimeType(handlers.MediaKindVideo, []byte("x"), name)
	if err != nil || mimeType != "video/webm" {
		t.Fatalf("video MIME=%q err=%v", mimeType, err)
	}

	fileWithMetadata := line.Message{ContentMetadata: map[string]string{"FILE_NAME": "metadata.pdf"}}
	fallbackName, fallbackErr := divaMediaFileName(handlers.MediaKindFile, fileWithMetadata, `{"fileName":""}`, "")
	if fallbackErr != nil || fallbackName != "metadata.pdf" {
		t.Fatalf("file metadata fallback name=%q err=%v", fallbackName, fallbackErr)
	}

	file := line.Message{ContentMetadata: map[string]string{}}
	name, err = divaMediaFileName(handlers.MediaKindFile, file, `{"fileName":"report.pdf"}`, "")
	if err != nil || name != "report.pdf" {
		t.Fatalf("file name=%q err=%v", name, err)
	}
	mimeType, err = divaMediaMimeType(handlers.MediaKindFile, []byte("%PDF"), name)
	if err != nil || mimeType != "application/pdf" {
		t.Fatalf("file MIME=%q err=%v", mimeType, err)
	}

	if _, err = divaMediaFileName(handlers.MediaKindFile, file, `{"fileName":`, ""); err == nil {
		t.Fatal("malformed encrypted fileName JSON was accepted")
	}
}

func TestPrepareDIVAInboundImageRejectsOversizeMetadataBeforeFetch(t *testing.T) {
	t.Setenv("DIVA_MEDIA_MAX_BYTES", "1024")
	lc := newDIVAV2TestClient(io.Discard)
	msg := line.Message{
		ID:              "oversize",
		ContentMetadata: map[string]string{"FILE_SIZE": "1057"},
	}
	media, err := lc.prepareDIVAInboundMedia(context.Background(), handlers.MediaKindImage, msg, "")
	if media != nil || err == nil || !strings.Contains(err.Error(), "exceeds DIVA media limit") {
		t.Fatalf("media=%v err=%v", media, err)
	}
}
