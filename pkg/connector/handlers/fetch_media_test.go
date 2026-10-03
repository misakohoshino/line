package handlers

import (
	"testing"

	"github.com/highesttt/matrix-line-messenger/pkg/line"
)

func TestMediaFetchSourceForPreservesExistingPaths(t *testing.T) {
	tests := []struct {
		name          string
		kind          MediaKind
		message       line.Message
		decryptedBody string
		wantOID       string
		wantPublic    string
		wantSID       string
		wantMessageID string
		wantPlain     bool
		wantUseSID    bool
		wantTID       string
		wantOBSPop    string
	}{
		{
			name: "image public resource",
			kind: MediaKindImage,
			message: line.Message{
				ID: "img-public",
				ContentMetadata: map[string]string{
					"DOWNLOAD_URL": "/r/official/image",
					"OID":          "ignored-oid",
				},
			},
			wantPublic:    "/r/official/image",
			wantSID:       "m",
			wantMessageID: "img-public",
		},
		{
			name: "image private OID keeps no-SID path",
			kind: MediaKindImage,
			message: line.Message{
				ID: "img-private",
				ContentMetadata: map[string]string{
					"OID":     "image-oid",
					"OBS_POP": "pop-token",
				},
			},
			wantOID:       "image-oid",
			wantSID:       "m",
			wantMessageID: "img-private",
			wantOBSPop:    "pop-token",
		},
		{
			name: "image plain original",
			kind: MediaKindImage,
			message: line.Message{
				ID: "img-plain",
				ContentMetadata: map[string]string{
					"MEDIA_CONTENT_INFO": `{"category":"original"}`,
					"OBS_POP":            "plain-pop",
				},
			},
			wantOID:    "img-plain",
			wantSID:    "m",
			wantPlain:  true,
			wantUseSID: true,
			wantTID:    "original",
			wantOBSPop: "plain-pop",
		},
		{
			name: "video encrypted OID from body",
			kind: MediaKindVideo,
			message: line.Message{
				ID:              "video-msg",
				ContentMetadata: map[string]string{"OBS_POP": "video-pop"},
			},
			decryptedBody: `{"OID":"video-oid","keyMaterial":"key","fileName":"clip.mp4"}`,
			wantOID:       "video-oid",
			wantSID:       "emv",
			wantMessageID: "video-msg",
			wantUseSID:    true,
			wantOBSPop:    "video-pop",
		},
		{
			name: "video malformed encrypted body stays plain",
			kind: MediaKindVideo,
			message: line.Message{
				ID: "video-plain",
			},
			decryptedBody: `{"OID":`,
			wantOID:       "video-plain",
			wantSID:       "m",
			wantPlain:     true,
			wantUseSID:    true,
		},
		{
			name: "audio encrypted metadata OID",
			kind: MediaKindAudio,
			message: line.Message{
				ID:              "audio-msg",
				ContentMetadata: map[string]string{"OID": "audio-oid"},
			},
			wantOID:       "audio-oid",
			wantSID:       "ema",
			wantMessageID: "audio-msg",
			wantUseSID:    true,
		},
		{
			name: "file encrypted metadata OID",
			kind: MediaKindFile,
			message: line.Message{
				ID:              "file-msg",
				ContentMetadata: map[string]string{"OID": "file-oid"},
			},
			wantOID:       "file-oid",
			wantSID:       "emf",
			wantMessageID: "file-msg",
			wantUseSID:    true,
		},
		{
			name: "file plain",
			kind: MediaKindFile,
			message: line.Message{
				ID: "file-plain",
			},
			wantOID:    "file-plain",
			wantSID:    "m",
			wantPlain:  true,
			wantUseSID: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := mediaFetchSourceFor(tc.kind, tc.message, tc.decryptedBody)
			if err != nil {
				t.Fatal(err)
			}
			if got.oid != tc.wantOID ||
				got.publicPath != tc.wantPublic ||
				got.sid != tc.wantSID ||
				got.messageID != tc.wantMessageID ||
				got.isPlainMedia != tc.wantPlain ||
				got.useSID != tc.wantUseSID ||
				got.options.TID != tc.wantTID ||
				got.options.OBSPop != tc.wantOBSPop {
				t.Fatalf("source = %#v", got)
			}
		})
	}
}

func TestMediaFetchSourceForRejectsUnknownKind(t *testing.T) {
	if _, err := mediaFetchSourceFor(MediaKind("sticker"), line.Message{ID: "msg"}, ""); err == nil {
		t.Fatal("unknown media kind was accepted")
	}
}
