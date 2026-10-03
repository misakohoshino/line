package handlers

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"time"

	"maunium.net/go/mautrix/bridgev2"
	"maunium.net/go/mautrix/event"

	"github.com/highesttt/matrix-line-messenger/pkg/line"
)

// FetchAudio downloads and decrypts a LINE audio message using the existing
// ema/m SID rules. Private OBS identifiers stay inside the bridge.
func (h *Handler) FetchAudio(ctx context.Context, data line.Message, decryptedBody string) (FetchedMedia, error) {
	result := FetchedMedia{}
	oid := data.ContentMetadata["OID"]
	isPlainMedia := oid == ""
	if oid == "" && decryptedBody != "" && strings.Contains(decryptedBody, "OID") {
		var decryptInfo struct {
			OID         string `json:"OID"`
			KeyMaterial string `json:"keyMaterial"`
		}
		if err := json.Unmarshal([]byte(decryptedBody), &decryptInfo); err == nil && decryptInfo.OID != "" {
			oid = decryptInfo.OID
			isPlainMedia = false
		}
	}
	if isPlainMedia {
		oid = data.ID
	}
	if oid == "" {
		return result, nil
	}
	sid := "ema"
	if isPlainMedia {
		sid = "m"
	}
	fetched, err := h.fetchMedia(ctx, mediaFetchRequest{
		MessageID: data.ID, OID: oid, SID: sid, UseSID: true, Plain: isPlainMedia,
		Options: lineOBSDownloadOptions(data.ContentMetadata, isPlainMedia),
	})
	result.Data = fetched.Data
	result.DownloadDuration = fetched.DownloadDuration
	if err != nil {
		h.Log.Warn().Err(err).Str("oid", oid).Str("msg_id", data.ID).
			Bool("plain_media", isPlainMedia).Msg("Failed to download audio from OBS")
		return result, err
	}
	result.Downloaded = true
	decryptStart := time.Now()
	result.Data, err = h.decryptDownloadedMedia(result.Data, decryptedBody, data.ContentMetadata, "audio")
	result.DecryptDuration = time.Since(decryptStart)
	if err != nil {
		h.Log.Error().Err(err).Msg("Failed to decrypt audio data")
		return result, err
	}
	return result, nil
}

// ConvertAudio converts a LINE audio message to a Matrix audio message.
func (h *Handler) ConvertAudio(ctx context.Context, portal *bridgev2.Portal, intent bridgev2.MatrixAPI, data line.Message, decryptedBody string, relatesTo *event.RelatesTo) (*bridgev2.ConvertedMessage, error) {
	if oversized := h.oversizedMediaNoticeFromMetadata(data.ContentMetadata, relatesTo); oversized != nil {
		return oversized, nil
	}

	fetched, err := h.FetchAudio(ctx, data, decryptedBody)
	audioData := fetched.Data
	if err != nil {
		if !fetched.Downloaded {
			return mediaDownloadFailure("Audio", err, relatesTo)
		}
		return nil, err
	}

	if oversized := h.oversizedMediaNotice(int64(len(audioData)), "downloaded", relatesTo); oversized != nil {
		return oversized, nil
	}

	var duration int
	if durationStr := data.ContentMetadata["DURATION"]; durationStr != "" {
		if d, err := strconv.Atoi(durationStr); err == nil {
			duration = d
		}
	}

	mxc, file, err := intent.UploadMedia(ctx, portal.MXID, audioData, "audio.m4a", "audio/mp4")
	if err != nil {
		return nil, fmt.Errorf("failed to upload audio to matrix: %w", err)
	}

	audioInfo := &event.FileInfo{
		MimeType: "audio/mp4",
		Size:     len(audioData),
	}
	if duration > 0 {
		audioInfo.Duration = duration
	}

	return &bridgev2.ConvertedMessage{
		Parts: []*bridgev2.ConvertedMessagePart{
			{
				Type: event.EventMessage,
				Content: &event.MessageEventContent{
					MsgType:   event.MsgAudio,
					Body:      "audio.m4a",
					URL:       mxc,
					File:      file,
					Info:      audioInfo,
					RelatesTo: relatesTo,
				},
			},
		},
	}, nil
}
