package handlers

import (
	"context"
	"fmt"
	"strconv"

	"maunium.net/go/mautrix/bridgev2"
	"maunium.net/go/mautrix/event"

	"github.com/highesttt/matrix-line-messenger/pkg/line"
)

// ConvertAudio converts a LINE audio message to a Matrix audio message.
func (h *Handler) ConvertAudio(ctx context.Context, portal *bridgev2.Portal, intent bridgev2.MatrixAPI, data line.Message, decryptedBody string, relatesTo *event.RelatesTo) (*bridgev2.ConvertedMessage, error) {
	if oversized := h.oversizedMediaNoticeFromMetadata(data.ContentMetadata, relatesTo); oversized != nil {
		return oversized, nil
	}

	fetched, err := h.FetchMedia(ctx, MediaKindAudio, data, decryptedBody)
	if fetched == nil {
		return nil, nil
	}
	if err != nil {
		h.Log.Warn().
			Err(err).
			Str("oid", fetched.OID).
			Str("msg_id", data.ID).
			Bool("plain_media", fetched.IsPlainMedia).
			Msg("Failed to download audio from OBS")
		return mediaDownloadFailure("Audio", err, relatesTo)
	}

	audioData := fetched.Data

	audioData, err = h.decryptDownloadedMedia(audioData, decryptedBody, data.ContentMetadata, "audio")
	if err != nil {
		h.Log.Error().Err(err).Msg("Failed to decrypt audio data")
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
