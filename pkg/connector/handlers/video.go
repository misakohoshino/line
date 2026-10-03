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

// FetchVideo downloads and decrypts a LINE video using the same source/SID
// rules as Matrix conversion. Private OBS identifiers stay inside the bridge.
func (h *Handler) FetchVideo(ctx context.Context, data line.Message, decryptedBody string) (FetchedMedia, error) {
	result := FetchedMedia{}
	oid := data.ContentMetadata["OID"]
	isPlainMedia := oid == ""
	if oid == "" && decryptedBody != "" && strings.Contains(decryptedBody, "OID") {
		var decryptInfo struct {
			OID         string `json:"OID"`
			KeyMaterial string `json:"keyMaterial"`
			FileName    string `json:"fileName"`
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
	sid := "emv"
	if isPlainMedia {
		sid = "m"
	}
	fetched, err := h.fetchMedia(ctx, mediaFetchRequest{
		MessageID: data.ID,
		OID:       oid,
		SID:       sid,
		UseSID:    true,
		Plain:     isPlainMedia,
		Options:   lineOBSDownloadOptions(data.ContentMetadata, isPlainMedia),
	})
	result.Data = fetched.Data
	result.DownloadDuration = fetched.DownloadDuration
	if err != nil {
		h.Log.Warn().Err(err).Str("oid", oid).Str("msg_id", data.ID).
			Bool("plain_media", isPlainMedia).Dur("download_duration", result.DownloadDuration).
			Msg("Failed to download video from OBS")
		return result, err
	}
	result.Downloaded = true
	decryptStart := time.Now()
	result.Data, err = h.decryptDownloadedMedia(result.Data, decryptedBody, data.ContentMetadata, "video")
	result.DecryptDuration = time.Since(decryptStart)
	if err != nil {
		h.Log.Error().Err(err).Msg("Failed to decrypt video data")
		return result, err
	}
	return result, nil
}

// ConvertVideo converts a LINE video message to a Matrix video message.
func (h *Handler) ConvertVideo(ctx context.Context, portal *bridgev2.Portal, intent bridgev2.MatrixAPI, data line.Message, decryptedBody string, relatesTo *event.RelatesTo) (*bridgev2.ConvertedMessage, error) {
	if oversized := h.oversizedMediaNoticeFromMetadata(data.ContentMetadata, relatesTo); oversized != nil {
		return oversized, nil
	}

	fetched, err := h.FetchVideo(ctx, data, decryptedBody)
	videoData := fetched.Data
	if err != nil {
		if !fetched.Downloaded {
			return mediaDownloadFailure("Video", err, relatesTo)
		}
		return nil, err
	}

	if oversized := h.oversizedMediaNotice(int64(len(videoData)), "downloaded", relatesTo); oversized != nil {
		return oversized, nil
	}

	fileName := data.ContentMetadata["FILE_NAME"]

	if fileName == "" && decryptedBody != "" && strings.Contains(decryptedBody, "fileName") {
		var decryptInfo struct {
			FileName string `json:"fileName"`
		}
		if err := json.Unmarshal([]byte(decryptedBody), &decryptInfo); err == nil && decryptInfo.FileName != "" {
			fileName = decryptInfo.FileName
		}
	}

	if fileName == "" {
		fileName = "video.mp4"
	}

	mimeType := "video/mp4"
	if strings.HasSuffix(strings.ToLower(fileName), ".webm") {
		mimeType = "video/webm"
	}

	mxc, file, err := intent.UploadMedia(ctx, portal.MXID, videoData, fileName, mimeType)
	if err != nil {
		h.Log.Error().Err(err).Int("size_bytes", len(videoData)).Msg("Failed to upload video to Matrix")
		return nil, fmt.Errorf("failed to upload video to matrix: %w", err)
	}

	h.Log.Info().
		Str("mxc", mxc.ParseOrIgnore().String()).
		Str("file_name", fileName).
		Int("size", len(videoData)).
		Dur("download_duration", fetched.DownloadDuration).
		Msg("Successfully uploaded video to Matrix")

	var duration int
	if durationStr := data.ContentMetadata["DURATION"]; durationStr != "" {
		if d, err := strconv.Atoi(durationStr); err == nil {
			duration = d
		}
	}

	videoInfo := &event.FileInfo{
		MimeType: mimeType,
		Size:     len(videoData),
	}
	if duration > 0 {
		videoInfo.Duration = duration
	}

	return &bridgev2.ConvertedMessage{
		Parts: []*bridgev2.ConvertedMessagePart{
			{
				Type: event.EventMessage,
				Content: &event.MessageEventContent{
					MsgType:   event.MsgVideo,
					Body:      fileName,
					URL:       mxc,
					File:      file,
					Info:      videoInfo,
					RelatesTo: relatesTo,
				},
			},
		},
	}, nil
}
