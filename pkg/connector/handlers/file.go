package handlers

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"maunium.net/go/mautrix/bridgev2"
	"maunium.net/go/mautrix/event"

	"github.com/highesttt/matrix-line-messenger/pkg/line"
)

// ConvertFile converts a LINE file message to a Matrix file message.
func (h *Handler) ConvertFile(ctx context.Context, portal *bridgev2.Portal, intent bridgev2.MatrixAPI, data line.Message, decryptedBody string, relatesTo *event.RelatesTo) (*bridgev2.ConvertedMessage, error) {
	if oversized := h.oversizedMediaNoticeFromMetadata(data.ContentMetadata, relatesTo); oversized != nil {
		return oversized, nil
	}

	fetched, err := h.FetchMedia(ctx, MediaKindFile, data, decryptedBody)
	if fetched == nil {
		return nil, nil
	}
	if err != nil {
		h.Log.Warn().
			Err(err).
			Str("oid", fetched.OID).
			Bool("plain_media", fetched.IsPlainMedia).
			Msg("Failed to download file from OBS")
		return mediaDownloadFailure("File", err, relatesTo)
	}

	fileData := fetched.Data

	var fileName string
	if strings.Contains(decryptedBody, "fileName") {
		var fileInfo struct {
			FileName string `json:"fileName"`
		}
		if err := json.Unmarshal([]byte(decryptedBody), &fileInfo); err != nil {
			h.Log.Error().Err(err).Msg("Failed to parse file payload JSON")
			return nil, fmt.Errorf("failed to parse file payload: %w", err)
		}
		fileName = fileInfo.FileName
	}

	fileData, err = h.decryptDownloadedMedia(fileData, decryptedBody, data.ContentMetadata, "file")
	if err != nil {
		h.Log.Error().Err(err).Msg("Failed to decrypt file data")
		return nil, err
	}

	if oversized := h.oversizedMediaNotice(int64(len(fileData)), "downloaded", relatesTo); oversized != nil {
		return oversized, nil
	}

	if fileName == "" {
		fileName = data.ContentMetadata["FILE_NAME"]
	}

	if fileName == "" {
		fileName = "file.bin"
	}

	// Detect MIME type from file extension
	mimeType := "application/octet-stream"
	if strings.HasSuffix(strings.ToLower(fileName), ".pdf") {
		mimeType = "application/pdf"
	}

	mxc, file, err := intent.UploadMedia(ctx, portal.MXID, fileData, fileName, mimeType)
	if err != nil {
		h.Log.Error().Err(err).Int("size_bytes", len(fileData)).Msg("Failed to upload file to Matrix")
		return nil, fmt.Errorf("failed to upload file to matrix: %w", err)
	}

	h.Log.Info().
		Str("mxc", mxc.ParseOrIgnore().String()).
		Str("file_name", fileName).
		Int("size", len(fileData)).
		Msg("Successfully uploaded file to Matrix")

	return &bridgev2.ConvertedMessage{
		Parts: []*bridgev2.ConvertedMessagePart{
			{
				Type: event.EventMessage,
				Content: &event.MessageEventContent{
					MsgType: event.MsgFile,
					Body:    fileName,
					URL:     mxc,
					File:    file,
					Info: &event.FileInfo{
						MimeType: mimeType,
						Size:     len(fileData),
					},
					RelatesTo: relatesTo,
				},
			},
		},
	}, nil
}
