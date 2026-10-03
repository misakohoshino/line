package handlers

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"maunium.net/go/mautrix/bridgev2"
	"maunium.net/go/mautrix/event"

	"github.com/highesttt/matrix-line-messenger/pkg/line"
)

type FetchedFile struct {
	FetchedMedia
	FileName string
}

// FetchFile downloads and decrypts a LINE file using the existing emf/m SID
// rules and resolves the E2EE fileName payload inside the bridge.
func (h *Handler) FetchFile(ctx context.Context, data line.Message, decryptedBody string) (FetchedFile, error) {
	result := FetchedFile{}
	oid := data.ContentMetadata["OID"]
	isPlainMedia := oid == ""
	if oid == "" && decryptedBody != "" && strings.Contains(decryptedBody, "fileName") {
		h.Log.Debug().Msg("File message with encrypted payload, OID in metadata")
	}
	if isPlainMedia {
		oid = data.ID
	}
	if oid == "" {
		return result, nil
	}
	sid := "emf"
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
		h.Log.Warn().Err(err).Str("oid", oid).Bool("plain_media", isPlainMedia).
			Msg("Failed to download file from OBS")
		return result, err
	}
	result.Downloaded = true
	if strings.Contains(decryptedBody, "fileName") {
		var fileInfo struct {
			FileName string `json:"fileName"`
		}
		if err := json.Unmarshal([]byte(decryptedBody), &fileInfo); err != nil {
			h.Log.Error().Err(err).Msg("Failed to parse file payload JSON")
			return result, fmt.Errorf("failed to parse file payload: %w", err)
		}
		result.FileName = fileInfo.FileName
	}
	decryptStart := time.Now()
	result.Data, err = h.decryptDownloadedMedia(result.Data, decryptedBody, data.ContentMetadata, "file")
	result.DecryptDuration = time.Since(decryptStart)
	if err != nil {
		h.Log.Error().Err(err).Msg("Failed to decrypt file data")
		return result, err
	}
	return result, nil
}

// ConvertFile converts a LINE file message to a Matrix file message.
func (h *Handler) ConvertFile(ctx context.Context, portal *bridgev2.Portal, intent bridgev2.MatrixAPI, data line.Message, decryptedBody string, relatesTo *event.RelatesTo) (*bridgev2.ConvertedMessage, error) {
	if oversized := h.oversizedMediaNoticeFromMetadata(data.ContentMetadata, relatesTo); oversized != nil {
		return oversized, nil
	}

	fetched, err := h.FetchFile(ctx, data, decryptedBody)
	fileData := fetched.Data
	fileName := fetched.FileName
	if err != nil {
		if !fetched.Downloaded {
			return mediaDownloadFailure("File", err, relatesTo)
		}
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
