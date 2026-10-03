package handlers

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"maunium.net/go/mautrix/bridgev2"
	"maunium.net/go/mautrix/event"

	"github.com/highesttt/matrix-line-messenger/pkg/line"
)

// ConvertImage converts a LINE image message to a Matrix image message.
func (h *Handler) ConvertImage(ctx context.Context, portal *bridgev2.Portal, intent bridgev2.MatrixAPI, data line.Message, decryptedBody string, relatesTo *event.RelatesTo) (*bridgev2.ConvertedMessage, error) {
	if oversized := h.oversizedMediaNoticeFromMetadata(data.ContentMetadata, relatesTo); oversized != nil {
		return oversized, nil
	}

	fetched, err := h.FetchMedia(ctx, MediaKindImage, data, decryptedBody)
	if fetched == nil {
		return nil, nil
	}
	downloadDuration := fetched.DownloadDuration

	if err != nil {
		h.Log.Warn().
			Err(err).
			Str("oid", fetched.OID).
			Str("msg_id", data.ID).
			Bool("plain_media", fetched.IsPlainMedia).
			Bool("public_resource", fetched.PublicPath != "").
			Dur("download_duration", downloadDuration).
			Msg("Failed to download image from OBS")
		return mediaDownloadFailure("Image", err, relatesTo)
	}

	imgData := fetched.Data

	// Decrypt encrypted media before it can reach Matrix.
	decryptStart := time.Now()
	imgData, err = h.decryptDownloadedMedia(imgData, decryptedBody, data.ContentMetadata, "image")
	decryptDuration := time.Since(decryptStart)
	if err != nil {
		h.Log.Error().
			Err(err).
			Dur("download_duration", downloadDuration).
			Dur("decrypt_duration", decryptDuration).
			Msg("Failed to decrypt image data")
		return nil, err
	}

	if oversized := h.oversizedMediaNotice(int64(len(imgData)), "downloaded", relatesTo); oversized != nil {
		return oversized, nil
	}

	// Upload to Matrix
	uploadStart := time.Now()
	mxc, file, err := intent.UploadMedia(ctx, portal.MXID, imgData, "image.jpg", "image/jpeg")
	uploadDuration := time.Since(uploadStart)
	if err != nil {
		h.Log.Error().
			Err(err).
			Int("size_bytes", len(imgData)).
			Dur("download_duration", downloadDuration).
			Dur("decrypt_duration", decryptDuration).
			Dur("upload_duration", uploadDuration).
			Msg("Failed to upload image to Matrix")
		return nil, fmt.Errorf("failed to upload image to matrix: %w", err)
	}

	matrixMediaURL := string(mxc)
	if file != nil && file.URL != "" {
		matrixMediaURL = string(file.URL)
	}

	h.Log.Info().
		Str("matrix_media_url", matrixMediaURL).
		Int("size", len(imgData)).
		Dur("download_duration", downloadDuration).
		Dur("decrypt_duration", decryptDuration).
		Dur("upload_duration", uploadDuration).
		Msg("Successfully uploaded image to Matrix")

	return &bridgev2.ConvertedMessage{
		Parts: []*bridgev2.ConvertedMessagePart{
			{
				Type: event.EventMessage,
				Content: &event.MessageEventContent{
					MsgType:   event.MsgImage,
					Body:      "image.jpg",
					URL:       mxc,
					File:      file,
					RelatesTo: relatesTo,
				},
			},
		},
	}, nil
}

type imageDownloadSource struct {
	publicPath   string
	oid          string
	isPlainMedia bool
}

func lineImageDownloadSource(data line.Message) imageDownloadSource {
	if publicPath := data.ContentMetadata["DOWNLOAD_URL"]; publicPath != "" {
		return imageDownloadSource{publicPath: publicPath}
	}

	oid := data.ContentMetadata["OID"]
	if oid != "" {
		return imageDownloadSource{oid: oid}
	}

	// For plain media, the image is stored at r/talk/m/{messageID}.
	return imageDownloadSource{oid: data.ID, isPlainMedia: true}
}

func lineMediaCategory(metadata map[string]string) string {
	if metadata == nil || metadata["MEDIA_CONTENT_INFO"] == "" {
		return ""
	}

	var info struct {
		Category string `json:"category"`
	}
	if err := json.Unmarshal([]byte(metadata["MEDIA_CONTENT_INFO"]), &info); err != nil {
		return ""
	}

	return info.Category
}

func lineOBSDownloadOptions(metadata map[string]string, isPlainMedia bool) line.OBSDownloadOptions {
	opts := line.OBSDownloadOptions{
		OBSPop: metadata["OBS_POP"],
	}
	if isPlainMedia && lineMediaCategory(metadata) == "original" {
		opts.TID = "original"
	}
	return opts
}
