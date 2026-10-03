package handlers

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/highesttt/matrix-line-messenger/pkg/line"
)

type MediaKind string

const (
	MediaKindImage MediaKind = "image"
	MediaKindVideo MediaKind = "video"
	MediaKindAudio MediaKind = "audio"
	MediaKindFile  MediaKind = "file"
)

type FetchedMedia struct {
	Data              []byte
	OID               string
	PublicPath        string
	IsPlainMedia      bool
	DownloadStartedAt time.Time
	DownloadDuration  time.Duration
}

type mediaFetchSource struct {
	oid          string
	publicPath   string
	sid          string
	messageID    string
	isPlainMedia bool
	useSID       bool
	options      line.OBSDownloadOptions
}

func (h *Handler) FetchMedia(ctx context.Context, kind MediaKind, data line.Message, decryptedBody string) (*FetchedMedia, error) {
	client := h.NewClient()
	source, err := mediaFetchSourceFor(kind, data, decryptedBody)
	if err != nil {
		return nil, err
	}
	if source.publicPath == "" && source.oid == "" {
		return nil, nil
	}

	if kind == MediaKindFile && data.ContentMetadata["OID"] == "" && decryptedBody != "" && strings.Contains(decryptedBody, "fileName") {
		h.Log.Debug().Msg("File message with encrypted payload, OID in metadata")
	}
	if kind == MediaKindImage {
		h.Log.Debug().
			Str("oid", source.oid).
			Str("msg_id", data.ID).
			Str("tid", source.options.TID).
			Str("media_category", lineMediaCategory(data.ContentMetadata)).
			Bool("has_obs_pop", source.options.OBSPop != "").
			Bool("plain_media", source.isPlainMedia).
			Bool("public_resource", source.publicPath != "").
			Msg("Downloading image from LINE OBS")
	}

	startedAt := time.Now()
	download := func(downloadClient *line.Client) ([]byte, error) {
		switch {
		case source.publicPath != "":
			return downloadClient.DownloadOBSPublicResource(ctx, source.publicPath)
		case source.useSID:
			return downloadClient.DownloadOBSWithSIDOptions(ctx, source.oid, source.messageID, source.sid, source.options)
		default:
			return downloadClient.DownloadOBSWithOptions(ctx, source.oid, source.messageID, source.options)
		}
	}

	mediaData, err := download(client)
	if source.publicPath == "" {
		if newClient, ok := h.tryRecoverClient(ctx, client, err); ok {
			client = newClient
			mediaData, err = download(client)
		}
		h.handleFinalAuthError(ctx, client, err)
	}

	return &FetchedMedia{
		Data:              mediaData,
		OID:               source.oid,
		PublicPath:        source.publicPath,
		IsPlainMedia:      source.isPlainMedia,
		DownloadStartedAt: startedAt,
		DownloadDuration:  time.Since(startedAt),
	}, err
}

func mediaFetchSourceFor(kind MediaKind, data line.Message, decryptedBody string) (mediaFetchSource, error) {
	switch kind {
	case MediaKindImage:
		source := lineImageDownloadSource(data)
		return mediaFetchSource{
			oid:          source.oid,
			publicPath:   source.publicPath,
			sid:          "m",
			messageID:    obsTalkMetaMessageID(data.ID, source.isPlainMedia),
			isPlainMedia: source.isPlainMedia,
			useSID:       source.isPlainMedia,
			options:      lineOBSDownloadOptions(data.ContentMetadata, source.isPlainMedia),
		}, nil
	case MediaKindVideo, MediaKindAudio:
		oid := data.ContentMetadata["OID"]
		isPlainMedia := oid == ""
		if oid == "" && decryptedBody != "" && strings.Contains(decryptedBody, "OID") {
			var decryptInfo struct {
				OID string `json:"OID"`
			}
			if err := json.Unmarshal([]byte(decryptedBody), &decryptInfo); err == nil && decryptInfo.OID != "" {
				oid = decryptInfo.OID
				isPlainMedia = false
			}
		}
		if isPlainMedia {
			oid = data.ID
		}
		sid := "emv"
		if kind == MediaKindAudio {
			sid = "ema"
		}
		if isPlainMedia {
			sid = "m"
		}
		return mediaFetchSource{
			oid:          oid,
			sid:          sid,
			messageID:    obsTalkMetaMessageID(data.ID, isPlainMedia),
			isPlainMedia: isPlainMedia,
			useSID:       true,
			options:      lineOBSDownloadOptions(data.ContentMetadata, isPlainMedia),
		}, nil
	case MediaKindFile:
		oid := data.ContentMetadata["OID"]
		isPlainMedia := oid == ""
		if isPlainMedia {
			oid = data.ID
		}
		sid := "emf"
		if isPlainMedia {
			sid = "m"
		}
		return mediaFetchSource{
			oid:          oid,
			sid:          sid,
			messageID:    obsTalkMetaMessageID(data.ID, isPlainMedia),
			isPlainMedia: isPlainMedia,
			useSID:       true,
			options:      lineOBSDownloadOptions(data.ContentMetadata, isPlainMedia),
		}, nil
	default:
		return mediaFetchSource{}, fmt.Errorf("unsupported LINE media kind %q", kind)
	}
}
