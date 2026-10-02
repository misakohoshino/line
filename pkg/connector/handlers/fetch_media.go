package handlers

import (
	"context"
	"time"

	"github.com/highesttt/matrix-line-messenger/pkg/line"
)

// mediaFetchRequest describes one already-resolved LINE OBS download.
// Callers keep ownership of media-specific source selection (OID/public path,
// SID, plain-vs-E2EE). This helper only centralizes the actual fetch + one
// auth-recovery retry, so the refactor does not change media semantics.
type mediaFetchRequest struct {
	MessageID  string
	OID        string
	PublicPath string
	SID        string
	UseSID     bool
	Plain      bool
	Options    line.OBSDownloadOptions
}

type mediaFetchResult struct {
	Data             []byte
	StartedAt        time.Time
	DownloadDuration time.Duration
}

func (h *Handler) fetchMedia(ctx context.Context, req mediaFetchRequest) (mediaFetchResult, error) {
	result := mediaFetchResult{StartedAt: time.Now()}
	client := h.NewClient()

	download := func(current *line.Client) ([]byte, error) {
		if req.PublicPath != "" {
			return current.DownloadOBSPublicResource(ctx, req.PublicPath)
		}
		talkMetaMessageID := obsTalkMetaMessageID(req.MessageID, req.Plain)
		if req.UseSID {
			return current.DownloadOBSWithSIDOptions(ctx, req.OID, talkMetaMessageID, req.SID, req.Options)
		}
		return current.DownloadOBSWithOptions(ctx, req.OID, talkMetaMessageID, req.Options)
	}

	data, err := download(client)
	// Public resources are intentionally unauthenticated and keep the existing
	// behavior: no LINE token recovery is attempted for that path.
	if req.PublicPath == "" {
		if newClient, ok := h.tryRecoverClient(ctx, client, err); ok {
			client = newClient
			data, err = download(client)
		}
		h.handleFinalAuthError(ctx, client, err)
	}

	result.Data = data
	result.DownloadDuration = time.Since(result.StartedAt)
	return result, err
}
