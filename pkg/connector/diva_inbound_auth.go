package connector

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"
)

// Sign the final body (including multipart bytes), never sender-supplied fields
// in isolation. Missing credentials leave raw/call forwarding compatible;
// Worker management admission fails closed. No credentials go into logs.
func signDIVAInbound(req *http.Request, body []byte) {
	signDIVAInboundAt(req, body, strings.TrimSpace(os.Getenv("DIVA_CONTROL_TOKEN")), time.Now().Unix())
}

func signDIVAInboundAt(req *http.Request, body []byte, token string, timestamp int64) {
	if len(token) < 32 {
		return
	}
	stamp := strconv.FormatInt(timestamp, 10)
	digest := sha256.Sum256(body)
	canonical := fmt.Sprintf("diva-owner-inbound-v1\n%s\n%s\n%s\n%s\n%x",
		req.Method, req.URL.EscapedPath(), stamp, req.Header.Get("Content-Type"), digest)
	mac := hmac.New(sha256.New, []byte(token))
	_, _ = mac.Write([]byte(canonical))
	req.Header.Set("X-DIVA-Inbound-Timestamp", stamp)
	req.Header.Set("X-DIVA-Inbound-Signature", hex.EncodeToString(mac.Sum(nil)))
}
