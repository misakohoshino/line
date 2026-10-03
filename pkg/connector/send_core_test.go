package connector

// Characterization tests for the LINE send path.
//
// They were written against the send path before it was split into
// sendLineOutbound, and must keep passing unchanged afterwards: they are the
// proof that the Matrix and DIVA send behaviour did not change.
//
// A fake transport stands in for both the LINE Talk RPC gateway and OBS, and
// records every request. Message-level E2EE goes through the send-path seams
// because LTSM cannot export a generated private key.

import (
	"archive/zip"
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"image"
	"image/color"
	"image/png"
	"io"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/rs/zerolog"

	"maunium.net/go/mautrix/bridgev2"
	"maunium.net/go/mautrix/bridgev2/database"
	"maunium.net/go/mautrix/bridgev2/networkid"
	"maunium.net/go/mautrix/event"
	"maunium.net/go/mautrix/id"

	"github.com/highesttt/matrix-line-messenger/pkg/e2ee"
	"github.com/highesttt/matrix-line-messenger/pkg/line"
)

const (
	sendTestSelf  = "uself0000000000000000000000000000"
	sendTestGroup = "cgroup000000000000000000000000000"
	sendTestPeer  = "upeer0000000000000000000000000000"
)

// ---------------------------------------------------------------------------
// fake LINE transport
// ---------------------------------------------------------------------------

type fakeLineCall struct {
	// Method is the Talk RPC method name, or "OBS" for object storage.
	Method string
	Path   string
	Args   []json.RawMessage
	Body   []byte
	// ObsType is the "type" in X-Obs-Params for OBS uploads.
	ObsType string
}

type fakeSendReply struct {
	status int
	body   string
}

type fakeLine struct {
	mu        sync.Mutex
	calls     []fakeLineCall
	sendQueue []fakeSendReply
	sends     int
	oids      int
	mediaFlow map[string]int
	sent      chan struct{}
	// contacts answers getContactsV2 (mid -> display name); other mids are
	// unknown. contactDelay slows getContactsV2 down; set it before use.
	contacts       map[string]string
	contactDelay   time.Duration
	memberChatMids []string
	obsDownloads   map[string][]byte
}

func newFakeLine() *fakeLine {
	return &fakeLine{
		mediaFlow:    map[string]int{"1": 2, "2": 2, "3": 2, "14": 2},
		sent:         make(chan struct{}, 16),
		obsDownloads: map[string][]byte{},
	}
}

// failNextSend makes the next sendMessage call answer with status/body.
func (f *fakeLine) failNextSend(status int, body string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.sendQueue = append(f.sendQueue, fakeSendReply{status: status, body: body})
}

func fakeHTTPResponse(req *http.Request, status int, body string, header http.Header) *http.Response {
	if header == nil {
		header = http.Header{}
	}
	return &http.Response{
		StatusCode: status,
		Header:     header,
		Body:       io.NopCloser(strings.NewReader(body)),
		Request:    req,
	}
}

func (f *fakeLine) RoundTrip(req *http.Request) (*http.Response, error) {
	var body []byte
	if req.Body != nil {
		body, _ = io.ReadAll(req.Body)
	}
	if f.contactDelay > 0 && strings.HasSuffix(req.URL.Path, "/getContactsV2") {
		time.Sleep(f.contactDelay)
	}
	f.mu.Lock()
	defer f.mu.Unlock()

	if req.URL.Host == "obs.line-apps.com" {
		call := fakeLineCall{Method: "OBS", Path: req.URL.Path, Body: body}
		if raw, err := base64.StdEncoding.DecodeString(req.Header.Get("X-Obs-Params")); err == nil {
			var params map[string]string
			_ = json.Unmarshal(raw, &params)
			call.ObsType = params["type"]
		}
		f.calls = append(f.calls, call)
		if req.Method == http.MethodGet {
			if data, ok := f.obsDownloads[req.URL.Path]; ok {
				return fakeHTTPResponse(req, http.StatusOK, string(data), nil), nil
			}
			return fakeHTTPResponse(req, http.StatusNotFound, "", nil), nil
		}
		header := http.Header{}
		if strings.Contains(req.URL.Path, "/reqid-") {
			f.oids++
			header.Set("x-obs-oid", fmt.Sprintf("oid-%d", f.oids))
		}
		return fakeHTTPResponse(req, 201, "", header), nil
	}

	method := req.URL.Path[strings.LastIndex(req.URL.Path, "/")+1:]
	var args []json.RawMessage
	_ = json.Unmarshal(body, &args)
	f.calls = append(f.calls, fakeLineCall{Method: method, Path: req.URL.Path, Args: args, Body: body})

	switch method {
	case "sendMessage":
		if len(f.sendQueue) > 0 {
			reply := f.sendQueue[0]
			f.sendQueue = f.sendQueue[1:]
			return fakeHTTPResponse(req, reply.status, reply.body, nil), nil
		}
		f.sends++
		var msg map[string]any
		_ = json.Unmarshal(args[1], &msg)
		msg["id"] = fmt.Sprintf("srv-%d", f.sends)
		data, _ := json.Marshal(map[string]any{"code": 0, "message": "", "data": msg})
		select {
		case f.sent <- struct{}{}:
		default:
		}
		return fakeHTTPResponse(req, 200, string(data), nil), nil
	case "getAllChatMids":
		data, _ := json.Marshal(map[string]any{
			"code": 0,
			"message": "",
			"data": map[string]any{
				"memberChatMids":  append([]string(nil), f.memberChatMids...),
				"invitedChatMids": []string{},
			},
		})
		return fakeHTTPResponse(req, 200, string(data), nil), nil
	case "getContactsV2":
		var query line.GetContactsV2Request
		if len(args) > 0 {
			_ = json.Unmarshal(args[0], &query)
		}
		contacts := map[string]any{}
		for _, mid := range query.TargetUserMids {
			if name, ok := f.contacts[mid]; ok {
				contacts[mid] = map[string]any{"contact": map[string]any{"mid": mid, "displayName": name}}
			}
		}
		data, _ := json.Marshal(map[string]any{"code": 0, "message": "", "data": map[string]any{"contacts": contacts}})
		return fakeHTTPResponse(req, 200, string(data), nil), nil
	case "acquireEncryptedAccessToken":
		return fakeHTTPResponse(req, 200, `{"code":0,"message":"","data":"3600\u001eobs-token"}`, nil), nil
	case "determineMediaMessageFlow":
		data, _ := json.Marshal(map[string]any{
			"code": 0, "message": "",
			"data": map[string]any{"flowMap": f.mediaFlow, "cacheTtlMillis": "60000"},
		})
		return fakeHTTPResponse(req, 200, string(data), nil), nil
	}
	return fakeHTTPResponse(req, 500, `{"code":500,"message":"unexpected method in fake LINE"}`, nil), nil
}

func (f *fakeLine) snapshot() []fakeLineCall {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]fakeLineCall(nil), f.calls...)
}

func (f *fakeLine) methods() []string {
	var out []string
	for _, call := range f.snapshot() {
		if call.Method == "OBS" {
			out = append(out, "OBS "+call.Path)
		} else {
			out = append(out, call.Method)
		}
	}
	return out
}

type sentLineMessage struct {
	ReqSeq int
	Msg    line.Message
}

func (f *fakeLine) sentMessages(t *testing.T) []sentLineMessage {
	t.Helper()
	var out []sentLineMessage
	for _, call := range f.snapshot() {
		if call.Method != "sendMessage" {
			continue
		}
		var seq int
		var msg line.Message
		if err := json.Unmarshal(call.Args[0], &seq); err != nil {
			t.Fatalf("sendMessage reqSeq: %v", err)
		}
		if err := json.Unmarshal(call.Args[1], &msg); err != nil {
			t.Fatalf("sendMessage message: %v", err)
		}
		out = append(out, sentLineMessage{ReqSeq: seq, Msg: msg})
	}
	return out
}

const (
	talkExceptionNotFound = `{"code":10051,"message":"RESPONSE_ERROR","data":{"name":"TalkException","code":5,"reason":"not found"}}`
	talkExceptionNoGroupK = `{"code":10051,"message":"RESPONSE_ERROR","data":{"name":"TalkException","code":99,"reason":"group key is not registered"}}`
	extensionRejectsFile  = `{"code":1,"message":"Extension does not support file upload","data":null}`
)

// ---------------------------------------------------------------------------
// fake E2EE seams
// ---------------------------------------------------------------------------

type fakeCrypto struct {
	mu        sync.Mutex
	calls     []string
	failGroup int // number of upcoming group encrypt calls to fail
	fetchErr  error
	fetches   int
	registers []string
}

func (c *fakeCrypto) record(call string) int {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.calls = append(c.calls, call)
	return len(c.calls)
}

func (c *fakeCrypto) snapshot() []string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]string(nil), c.calls...)
}

func (c *fakeCrypto) groupResult(n int) ([]string, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.failGroup > 0 {
		c.failGroup--
		return nil, e2ee.ErrGroupKeyNotLoaded
	}
	return []string{fmt.Sprintf("chunk-%d", n), "b", "c", "d", "e"}, nil
}

// installFakeCrypto replaces the send-path E2EE seams for one test.
func installFakeCrypto(t *testing.T) *fakeCrypto {
	t.Helper()
	fc := &fakeCrypto{}
	oldGroup, oldGroupRaw := e2eeEncryptGroupMessage, e2eeEncryptGroupMessageRaw
	oldV2Raw, oldKeyIDs := e2eeEncryptMessageV2Raw, e2eeMyKeyIDs
	oldRegister, oldFetch := lineAutoRegisterGroupKey, lineFetchAndUnwrapGroupKey
	oldNegotiate := negotiateE2EEPublicKeyWithClient
	t.Cleanup(func() {
		e2eeEncryptGroupMessage, e2eeEncryptGroupMessageRaw = oldGroup, oldGroupRaw
		e2eeEncryptMessageV2Raw, e2eeMyKeyIDs = oldV2Raw, oldKeyIDs
		lineAutoRegisterGroupKey, lineFetchAndUnwrapGroupKey = oldRegister, oldFetch
		negotiateE2EEPublicKeyWithClient = oldNegotiate
	})
	e2eeEncryptGroupMessage = func(_ *e2ee.Manager, chat, from, text string) ([]string, error) {
		return fc.groupResult(fc.record(fmt.Sprintf("group text %s %s %q", chat, from, text)))
	}
	e2eeEncryptGroupMessageRaw = func(_ *e2ee.Manager, chat, from string, contentType int, payload []byte) ([]string, error) {
		return fc.groupResult(fc.record(fmt.Sprintf("group raw %s %s %d %s", chat, from, contentType, payload)))
	}
	e2eeEncryptMessageV2Raw = func(_ *e2ee.Manager, chat, from string, myKeyID int, peerPub string, senderKeyID, receiverKeyID, contentType int, payload []byte) ([]string, error) {
		n := fc.record(fmt.Sprintf("direct %s %s key=%d peer=%s %d->%d %d %s", chat, from, myKeyID, peerPub, senderKeyID, receiverKeyID, contentType, payload))
		return []string{fmt.Sprintf("chunk-%d", n), "b", "c", "d", "e"}, nil
	}
	e2eeMyKeyIDs = func(*e2ee.Manager) (int, int, error) { return 7, 70, nil }
	lineAutoRegisterGroupKey = func(_ *LineClient, _ context.Context, chat string) error {
		fc.mu.Lock()
		defer fc.mu.Unlock()
		fc.registers = append(fc.registers, chat)
		return nil
	}
	lineFetchAndUnwrapGroupKey = func(_ *LineClient, _ context.Context, _ string, _ int) error {
		fc.mu.Lock()
		defer fc.mu.Unlock()
		fc.fetches++
		return fc.fetchErr
	}
	negotiateE2EEPublicKeyWithClient = func(_ *line.Client, mid string) (*line.E2EEPublicKey, error) {
		if mid == sendTestPeer {
			return &line.E2EEPublicKey{KeyID: json.Number("42"), PublicKey: "peer-pub"}, nil
		}
		return nil, line.ErrNoUsableE2EEPublicKey
	}
	return fc
}

// ---------------------------------------------------------------------------
// test client and Matrix messages
// ---------------------------------------------------------------------------

type fakeMediaBot struct {
	bridgev2.MatrixAPI
	media map[id.ContentURIString][]byte
}

func (b *fakeMediaBot) DownloadMedia(_ context.Context, uri id.ContentURIString, _ *event.EncryptedFileInfo) ([]byte, error) {
	data, ok := b.media[uri]
	if !ok {
		return nil, fmt.Errorf("no media %s", uri)
	}
	return data, nil
}

type sendTestEnv struct {
	lc     *LineClient
	fake   *fakeLine
	crypto *fakeCrypto
	bot    *fakeMediaBot
}

// newSendTestEnv builds a LineClient wired to the fake LINE transport.
// withE2EE decides whether the client has an E2EE manager at all.
func newSendTestEnv(t *testing.T, withE2EE bool) *sendTestEnv {
	t.Helper()
	line.InvalidateOBSTokenCache()
	fake := newFakeLine()
	oldNewClient := newLineAPIClient
	t.Cleanup(func() { newLineAPIClient = oldNewClient })
	newLineAPIClient = func(token string) *line.Client {
		client := line.NewClient(token)
		client.HTTPClient = &http.Client{Transport: fake}
		client.OBSClient = &http.Client{Transport: fake}
		return client
	}

	bot := &fakeMediaBot{media: map[id.ContentURIString][]byte{}}
	lc := &LineClient{
		Mid:         sendTestSelf,
		AccessToken: "access-token",
		UserLogin: &bridgev2.UserLogin{
			UserLogin: &database.UserLogin{ID: networkid.UserLoginID(sendTestSelf)},
			Bridge: &bridgev2.Bridge{
				Log: zerolog.New(io.Discard),
				Matrix: &mentionTestMatrix{ghosts: map[id.UserID]networkid.UserID{
					"@line_amin:example.com": "uamin",
				}},
				Bot: bot,
			},
		},
		blockedUsers: map[string]bool{},
	}
	env := &sendTestEnv{lc: lc, fake: fake, crypto: installFakeCrypto(t), bot: bot}
	if withE2EE {
		manager, err := e2ee.NewManager()
		if err != nil {
			t.Fatalf("e2ee.NewManager: %v", err)
		}
		lc.E2EE = manager
	}
	return env
}

func matrixMessage(chatMID string, content *event.MessageEventContent) *bridgev2.MatrixMessage {
	return &bridgev2.MatrixMessage{
		MatrixEventBase: bridgev2.MatrixEventBase[*event.MessageEventContent]{
			Event:   &event.Event{ID: "$event"},
			Content: content,
			Portal: &bridgev2.Portal{Portal: &database.Portal{
				PortalKey: networkid.PortalKey{ID: networkid.PortalID(chatMID)},
			}},
		},
	}
}

func textContent(body string) *event.MessageEventContent {
	return &event.MessageEventContent{MsgType: event.MsgText, Body: body}
}

func testPNG(t *testing.T) []byte {
	t.Helper()
	img := image.NewRGBA(image.Rect(0, 0, 40, 30))
	for x := 0; x < 40; x++ {
		for y := 0; y < 30; y++ {
			img.Set(x, y, color.RGBA{R: uint8(x * 6), G: uint8(y * 8), B: 90, A: 255})
		}
	}
	var buf bytes.Buffer
	if err := png.Encode(&buf, img); err != nil {
		t.Fatal(err)
	}
	return buf.Bytes()
}

func (env *sendTestEnv) addMedia(uri string, data []byte) id.ContentURIString {
	env.bot.media[id.ContentURIString(uri)] = data
	return id.ContentURIString(uri)
}

func assertMethods(t *testing.T, fake *fakeLine, want ...string) {
	t.Helper()
	got := fake.methods()
	if strings.Join(got, "\n") != strings.Join(want, "\n") {
		t.Fatalf("LINE calls\n got: %q\nwant: %q", got, want)
	}
}

func assertCrypto(t *testing.T, crypto *fakeCrypto, want ...string) {
	t.Helper()
	got := crypto.snapshot()
	if strings.Join(got, "\n") != strings.Join(want, "\n") {
		t.Fatalf("E2EE calls\n got: %q\nwant: %q", got, want)
	}
}

func assertTracked(t *testing.T, lc *LineClient, seqs ...int) {
	t.Helper()
	for _, seq := range seqs {
		if !lc.consumeSentReqSeq(seq) {
			t.Fatalf("reqSeq %d was not tracked for own-echo filtering", seq)
		}
	}
}

func assertDistinctSeqs(t *testing.T, sent []sentLineMessage) {
	t.Helper()
	seen := map[int]bool{}
	for _, s := range sent {
		if seen[s.ReqSeq] {
			t.Fatalf("reqSeq %d used twice: %+v", s.ReqSeq, sent)
		}
		seen[s.ReqSeq] = true
	}
}

// ---------------------------------------------------------------------------
// text
// ---------------------------------------------------------------------------

func TestSendCorePlainTextGroup(t *testing.T) {
	env := newSendTestEnv(t, false)
	resp, err := env.lc.HandleMatrixMessage(t.Context(), matrixMessage(sendTestGroup, textContent("hello")))
	if err != nil {
		t.Fatalf("HandleMatrixMessage: %v", err)
	}
	assertMethods(t, env.fake, "sendMessage")
	sent := env.fake.sentMessages(t)[0]
	msg := sent.Msg
	if msg.Text != "hello" || len(msg.Chunks) != 0 || msg.ContentType != int(ContentText) || msg.HasContent {
		t.Fatalf("plain text message = %+v", msg)
	}
	if msg.To != sendTestGroup || msg.ToType != int(ToGroup) || msg.From != sendTestSelf || !strings.HasPrefix(msg.ID, "local-") {
		t.Fatalf("routing fields = %+v", msg)
	}
	if _, ok := msg.ContentMetadata["e2eeVersion"]; ok || len(msg.ContentMetadata) != 0 {
		t.Fatalf("plain text metadata = %v", msg.ContentMetadata)
	}
	if msg.RelatedMessageID != "" {
		t.Fatalf("unexpected reply relation: %+v", msg)
	}
	assertTracked(t, env.lc, sent.ReqSeq)
	if resp.DB.ID != "srv-1" || resp.DB.SenderID != networkid.UserID(sendTestSelf) || resp.DB.Timestamp.IsZero() {
		t.Fatalf("response = %+v", resp.DB)
	}
	assertCrypto(t, env.crypto)
}

func TestSendCoreE2EETextGroup(t *testing.T) {
	env := newSendTestEnv(t, true)
	resp, err := env.lc.HandleMatrixMessage(t.Context(), matrixMessage(sendTestGroup, textContent("hello")))
	if err != nil {
		t.Fatalf("HandleMatrixMessage: %v", err)
	}
	assertMethods(t, env.fake, "sendMessage")
	assertCrypto(t, env.crypto, fmt.Sprintf("group text %s %s %q", sendTestGroup, sendTestSelf, "hello"))
	if env.crypto.fetches != 1 {
		t.Fatalf("group key fetches = %d, want 1", env.crypto.fetches)
	}
	msg := env.fake.sentMessages(t)[0].Msg
	if msg.Text != "" || len(msg.Chunks) != 5 || msg.Chunks[0] != "chunk-1" || msg.ContentMetadata["e2eeVersion"] != "2" {
		t.Fatalf("E2EE text message = %+v", msg)
	}
	if resp.DB.ID != "srv-1" {
		t.Fatalf("response id = %q", resp.DB.ID)
	}
}

func TestSendCoreE2EEGroupFallsBackToPlainText(t *testing.T) {
	env := newSendTestEnv(t, true)
	env.crypto.failGroup = 2
	_, err := env.lc.HandleMatrixMessage(t.Context(), matrixMessage(sendTestGroup, textContent("hello")))
	if err != nil {
		t.Fatalf("HandleMatrixMessage: %v", err)
	}
	assertCrypto(t, env.crypto,
		fmt.Sprintf("group text %s %s %q", sendTestGroup, sendTestSelf, "hello"),
		fmt.Sprintf("group text %s %s %q", sendTestGroup, sendTestSelf, "hello"),
	)
	if env.crypto.fetches != 2 {
		t.Fatalf("group key fetches = %d, want 2 (initial + retry)", env.crypto.fetches)
	}
	msg := env.fake.sentMessages(t)[0].Msg
	if msg.Text != "hello" || len(msg.Chunks) != 0 || msg.ContentMetadata["e2eeVersion"] != "" {
		t.Fatalf("fallback message = %+v", msg)
	}
	if !env.lc.isGroupNoE2EE(sendTestGroup) {
		t.Fatal("group was not cached as no-E2EE after fallback")
	}

	// The cached decision skips E2EE entirely on the next send.
	_, err = env.lc.HandleMatrixMessage(t.Context(), matrixMessage(sendTestGroup, textContent("again")))
	if err != nil {
		t.Fatalf("second send: %v", err)
	}
	if len(env.crypto.snapshot()) != 2 || env.crypto.fetches != 2 {
		t.Fatalf("no-E2EE cache ignored: crypto=%q fetches=%d", env.crypto.snapshot(), env.crypto.fetches)
	}
}

func TestSendCoreGroupKeyFetchAbortsOnMissingOwnKey(t *testing.T) {
	env := newSendTestEnv(t, true)
	env.crypto.fetchErr = fmt.Errorf("unwrap: %w", e2ee.ErrMissingOwnPrivateKey)
	_, err := env.lc.HandleMatrixMessage(t.Context(), matrixMessage(sendTestGroup, textContent("hello")))
	var status bridgev2.MessageStatus
	if !errors.As(err, &status) || !status.SendNotice || status.Message != lineGroupE2EEReconnectNotice {
		t.Fatalf("error = %#v, want reconnect notice status", err)
	}
	assertMethods(t, env.fake)
}

func TestSendCoreDirectE2EEText(t *testing.T) {
	env := newSendTestEnv(t, true)
	_, err := env.lc.HandleMatrixMessage(t.Context(), matrixMessage(sendTestPeer, textContent("hi")))
	if err != nil {
		t.Fatalf("HandleMatrixMessage: %v", err)
	}
	assertCrypto(t, env.crypto, fmt.Sprintf("direct %s %s key=70 peer=peer-pub 7->42 0 {\"text\":\"hi\"}", sendTestPeer, sendTestSelf))
	msg := env.fake.sentMessages(t)[0].Msg
	if msg.ToType != int(ToUser) || len(msg.Chunks) != 5 || msg.Text != "" || msg.ContentMetadata["e2eeVersion"] != "2" {
		t.Fatalf("direct E2EE message = %+v", msg)
	}
}

func TestSendCoreDirectLetterSealingOffSendsPlain(t *testing.T) {
	env := newSendTestEnv(t, true)
	const plainPeer = "uplain000000000000000000000000000"
	_, err := env.lc.HandleMatrixMessage(t.Context(), matrixMessage(plainPeer, textContent("hi")))
	if err != nil {
		t.Fatalf("HandleMatrixMessage: %v", err)
	}
	assertCrypto(t, env.crypto)
	msg := env.fake.sentMessages(t)[0].Msg
	if msg.Text != "hi" || len(msg.Chunks) != 0 || msg.ToType != int(ToUser) {
		t.Fatalf("plain direct message = %+v", msg)
	}
}

func TestSendCoreBlockedDirectIsRejected(t *testing.T) {
	env := newSendTestEnv(t, true)
	env.lc.blockedUsers[sendTestPeer] = true
	_, err := env.lc.HandleMatrixMessage(t.Context(), matrixMessage(sendTestPeer, textContent("hi")))
	var status bridgev2.MessageStatus
	if !errors.As(err, &status) || !status.IsCertain || !status.SendNotice || status.ErrorReason != event.MessageStatusGenericError {
		t.Fatalf("error = %#v, want blocked status", err)
	}
	if !strings.Contains(err.Error(), "user is blocked on LINE") {
		t.Fatalf("error text = %q", err)
	}
	assertMethods(t, env.fake)
	assertCrypto(t, env.crypto)
}

// ---------------------------------------------------------------------------
// mention and reply
// ---------------------------------------------------------------------------

func TestSendCoreMentionMetadata(t *testing.T) {
	env := newSendTestEnv(t, false)
	content := textContent("@阿明 這張你出")
	content.Format = event.FormatHTML
	content.FormattedBody = `<a href="https://matrix.to/#/@line_amin:example.com">阿明</a> 這張你出`
	content.Mentions = &event.Mentions{UserIDs: []id.UserID{"@line_amin:example.com"}}
	if _, err := env.lc.HandleMatrixMessage(t.Context(), matrixMessage(sendTestGroup, content)); err != nil {
		t.Fatalf("HandleMatrixMessage: %v", err)
	}
	msg := env.fake.sentMessages(t)[0].Msg
	if got := msg.ContentMetadata["MENTION"]; got != `{"MENTIONEES":[{"S":"0","E":"3","M":"uamin"}]}` {
		t.Fatalf("MENTION = %s", got)
	}
	if msg.Text != "@阿明 這張你出" {
		t.Fatalf("text = %q", msg.Text)
	}

	// @all in the body becomes a room mention even without m.mentions.
	env2 := newSendTestEnv(t, false)
	content2 := textContent("@all 集合")
	if _, err := env2.lc.HandleMatrixMessage(t.Context(), matrixMessage(sendTestGroup, content2)); err != nil {
		t.Fatalf("HandleMatrixMessage: %v", err)
	}
	msg2 := env2.fake.sentMessages(t)[0].Msg
	if got := msg2.ContentMetadata["MENTION"]; got != `{"MENTIONEES":[{"S":"0","E":"4","A":"1"}]}` {
		t.Fatalf("room MENTION = %s", got)
	}
	if content2.Mentions == nil || !content2.Mentions.Room {
		t.Fatalf("Matrix content mentions not marked as room: %+v", content2.Mentions)
	}
}

func TestSendCoreReplyRelation(t *testing.T) {
	env := newSendTestEnv(t, false)
	mm := matrixMessage(sendTestGroup, textContent("收到"))
	mm.ReplyTo = &database.Message{ID: "600000000000000123"}
	if _, err := env.lc.HandleMatrixMessage(t.Context(), mm); err != nil {
		t.Fatalf("HandleMatrixMessage: %v", err)
	}
	msg := env.fake.sentMessages(t)[0].Msg
	if msg.RelatedMessageID != "600000000000000123" || msg.MessageRelationType != 3 || msg.RelatedMessageServiceCode != 1 {
		t.Fatalf("reply relation = %+v", msg)
	}

	// A reply to a not-yet-confirmed local echo carries no relation.
	env2 := newSendTestEnv(t, false)
	mm2 := matrixMessage(sendTestGroup, textContent("收到"))
	mm2.ReplyTo = &database.Message{ID: "local-1727654321000"}
	if _, err := env2.lc.HandleMatrixMessage(t.Context(), mm2); err != nil {
		t.Fatalf("HandleMatrixMessage: %v", err)
	}
	if msg2 := env2.fake.sentMessages(t)[0].Msg; msg2.RelatedMessageID != "" || msg2.MessageRelationType != 0 {
		t.Fatalf("local reply relation = %+v", msg2)
	}
}

func TestSendCoreReplyTargetMissingFallsBackWithoutRelation(t *testing.T) {
	env := newSendTestEnv(t, false)
	env.fake.failNextSend(400, talkExceptionNotFound)
	mm := matrixMessage(sendTestGroup, textContent("收到"))
	mm.ReplyTo = &database.Message{ID: "600000000000000123"}
	resp, err := env.lc.HandleMatrixMessage(t.Context(), mm)
	if err != nil {
		t.Fatalf("HandleMatrixMessage: %v", err)
	}
	sent := env.fake.sentMessages(t)
	if len(sent) != 2 {
		t.Fatalf("sendMessage calls = %d, want 2", len(sent))
	}
	if sent[0].Msg.RelatedMessageID != "600000000000000123" {
		t.Fatalf("first attempt lost the relation: %+v", sent[0].Msg)
	}
	if sent[1].Msg.RelatedMessageID != "" || sent[1].Msg.MessageRelationType != 0 || sent[1].Msg.RelatedMessageServiceCode != 0 {
		t.Fatalf("retry kept the relation: %+v", sent[1].Msg)
	}
	assertDistinctSeqs(t, sent)
	assertTracked(t, env.lc, sent[0].ReqSeq, sent[1].ReqSeq)
	if resp.DB.ID != "srv-1" {
		t.Fatalf("response id = %q", resp.DB.ID)
	}
}

// ---------------------------------------------------------------------------
// code 99: group key not registered
// ---------------------------------------------------------------------------

func TestSendCoreGroupKeyNotRegisteredRegistersAndRetries(t *testing.T) {
	env := newSendTestEnv(t, true)
	env.fake.failNextSend(400, talkExceptionNoGroupK)
	_, err := env.lc.HandleMatrixMessage(t.Context(), matrixMessage(sendTestGroup, textContent("hello")))
	if err != nil {
		t.Fatalf("HandleMatrixMessage: %v", err)
	}
	if len(env.crypto.registers) != 1 || env.crypto.registers[0] != sendTestGroup {
		t.Fatalf("group key registrations = %v", env.crypto.registers)
	}
	if env.crypto.fetches != 2 {
		t.Fatalf("group key fetches = %d, want 2", env.crypto.fetches)
	}
	sent := env.fake.sentMessages(t)
	if len(sent) != 2 || sent[0].Msg.Chunks[0] != "chunk-1" || sent[1].Msg.Chunks[0] != "chunk-2" || sent[1].Msg.Text != "" {
		t.Fatalf("code 99 retry messages = %+v", sent)
	}
	assertDistinctSeqs(t, sent)
	assertTracked(t, env.lc, sent[0].ReqSeq, sent[1].ReqSeq)
}

// In plaintext mode the registration still runs, but the message is not
// resent and the code 99 error is returned (existing behaviour).
func TestSendCoreGroupKeyNotRegisteredInPlainModeDoesNotResend(t *testing.T) {
	env := newSendTestEnv(t, false)
	env.fake.failNextSend(400, talkExceptionNoGroupK)
	_, err := env.lc.HandleMatrixMessage(t.Context(), matrixMessage(sendTestGroup, textContent("hello")))
	if err == nil || !line.IsGroupKeyNotRegisteredError(err) {
		t.Fatalf("error = %v, want code 99", err)
	}
	if len(env.crypto.registers) != 1 {
		t.Fatalf("group key registrations = %v", env.crypto.registers)
	}
	if n := len(env.fake.sentMessages(t)); n != 1 {
		t.Fatalf("sendMessage calls = %d, want 1", n)
	}
}

// ---------------------------------------------------------------------------
// media
// ---------------------------------------------------------------------------

func TestSendCorePlainImageSendsThenUploads(t *testing.T) {
	env := newSendTestEnv(t, false)
	data := testPNG(t)
	content := &event.MessageEventContent{
		MsgType:  event.MsgImage,
		Body:     "car.png",
		FileName: "car.png",
		URL:      env.addMedia("mxc://example.com/car", data),
		Info:     &event.FileInfo{MimeType: "image/png"},
	}
	resp, err := env.lc.HandleMatrixMessage(t.Context(), matrixMessage(sendTestGroup, content))
	if err != nil {
		t.Fatalf("HandleMatrixMessage: %v", err)
	}
	assertMethods(t, env.fake,
		"sendMessage",
		"acquireEncryptedAccessToken",
		"OBS /r/talk/m/srv-1",
		"OBS /r/talk/m/srv-1__ud-preview",
	)
	msg := env.fake.sentMessages(t)[0].Msg
	meta := msg.ContentMetadata
	if msg.ContentType != int(ContentImage) || !msg.HasContent || len(msg.Chunks) != 0 {
		t.Fatalf("plain image message = %+v", msg)
	}
	if meta["FILE_NAME"] != "car.png" || meta["FILE_SIZE"] != fmt.Sprint(len(data)) || meta["contentType"] != "1" ||
		meta["MEDIA_THUMB_INFO"] == "" || meta["OID"] != "" || meta["ENC_KM"] != "" || meta["e2eeVersion"] != "" {
		t.Fatalf("plain image metadata = %v", meta)
	}
	if meta["MEDIA_CONTENT_INFO"] != fmt.Sprintf(`{"category":"original","extension":"png","fileSize":%d}`, len(data)) {
		t.Fatalf("MEDIA_CONTENT_INFO = %s", meta["MEDIA_CONTENT_INFO"])
	}
	calls := env.fake.snapshot()
	if !bytes.Equal(calls[2].Body, data) || calls[2].ObsType != "image" || calls[3].ObsType != "image" {
		t.Fatalf("plain upload = type %q, %d bytes", calls[2].ObsType, len(calls[2].Body))
	}
	if resp.DB.ID != "srv-1" {
		t.Fatalf("response id = %q", resp.DB.ID)
	}
}

func TestSendCoreE2EEImageUploadsThenSends(t *testing.T) {
	env := newSendTestEnv(t, true)
	data := testPNG(t)
	content := &event.MessageEventContent{
		MsgType: event.MsgImage,
		Body:    "image",
		URL:     env.addMedia("mxc://example.com/car", data),
		Info:    &event.FileInfo{MimeType: "image/jpeg"},
	}
	if _, err := env.lc.HandleMatrixMessage(t.Context(), matrixMessage(sendTestGroup, content)); err != nil {
		t.Fatalf("HandleMatrixMessage: %v", err)
	}
	methods := env.fake.methods()
	if len(methods) != 5 || methods[0] != "determineMediaMessageFlow" || methods[1] != "acquireEncryptedAccessToken" ||
		!strings.HasPrefix(methods[2], "OBS /r/talk/emi/reqid-") || methods[3] != "OBS /r/talk/emi/oid-1__ud-preview" ||
		methods[4] != "sendMessage" {
		t.Fatalf("LINE calls = %q", methods)
	}
	msg := env.fake.sentMessages(t)[0].Msg
	meta := msg.ContentMetadata
	if meta["OID"] != "oid-1" || meta["SID"] != "emi" || meta["ENC_KM"] == "" || meta["e2eeVersion"] != "2" ||
		meta["FILE_NAME"] != "image" || meta["contentType"] != "1" || meta["MEDIA_THUMB_INFO"] == "" {
		t.Fatalf("E2EE image metadata = %v", meta)
	}
	uploaded := env.fake.snapshot()[2].Body
	if meta["FILE_SIZE"] != fmt.Sprint(len(uploaded)) || bytes.Equal(uploaded, data) {
		t.Fatalf("upload was not the encrypted payload: FILE_SIZE=%s uploaded=%d", meta["FILE_SIZE"], len(uploaded))
	}
	payload, _ := json.Marshal(map[string]string{"keyMaterial": meta["ENC_KM"]})
	assertCrypto(t, env.crypto, fmt.Sprintf("group raw %s %s 1 %s", sendTestGroup, sendTestSelf, payload))
	if len(msg.Chunks) != 5 || msg.Text != "" {
		t.Fatalf("E2EE image message = %+v", msg)
	}
}

// When the server says this chat uses the plain media flow, an E2EE-capable
// client still sends media in plaintext.
func TestSendCoreServerSelectedPlainMediaFlow(t *testing.T) {
	env := newSendTestEnv(t, true)
	env.fake.mediaFlow = map[string]int{"1": 1}
	content := &event.MessageEventContent{
		MsgType: event.MsgImage,
		Body:    "image",
		URL:     env.addMedia("mxc://example.com/car", testPNG(t)),
		Info:    &event.FileInfo{MimeType: "image/png"},
	}
	if _, err := env.lc.HandleMatrixMessage(t.Context(), matrixMessage(sendTestGroup, content)); err != nil {
		t.Fatalf("HandleMatrixMessage: %v", err)
	}
	assertMethods(t, env.fake,
		"determineMediaMessageFlow",
		"sendMessage",
		"acquireEncryptedAccessToken",
		"OBS /r/talk/m/srv-1",
		"OBS /r/talk/m/srv-1__ud-preview",
	)
	assertCrypto(t, env.crypto)
	if env.crypto.fetches != 0 {
		t.Fatalf("group key fetched in plain media flow")
	}
}

// Matrix clients often send media as m.file; the MIME type decides.
func TestSendCoreFileWithAudioMimeIsSentAsAudio(t *testing.T) {
	env := newSendTestEnv(t, false)
	data := []byte("fake-m4a-bytes")
	content := &event.MessageEventContent{
		MsgType: event.MsgFile,
		Body:    "voice.m4a",
		URL:     env.addMedia("mxc://example.com/voice", data),
		Info:    &event.FileInfo{MimeType: "audio/mp4", Duration: 4200},
	}
	if _, err := env.lc.HandleMatrixMessage(t.Context(), matrixMessage(sendTestGroup, content)); err != nil {
		t.Fatalf("HandleMatrixMessage: %v", err)
	}
	assertMethods(t, env.fake, "sendMessage", "acquireEncryptedAccessToken", "OBS /r/talk/m/srv-1")
	msg := env.fake.sentMessages(t)[0].Msg
	if msg.ContentType != int(ContentAudio) || msg.ContentMetadata["DURATION"] != "4200" || msg.ContentMetadata["AUDLEN"] != "4200" {
		t.Fatalf("audio message = %+v", msg)
	}
	if call := env.fake.snapshot()[2]; call.ObsType != "audio" || !bytes.Equal(call.Body, data) {
		t.Fatalf("audio upload = %+v", call)
	}
}

func TestSendCorePlainFileZipFallback(t *testing.T) {
	env := newSendTestEnv(t, false)
	env.fake.failNextSend(200, extensionRejectsFile)
	data := []byte("PK-not-really-a-zip but some file bytes")
	content := &event.MessageEventContent{
		MsgType:  event.MsgFile,
		Body:     "report.xyz",
		FileName: "report.xyz",
		URL:      env.addMedia("mxc://example.com/report", data),
		Info:     &event.FileInfo{MimeType: "application/octet-stream"},
	}
	if _, err := env.lc.HandleMatrixMessage(t.Context(), matrixMessage(sendTestGroup, content)); err != nil {
		t.Fatalf("HandleMatrixMessage: %v", err)
	}
	assertMethods(t, env.fake, "sendMessage", "sendMessage", "acquireEncryptedAccessToken", "OBS /r/talk/m/srv-1")
	sent := env.fake.sentMessages(t)
	if sent[0].Msg.ContentMetadata["FILE_NAME"] != "report.xyz" || sent[1].Msg.ContentMetadata["FILE_NAME"] != "report.xyz.zip" {
		t.Fatalf("zip retry file names = %q, %q", sent[0].Msg.ContentMetadata["FILE_NAME"], sent[1].Msg.ContentMetadata["FILE_NAME"])
	}
	assertDistinctSeqs(t, sent)
	upload := env.fake.snapshot()[3]
	if upload.ObsType != "file" || sent[1].Msg.ContentMetadata["FILE_SIZE"] != fmt.Sprint(len(upload.Body)) {
		t.Fatalf("zip upload = type %q size %d, FILE_SIZE %s", upload.ObsType, len(upload.Body), sent[1].Msg.ContentMetadata["FILE_SIZE"])
	}
	zr, err := zip.NewReader(bytes.NewReader(upload.Body), int64(len(upload.Body)))
	if err != nil || len(zr.File) != 1 || zr.File[0].Name != "report.xyz" {
		t.Fatalf("uploaded data is not the wrapped file: %v", err)
	}
	rc, _ := zr.File[0].Open()
	inner, _ := io.ReadAll(rc)
	if !bytes.Equal(inner, data) {
		t.Fatal("zip content differs from the original file")
	}
}

func TestSendCoreE2EEFileZipFallback(t *testing.T) {
	env := newSendTestEnv(t, true)
	env.fake.failNextSend(200, extensionRejectsFile)
	content := &event.MessageEventContent{
		MsgType:  event.MsgFile,
		Body:     "report.xyz",
		FileName: "report.xyz",
		URL:      env.addMedia("mxc://example.com/report", []byte("file bytes")),
		Info:     &event.FileInfo{MimeType: "application/octet-stream"},
	}
	if _, err := env.lc.HandleMatrixMessage(t.Context(), matrixMessage(sendTestGroup, content)); err != nil {
		t.Fatalf("HandleMatrixMessage: %v", err)
	}
	methods := env.fake.methods()
	if len(methods) != 6 || methods[0] != "determineMediaMessageFlow" || methods[1] != "acquireEncryptedAccessToken" ||
		!strings.HasPrefix(methods[2], "OBS /r/talk/emf/reqid-") || methods[3] != "sendMessage" ||
		!strings.HasPrefix(methods[4], "OBS /r/talk/emf/reqid-") || methods[5] != "sendMessage" {
		t.Fatalf("LINE calls = %q", methods)
	}
	sent := env.fake.sentMessages(t)
	first, second := sent[0].Msg.ContentMetadata, sent[1].Msg.ContentMetadata
	if first["OID"] != "oid-1" || second["OID"] != "oid-2" || second["FILE_NAME"] != "report.xyz.zip" || first["ENC_KM"] == second["ENC_KM"] {
		t.Fatalf("zip retry metadata: first=%v second=%v", first, second)
	}
	crypto := env.crypto.snapshot()
	if len(crypto) != 2 || !strings.Contains(crypto[0], `"fileName":"report.xyz"`) || !strings.Contains(crypto[1], `"fileName":"report.xyz.zip"`) {
		t.Fatalf("E2EE calls = %q", crypto)
	}
	if sent[1].Msg.Chunks[0] != "chunk-2" {
		t.Fatalf("retry did not use the re-encrypted payload: %+v", sent[1].Msg)
	}
}

// ---------------------------------------------------------------------------
// DIVA reply_text
// ---------------------------------------------------------------------------

func TestSendCoreDIVAReplyTextUsesGroupE2EE(t *testing.T) {
	env := newSendTestEnv(t, true)
	env.lc.UserLogin.Bridge.Log = zerolog.New(&divaSyncBuffer{})
	startDIVAReplyingWorker(t, "")

	env.lc.handleDIVAInbound(&line.Message{
		ID: "600000000000000001", From: "udriver", To: sendTestGroup, ToType: int(ToGroup), ContentType: int(ContentText),
	}, sendTestGroup, "123456654", false, int(OpReceiveMessage), divaOriginLive)

	select {
	case <-env.fake.sent:
	case <-time.After(5 * time.Second):
		t.Fatalf("reply_text was not sent to LINE; calls=%q", env.fake.methods())
	}
	sent := env.fake.sentMessages(t)
	if len(sent) != 1 {
		t.Fatalf("sendMessage calls = %d", len(sent))
	}
	msg := sent[0].Msg
	if msg.To != sendTestGroup || msg.ToType != int(ToGroup) || msg.ContentType != int(ContentText) ||
		msg.Text != "" || len(msg.Chunks) != 5 || msg.ContentMetadata["e2eeVersion"] != "2" || msg.RelatedMessageID != "" {
		t.Fatalf("reply_text message = %+v", msg)
	}
	assertCrypto(t, env.crypto, fmt.Sprintf("group text %s %s %q", sendTestGroup, sendTestSelf, "789"))
	assertTracked(t, env.lc, sent[0].ReqSeq)
}

func TestSendCoreDIVAReplyTextPlainGroup(t *testing.T) {
	env := newSendTestEnv(t, false)
	startDIVAReplyingWorker(t, "")

	env.lc.handleDIVAInbound(&line.Message{
		ID: "600000000000000002", From: "udriver", To: sendTestGroup, ToType: int(ToGroup), ContentType: int(ContentText),
	}, sendTestGroup, "123456654", false, int(OpReceiveMessage), divaOriginLive)

	select {
	case <-env.fake.sent:
	case <-time.After(5 * time.Second):
		t.Fatalf("reply_text was not sent to LINE; calls=%q", env.fake.methods())
	}
	msg := env.fake.sentMessages(t)[0].Msg
	if msg.Text != "789" || len(msg.Chunks) != 0 || msg.ContentMetadata["e2eeVersion"] != "" {
		t.Fatalf("plain reply_text message = %+v", msg)
	}
}

func TestSendCoreE2EEVideoUploadsHashAndPreview(t *testing.T) {
	env := newSendTestEnv(t, true)
	data := bytes.Repeat([]byte("not-a-real-mp4-"), 200)
	content := &event.MessageEventContent{
		MsgType: event.MsgVideo,
		Body:    "clip.mp4",
		URL:     env.addMedia("mxc://example.com/clip", data),
		Info:    &event.FileInfo{MimeType: "video/mp4", Duration: 3000},
	}
	if _, err := env.lc.HandleMatrixMessage(t.Context(), matrixMessage(sendTestGroup, content)); err != nil {
		t.Fatalf("HandleMatrixMessage: %v", err)
	}
	methods := env.fake.methods()
	if len(methods) != 6 || methods[0] != "determineMediaMessageFlow" || methods[1] != "acquireEncryptedAccessToken" ||
		!strings.HasPrefix(methods[2], "OBS /r/talk/emv/reqid-") || methods[3] != "OBS /r/talk/emv/oid-1__ud-hash" ||
		methods[4] != "OBS /r/talk/emv/oid-1__ud-preview" || methods[5] != "sendMessage" {
		t.Fatalf("LINE calls = %q", methods)
	}
	msg := env.fake.sentMessages(t)[0].Msg
	meta := msg.ContentMetadata
	if msg.ContentType != int(ContentVideo) || meta["OID"] != "oid-1" || meta["SID"] != "emv" || meta["DURATION"] != "3000" ||
		meta["FILE_SIZE"] != fmt.Sprint(len(data)) || meta["contentType"] != "2" || meta["MEDIA_THUMB_INFO"] == "" {
		t.Fatalf("E2EE video message = %+v", msg)
	}
	payload, _ := json.Marshal(map[string]string{"keyMaterial": meta["ENC_KM"]})
	assertCrypto(t, env.crypto, fmt.Sprintf("group raw %s %s 2 %s", sendTestGroup, sendTestSelf, payload))
}

func TestSendCorePlainVideoSendsThenUploads(t *testing.T) {
	env := newSendTestEnv(t, false)
	data := bytes.Repeat([]byte("not-a-real-mp4-"), 50)
	content := &event.MessageEventContent{
		MsgType: event.MsgVideo,
		Body:    "clip.mp4",
		URL:     env.addMedia("mxc://example.com/clip", data),
		Info:    &event.FileInfo{MimeType: "video/mp4", Duration: 3000},
	}
	if _, err := env.lc.HandleMatrixMessage(t.Context(), matrixMessage(sendTestGroup, content)); err != nil {
		t.Fatalf("HandleMatrixMessage: %v", err)
	}
	assertMethods(t, env.fake,
		"sendMessage",
		"acquireEncryptedAccessToken",
		"OBS /r/talk/m/srv-1",
		"OBS /r/talk/m/srv-1__ud-preview",
	)
	if call := env.fake.snapshot()[2]; call.ObsType != "video" || !bytes.Equal(call.Body, data) {
		t.Fatalf("plain video upload = type %q, %d bytes", call.ObsType, len(call.Body))
	}
	if meta := env.fake.sentMessages(t)[0].Msg.ContentMetadata; meta["DURATION"] != "3000" || meta["MEDIA_THUMB_INFO"] == "" {
		t.Fatalf("plain video metadata = %v", meta)
	}
}

func TestSendCoreE2EEAudioUploadsThenSends(t *testing.T) {
	env := newSendTestEnv(t, true)
	content := &event.MessageEventContent{
		MsgType: event.MsgAudio,
		Body:    "voice.m4a",
		URL:     env.addMedia("mxc://example.com/voice", []byte("fake-m4a-bytes")),
		Info:    &event.FileInfo{MimeType: "audio/mp4", Duration: 4200},
	}
	if _, err := env.lc.HandleMatrixMessage(t.Context(), matrixMessage(sendTestGroup, content)); err != nil {
		t.Fatalf("HandleMatrixMessage: %v", err)
	}
	methods := env.fake.methods()
	if len(methods) != 4 || methods[0] != "determineMediaMessageFlow" || !strings.HasPrefix(methods[2], "OBS /r/talk/ema/reqid-") || methods[3] != "sendMessage" {
		t.Fatalf("LINE calls = %q", methods)
	}
	meta := env.fake.sentMessages(t)[0].Msg.ContentMetadata
	if meta["SID"] != "ema" || meta["OID"] != "oid-1" || meta["AUDLEN"] != "4200" {
		t.Fatalf("E2EE audio metadata = %v", meta)
	}
	payload, _ := json.Marshal(map[string]string{"keyMaterial": meta["ENC_KM"]})
	assertCrypto(t, env.crypto, fmt.Sprintf("group raw %s %s 3 %s", sendTestGroup, sendTestSelf, payload))
}
