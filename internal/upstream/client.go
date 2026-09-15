// Package upstream 封装对 LobsterAI 上游的全部 HTTP 调用。
package upstream

import (
	"bufio"
	"bytes"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"strings"
	"time"

	"lobsterai2api/internal/auth"
)

const (
	clientVersion = "0.1.0"
	clientUA      = "LobsterAI/0.1.0"
)

// ServerBase returns the upstream API base URL from LB2A_UPSTREAM_BASE env.
// No hardcoded domain — users must set this in their config or environment.
func ServerBase() string {
	if v := os.Getenv("LB2A_UPSTREAM_BASE"); v != "" {
		return strings.TrimRight(v, "/")
	}
	return ""
}

// apiEnvelope 上游统一信封。
type apiEnvelope struct {
	Code int             `json:"code"`
	Msg  string          `json:"msg"`
	Data json.RawMessage `json:"data"`
}

// Client 上游 HTTP 客户端。
type Client struct {
	HTTP     *http.Client
	LastBody []byte // 最近一次非 2xx 响应体，供调用方 Classify
}

// New 生产默认值。配置连接池减少 TLS 握手。
func New() *Client {
	tr := &http.Transport{
		MaxIdleConns:        100,
		MaxIdleConnsPerHost: 20,
		IdleConnTimeout:     90 * time.Second,
	}
	return &Client{
		HTTP: &http.Client{Timeout: 180 * time.Second, Transport: tr},
	}
}

func truncate(s string, n int) string {
	s = strings.TrimSpace(s)
	if len(s) > n {
		return s[:n]
	}
	return s
}

// doJSON 发请求并解信封；HTTP 非 2xx 或业务 code != 0 时返回带 body 片段的 *Error。
func (c *Client) doJSON(req *http.Request) (json.RawMessage, error) {
	resp, err := c.HTTP.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	c.LastBody = raw
	if resp.StatusCode >= 400 {
		kind := Classify(resp.StatusCode, string(raw))
		return nil, &Error{Kind: kind, Status: resp.StatusCode, Msg: truncate(string(raw), 200)}
	}
	var env apiEnvelope
	if err := json.Unmarshal(raw, &env); err != nil {
		return nil, fmt.Errorf("parse failed: %w (body: %s)", err, truncate(string(raw), 120))
	}
	if env.Code != 0 {
		kind := Classify(resp.StatusCode, env.Msg)
		if kind == ErrNone {
			kind = ErrClient
		}
		return nil, &Error{Kind: kind, Status: resp.StatusCode, Msg: fmt.Sprintf("code=%d msg=%s", env.Code, truncate(env.Msg, 160))}
	}
	return env.Data, nil
}

// chatHeaders 设置 chat completions 请求头。
func chatHeaders(req *http.Request, a *auth.Auth) {
	req.Header.Set("Authorization", "Bearer "+a.AccessToken)
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "text/event-stream, application/json")
	req.Header.Set("User-Agent", clientUA)
	req.Header.Set("X-LobsterAI-Client-Capabilities", "kimi-k3-agentic-v1")
	req.Header.Set("X-LobsterAI-Client-Version", clientVersion)
}

// authHeaders 设置 auth 请求头（exchange/refresh 不需要 Bearer token）。
func authHeaders(req *http.Request) {
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json")
	req.Header.Set("User-Agent", clientUA)
}

// RefreshToken 刷新 access token；成功时更新 a 的字段（缺省值保留旧值），
// 调用方负责 SaveAtomic。
func (c *Client) RefreshToken(a *auth.Auth) error {
	if strings.TrimSpace(a.RefreshToken) == "" {
		return fmt.Errorf("no refreshToken")
	}
	url := ServerBase() + "/api/auth/refresh"
	body := a.KeyfromBody()
	body["refreshToken"] = a.RefreshToken
	raw, _ := json.Marshal(body)
	req, err := http.NewRequest(http.MethodPost, url, bytes.NewReader(raw))
	if err != nil {
		return err
	}
	authHeaders(req)
	data, err := c.doJSON(req)
	if err != nil {
		return err
	}
	var tok struct {
		AccessToken  string `json:"accessToken"`
		RefreshToken string `json:"refreshToken"`
		ExpiresIn    int64  `json:"expiresIn"`
	}
	if err := json.Unmarshal(data, &tok); err != nil || tok.AccessToken == "" {
		return fmt.Errorf("refresh_failed: no accessToken in response — re-login required")
	}
	a.AccessToken = tok.AccessToken
	if tok.RefreshToken != "" {
		a.RefreshToken = tok.RefreshToken
	}
	if tok.ExpiresIn > 0 {
		a.ExpiresAt = time.Now().Add(time.Duration(tok.ExpiresIn) * time.Second).Unix()
	} else if exp := jwtExpiry(tok.AccessToken); exp > 0 {
		a.ExpiresAt = exp
	}
	return nil
}

// jwtExpiry 解码 JWT payload 的 exp（Unix 秒）；失败返回 0。
func jwtExpiry(token string) int64 {
	parts := strings.Split(token, ".")
	if len(parts) != 3 {
		return 0
	}
	payload, err := base64.RawURLEncoding.DecodeString(parts[1])
	if err != nil {
		return 0
	}
	var claims struct {
		Exp int64 `json:"exp"`
	}
	if json.Unmarshal(payload, &claims) != nil || claims.Exp <= 0 {
		return 0
	}
	return claims.Exp
}

// prepareChatBody 预处理请求体：force stream=true（上游只支持流式，实测 stream:false 返回 500），
// 标准化 tool_choice。
func prepareChatBody(rawBody []byte) []byte {
	var body map[string]any
	if err := json.Unmarshal(rawBody, &body); err != nil {
		return rawBody // 解析失败原样发送
	}
	// force stream for upstream SSE compat
	body["stream"] = true
	// normalize tool_choice
	if tc, ok := body["tool_choice"]; ok {
		switch v := tc.(type) {
		case string:
			if v == "" || v == "none" {
				delete(body, "tool_choice")
			}
		case map[string]any:
			// object form, keep as-is
		case nil:
			delete(body, "tool_choice")
		}
	}
	out, err := json.Marshal(body)
	if err != nil {
		return rawBody
	}
	return out
}

// ChatStream 发 chat 请求并返回原始 SSE body 流（调用方负责 Close）。
// 非 2xx 时 rc 为 nil、status 为上游状态码、err 为 nil（body 在 c.LastBody，
// 调用方用 Classify(status, body) 判定）；只有传输层失败才返回 err。
// ⚠️ 龙虾会把部分业务错误（如额度不足 code=40201）藏在 HTTP 200 的 SSE 流里
//（event:error 帧），且帧在流首 → 对 200 响应先窥探首块，识别出错误帧就按
// 非 2xx 同样路径处理（rc=nil + LastBody，由调用方 Classify），否则错误会
// 假装成空 content 的正常响应穿透到客户端。
func (c *Client) ChatStream(a *auth.Auth, body []byte) (rc io.ReadCloser, status int, err error) {
	url := ServerBase() + "/api/proxy/v1/chat/completions"
	req, err := http.NewRequest(http.MethodPost, url, bytes.NewReader(prepareChatBody(body)))
	if err != nil {
		return nil, 0, err
	}
	chatHeaders(req, a)
	resp, err := c.HTTP.Do(req)
	if err != nil {
		log.Printf("chat_stream uid=%s: transport error: %v", a.UID, err)
		return nil, 0, err
	}
	if resp.StatusCode >= 400 {
		raw, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
		resp.Body.Close()
		c.LastBody = raw
		kind := Classify(resp.StatusCode, string(raw))
		log.Printf("chat_stream uid=%s: upstream %d %s body=%s",
			a.UID, resp.StatusCode, kind, truncate(string(raw), 200))
		return nil, resp.StatusCode, nil
	}
	if peeked, sseErr := peekSSEError(resp.Body); sseErr != nil {
		resp.Body.Close()
		c.LastBody = peeked
		kind := Classify(resp.StatusCode, string(peeked))
		log.Printf("chat_stream uid=%s: upstream 200-with-error %s body=%s",
			a.UID, kind, truncate(string(peeked), 200))
		return nil, resp.StatusCode, nil
	} else if peeked != nil {
		// 首块已缓冲在 br 里，Peek 不消费 → 继续从 br 读即可无损透传。
		return newPeekCloser(resp.Body), resp.StatusCode, nil
	}
	return resp.Body, resp.StatusCode, nil
}

// peekBufSize 首块窥探窗口：错误帧总在流首（实测 <200 字节），正常响应
// 首 4KB 内必有数据；不足 4KB 且流已结束（EOF）也照样能判。
const peekBufSize = 4 << 10

// peekSSEError 对 200 的 SSE 流做首块窥探。返回值：
//   - 命中错误帧 → (首块内容, err)；调用方应整条按错误处理。
//   - 正常流     → (nil, nil)；首块留在内部 bufio 里，用 newPeekCloser 接续读。
func peekSSEError(body io.Reader) ([]byte, error) {
	br := bufio.NewReaderSize(body, peekBufSize)
	head, err := br.Peek(peekBufSize)
	if err != nil && err != io.EOF {
		// 连接中途断开：按正常流放行，让下游读到半截时自行报传输错误。
		return nil, nil
	}
	if len(head) == 0 {
		return nil, nil
	}
	if isSSEErrorFrame(head) {
		return head, fmt.Errorf("sse error frame in 200 stream")
	}
	return nil, nil
}

// isSSEErrorFrame 判定首块是否为龙虾 200 流内的错误帧：
// `event:error` 行，或 data JSON 里带 error 对象 / 业务错误码。
func isSSEErrorFrame(head []byte) bool {
	s := string(head)
	if strings.Contains(s, "event:error") || strings.Contains(s, "event: error") {
		return true
	}
	if strings.Contains(s, "\"error\":{") || strings.Contains(s, "\"error\": {") {
		return true
	}
	return false
}

// peekCloser：把窥探过的首块与原始 body 无损拼回一条读流。
type peekCloser struct {
	r io.Reader
	c io.Closer
}

func newPeekCloser(body io.ReadCloser) io.ReadCloser {
	br := bufio.NewReaderSize(body, peekBufSize)
	head, _ := br.Peek(peekBufSize) // peekSSEError 判过正常，这里必然有首块
	return &peekCloser{
		r: io.MultiReader(bytes.NewReader(head), br),
		c: body,
	}
}

func (p *peekCloser) Read(b []byte) (int, error) { return p.r.Read(b) }
func (p *peekCloser) Close() error               { return p.c.Close() }

// FetchModels 调上游动态模型接口。
// GET {server}/api/models/available，Bearer accessToken。
// 返回模型 ID 列表；失败返回错误（调用方回退静态表）。
func (c *Client) FetchModels(a *auth.Auth) ([]string, error) {
	url := ServerBase() + "/api/models/available"
	body := a.KeyfromBody()
	// build query string from keyfrom
	parts := make([]string, 0)
	for k, v := range body {
		parts = append(parts, fmt.Sprintf("%s=%s", k, fmt.Sprintf("%v", v)))
	}
	if len(parts) > 0 {
		url += "?" + strings.Join(parts, "&")
	}
	req, err := http.NewRequest(http.MethodGet, url, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Authorization", "Bearer "+a.AccessToken)
	req.Header.Set("Accept", "application/json")
	req.Header.Set("User-Agent", clientUA)
	resp, err := c.HTTP.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("models api status %d: %s", resp.StatusCode, truncate(string(raw), 120))
	}
	var env struct {
		Code int `json:"code"`
		Data []struct {
			ModelID   string `json:"modelId"`
			ModelName string `json:"modelName"`
			Provider  string `json:"provider"`
			ApiFormat string `json:"apiFormat"`
		} `json:"data"`
	}
	if err := json.Unmarshal(raw, &env); err != nil {
		return nil, fmt.Errorf("models parse: %w", err)
	}
	if env.Code != 0 {
		return nil, fmt.Errorf("models api code=%d", env.Code)
	}
	ids := make([]string, 0, len(env.Data))
	for _, m := range env.Data {
		if m.ModelID != "" {
			ids = append(ids, m.ModelID)
		}
	}
	if len(ids) == 0 {
		return nil, fmt.Errorf("models api returned empty list")
	}
	return ids, nil
}

// QuotaUsage 查询账号当前积分。
// GET {server}/api/user/profile-summary 的 totalCreditsRemaining（含 free + campaign 活动积分）。
// 注意: /api/user/quota 只显示 freeCreditsTotal=300, 不含 5000 活动积分。
func (c *Client) QuotaUsage(a *auth.Auth) (remain int64, total int64, err error) {
	url := ServerBase() + "/api/user/profile-summary"
	req, err := http.NewRequest(http.MethodGet, url, nil)
	if err != nil {
		return 0, 0, err
	}
	req.Header.Set("Authorization", "Bearer "+a.AccessToken)
	req.Header.Set("Accept", "application/json")
	req.Header.Set("User-Agent", clientUA)
	data, err := c.doJSON(req)
	if err != nil {
		return 0, 0, err
	}
	var ps struct {
		TotalCreditsRemaining float64 `json:"totalCreditsRemaining"`
	}
	if err := json.Unmarshal(data, &ps); err != nil {
		return 0, 0, fmt.Errorf("profile-summary parse: %w", err)
	}
	clamp := func(v float64) int64 {
		if v < 0 {
			return 0
		}
		return int64(v)
	}
	if ps.TotalCreditsRemaining > 0 {
		return clamp(ps.TotalCreditsRemaining), 0, nil
	}
	return 0, 0, fmt.Errorf("profile-summary: no credits")
}

// DailyCheckin 执行每日签到。目前龙虾签到端点未知，返回 nil（no-op）。
// 后续抓包确定端点后再实现。
func (c *Client) DailyCheckin(a *auth.Auth) error {
	// TODO: LobsterAI daily sign-in endpoint TBD
	// placeholder: return nil means "checkin skipped silently"
	return nil
}
