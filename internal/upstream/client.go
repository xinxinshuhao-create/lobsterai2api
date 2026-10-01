// Package upstream 封装对 LobsterAI 上游的全部 HTTP 调用。
package upstream

import (
	"bytes"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"os"
	"strings"
	"sync"
	"time"

	"lobsterai2api/internal/auth"
)

// clientVersion 只用于 User-Agent 字符串（与上游更新接口拉取的版本无关）。
// 注意: client-activities/slot 会按 clientVersion 判断活动可见性 —— 旧版本号
// （如 0.1.0）会被服务端返回 slotState=empty，签到前必须用 fetchClientVersion 取真实版本。
const (
	clientVersion = "0.1.0"
	clientUA      = "LobsterAI/0.1.0"

	// updateAPIURL 官方客户端更新接口，用于取当前线上版本号。
	updateAPIURL = "https://api-overmind.youdao.com/openapi/get/luna/hardware/lobsterai/prod/update"
	// fallbackClientVersion 更新接口不可用时的兜底版本号。
	fallbackClientVersion = "2026.9.4"
	// checkinPlacement 签到活动所在位置槽。
	checkinPlacement = "desktop_sidebar"
)

// clientVersionCache 缓存从官方更新接口拉到的版本号（1h TTL，失败 10m 后重试）。
var clientVersionCache struct {
	sync.Mutex
	val string
	at  time.Time
}

// fetchClientVersion 取官方当前客户端版本号；失败返回 fallback。
// 签到活动接口按版本号下发活动，旧版本会拿到 slotState=empty。
func (c *Client) fetchClientVersion() string {
	clientVersionCache.Lock()
	defer clientVersionCache.Unlock()
	if clientVersionCache.val != "" && time.Since(clientVersionCache.at) < time.Hour {
		return clientVersionCache.val
	}
	v := fallbackClientVersion
	if req, err := http.NewRequest(http.MethodGet, updateAPIURL, nil); err == nil {
		req.Header.Set("Accept", "application/json")
		if resp, err := c.HTTP.Do(req); err == nil {
			raw, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
			resp.Body.Close()
			var env struct {
				Data struct {
					Value struct {
						Version string `json:"version"`
					} `json:"value"`
				} `json:"data"`
			}
			if json.Unmarshal(raw, &env) == nil && env.Data.Value.Version != "" {
				v = env.Data.Value.Version
			}
		}
	}
	clientVersionCache.val = v
	clientVersionCache.at = time.Now()
	return v
}

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
	return resp.Body, resp.StatusCode, nil
}

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

// newUUID 生成随机 UUID4（幂等键用）。
func newUUID() string {
	b := make([]byte, 16)
	if _, err := rand.Read(b); err != nil {
		return fmt.Sprintf("%d", time.Now().UnixNano())
	}
	b[6] = (b[6] & 0x0f) | 0x40
	b[8] = (b[8] & 0x3f) | 0x80
	return fmt.Sprintf("%x-%x-%x-%x-%x", b[0:4], b[4:6], b[6:8], b[8:10], b[10:16])
}

// checkinHeaders 设置 client-activities 请求头（Bearer + 真实客户端版本）。
func checkinHeaders(req *http.Request, a *auth.Auth, version string) {
	req.Header.Set("Authorization", "Bearer "+a.AccessToken)
	req.Header.Set("Accept", "application/json")
	req.Header.Set("User-Agent", "LobsterAI/"+version)
}

// activitySlot 是 slot 接口返回的活动描述。
type activitySlot struct {
	ActivityCode   string `json:"activityCode"`
	ConfigRevision int    `json:"configRevision"`
}

// activityContext 是 context 接口返回的活动状态。
type activityContext struct {
	State struct {
		ClaimedToday bool `json:"claimedToday"`
	} `json:"state"`
	Actions []string `json:"actions"`
}

// DailyCheckin 执行每日签到（+100 积分/号/天）。
// 协议: GET slot → GET context → POST actions/check_in → 复核 claimedToday。
// 幂等: 已签到 / 活动未投放 都返回 nil（不算失败），不会重复领取。
func (c *Client) DailyCheckin(a *auth.Auth) error {
	base := ServerBase()
	if base == "" {
		return fmt.Errorf("checkin: LB2A_UPSTREAM_BASE not set")
	}
	version := c.fetchClientVersion()

	// 1. 查活动槽：服务端按 clientVersion 决定活动可见性。
	slotURL := fmt.Sprintf("%s/api/client-activities/slot?placement=%s&clientVersion=%s&containerApiVersion=2&platform=win32",
		base, checkinPlacement, url.QueryEscape(version))
	req, err := http.NewRequest(http.MethodGet, slotURL, nil)
	if err != nil {
		return err
	}
	checkinHeaders(req, a, version)
	raw, err := c.doJSON(req)
	if err != nil {
		return err
	}
	var slot struct {
		SlotState string        `json:"slotState"`
		Activity  *activitySlot `json:"activity"`
	}
	if err := json.Unmarshal(raw, &slot); err != nil {
		return fmt.Errorf("checkin slot parse: %w", err)
	}
	if slot.SlotState != "available" || slot.Activity == nil || slot.Activity.ActivityCode == "" {
		log.Printf("checkin uid=%s: no activity (slotState=%q)", a.UID, slot.SlotState)
		return nil
	}
	code, rev := slot.Activity.ActivityCode, slot.Activity.ConfigRevision

	// 2. 查活动状态：已签到直接跳过。
	ctxURL := fmt.Sprintf("%s/api/client-activities/%s/context?configRevision=%d", base, url.PathEscape(code), rev)
	req, err = http.NewRequest(http.MethodGet, ctxURL, nil)
	if err != nil {
		return err
	}
	checkinHeaders(req, a, version)
	raw, err = c.doJSON(req)
	if err != nil {
		return err
	}
	var ctx activityContext
	if err := json.Unmarshal(raw, &ctx); err != nil {
		return fmt.Errorf("checkin context parse: %w", err)
	}
	if ctx.State.ClaimedToday || !containsStr(ctx.Actions, "check_in") {
		log.Printf("checkin uid=%s: already claimed today, skip", a.UID)
		return nil
	}

	// 3. 提交签到（幂等键防止重复发放）。
	body, _ := json.Marshal(map[string]any{
		"configRevision": rev,
		"idempotencyKey": newUUID(),
		"payload":        map[string]any{},
	})
	actionURL := fmt.Sprintf("%s/api/client-activities/%s/actions/check_in", base, url.PathEscape(code))
	req, err = http.NewRequest(http.MethodPost, actionURL, bytes.NewReader(body))
	if err != nil {
		return err
	}
	checkinHeaders(req, a, version)
	req.Header.Set("Content-Type", "application/json")
	raw, err = c.doJSON(req)
	if err != nil {
		return err
	}
	var res struct {
		Result map[string]any `json:"result"`
	}
	_ = json.Unmarshal(raw, &res)
	log.Printf("checkin uid=%s: claimed activity=%s reward=%v", a.UID, code, res.Result["rewardCredits"])

	// 4. 复核：再拉一次 context，只有 claimedToday 真的翻转才算成功。
	req, err = http.NewRequest(http.MethodGet, ctxURL, nil)
	if err != nil {
		return nil
	}
	checkinHeaders(req, a, version)
	raw, err = c.doJSON(req)
	if err != nil {
		return nil
	}
	var after activityContext
	if json.Unmarshal(raw, &after) == nil && !after.State.ClaimedToday {
		return fmt.Errorf("checkin uid=%s: claimedToday still false after check_in", a.UID)
	}
	return nil
}

func containsStr(list []string, want string) bool {
	for _, v := range list {
		if v == want {
			return true
		}
	}
	return false
}
