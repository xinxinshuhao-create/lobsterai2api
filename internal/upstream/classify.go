// Package upstream 封装对 LobsterAI 上游（chat / quota / auth）的全部 HTTP 调用，
// 以及错误分类（驱动 pool 冷却状态机）。
package upstream

import (
	"fmt"
	"net/http"
	"strings"
)

// ErrKind 错误分类，pool 据此决定冷却时长。
type ErrKind int

const (
	ErrNone        ErrKind = iota // 成功
	ErrHardCredit                 // 余额不足（402 或 body 关键词）→ 长冷却
	ErrSoftRate                   // 429 软限流 → 短冷却
	ErrSessionDead                // 401 + 40100/40101 刷新被拒 → 禁用
	ErrNotFound                   // 404 上游偶发 → 短冷却不累计 errCount（防雪崩）
	ErrServer                     // 5xx 上游故障
	ErrClient                     // 其他 4xx / 业务错误
)

func (k ErrKind) String() string {
	switch k {
	case ErrHardCredit:
		return "hard_credit"
	case ErrSoftRate:
		return "soft_rate"
	case ErrSessionDead:
		return "session_dead"
	case ErrNotFound:
		return "not_found"
	case ErrServer:
		return "server"
	case ErrClient:
		return "client"
	default:
		return "none"
	}
}

// Error 带分类的上游错误。
type Error struct {
	Kind   ErrKind
	Status int
	Msg    string
}

func (e *Error) Error() string {
	return fmt.Sprintf("upstream %s (http %d): %s", e.Kind, e.Status, e.Msg)
}

// hardMarkers 余额不足关键词（小写比较 + 中文原文比较双通道）。
var hardMarkers = []string{
	"insufficient credit", "no credit", "credit exhausted", "out of credit",
	"quota exceeded", "quota exhaust", "payment required", "credit not enough",
	"not enough credit", "freecreditsused", "free credits used",
	"积分不足", "额度不足", "余额不足", "积分用完", "额度用尽", "没有积分", "积分耗尽",
	// 龙虾 200 流内错误帧（code 40201）实测文案：「免费额度已用完，请升级套餐」
	"额度已用完", "免费额度", "升级套餐", "\"code\":40201",
}

// sessionDeadMarkers 龙虾刷新被拒的错误码（40100 / 40101 为终止性失败）。
var sessionDeadMarkers = []string{"40100", "40101", "token rejected", "refresh token was rejected"}

// Classify 按 HTTP 状态码 + body 判定错误类别。
func Classify(status int, body string) ErrKind {
	if status == http.StatusPaymentRequired {
		return ErrHardCredit
	}
	lower := strings.ToLower(body)
	for _, m := range hardMarkers {
		if strings.Contains(lower, strings.ToLower(m)) || strings.Contains(body, m) {
			return ErrHardCredit
		}
	}
	for _, m := range sessionDeadMarkers {
		if strings.Contains(body, m) {
			return ErrSessionDead
		}
	}
	if status == http.StatusTooManyRequests {
		return ErrSoftRate
	}
	if status == http.StatusNotFound {
		return ErrNotFound
	}
	if status >= 500 {
		return ErrServer
	}
	if status >= 400 {
		return ErrClient
	}
	return ErrNone
}
