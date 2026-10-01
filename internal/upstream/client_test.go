package upstream

import (
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"lobsterai2api/internal/auth"
)

// 正常流必须字节级无损透传。历史回归：窥探首块时另起了一个 bufio.Reader，
// 把已读进旧缓冲的流首 4KB 吞掉，下游拿到截断首帧（空 tool name / 截断参数）。
func TestChatStreamPassthroughByteExact(t *testing.T) {
	var sb strings.Builder
	for i := 0; i < 400; i++ {
		sb.WriteString(`data:{"id":"x","choices":[{"delta":{"content":"`)
		sb.WriteString(strings.Repeat("y", 40))
		sb.WriteString(`"}}]}` + "\n\n")
	}
	sb.WriteString("data:[DONE]\n\n")
	want := sb.String() // ~50KB，远超 4KB 窥探窗口

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		io.WriteString(w, want)
	}))
	defer srv.Close()

	t.Setenv("LB2A_UPSTREAM_BASE", srv.URL)
	c := New()
	rc, status, err := c.ChatStream(&auth.Auth{AccessToken: "tok", UID: "1"}, []byte(`{"model":"m"}`))
	if err != nil || rc == nil || status != 200 {
		t.Fatalf("ChatStream: rc=%v status=%d err=%v", rc, status, err)
	}
	defer rc.Close()
	got, err := io.ReadAll(rc)
	if err != nil {
		t.Fatalf("read stream: %v", err)
	}
	if string(got) != want {
		t.Fatalf("流首被破坏: got %d bytes, want %d bytes\n got[0:80]=%q\nwant[0:80]=%q",
			len(got), len(want), string(got[:min(80, len(got))]), want[:80])
	}
}

// 200 流内的错误帧仍须被识别为上游错误（窥探逻辑的原始目的）。
func TestChatStreamErrorFrameIn200(t *testing.T) {
	frame := "event:error\ndata:{\"type\":\"error\",\"error\":{\"type\":\"proxy_error\",\"message\":\"免费额度已用完，请升级套餐\",\"code\":40201}}\n\n"

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		io.WriteString(w, frame)
	}))
	defer srv.Close()

	t.Setenv("LB2A_UPSTREAM_BASE", srv.URL)
	c := New()
	rc, status, err := c.ChatStream(&auth.Auth{AccessToken: "tok", UID: "1"}, []byte(`{"model":"m"}`))
	if err != nil {
		t.Fatalf("ChatStream transport: %v", err)
	}
	if rc != nil || status != 200 {
		t.Fatalf("错误帧未被拦下: rc=%v status=%d", rc, status)
	}
	if k := Classify(status, string(c.LastBody)); k != ErrHardCredit {
		t.Fatalf("分类错误: got %s want %s", k, ErrHardCredit)
	}
}
