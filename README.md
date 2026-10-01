# lobsterai2api

OpenAI-compatible API bridge for LobsterAI — multi-account pool with credit-based load balancing, SSE streaming, and automatic token refresh.

- Language: Go (zero external dependencies, pure stdlib)
- Port: `:8367` (configurable via config or `LB2A_LISTEN`)

## Architecture

```
client (OpenAI SDK)
   │ POST /v1/chat/completions (Bearer ***)
   ▼
server: pool picks account (highest credits, healthy) → check token → forward
   ▼
upstream chat API (SSE only)
   ▼ on error → classify → cooldown/disable → rotate to next account (max 3)
```

## Build

```bash
go build -o lobsterai2api.exe ./cmd/server
go build -o login.exe ./cmd/login
go build -o credit.exe ./cmd/credit
```

## Login (add account)

```bash
./login.sh
# or manually:
./login.exe url   # prints login URL (local callback server ready)
# open URL in browser → phone/WeChat login
./login.exe poll  # wait for callback → exchange → save auths/lobsterai-<uid>.json
```

## Run

```bash
./lobsterai2api.exe -config config.json
```

## Credit query

```bash
./credit.sh        # human-readable
./credit.exe       # JSON output (for scripts)
```

## Test

```bash
# non-streaming
curl -s http://127.0.0.1:8367/v1/chat/completions \
  -H "Authorization: Bearer ***" \
  -H "Content-Type: application/json" \
  -d '{"model":"deepseek-v4-pro","messages":[{"role":"user","content":"你好"}],"stream":false}'

# streaming
curl -s http://127.0.0.1:8367/v1/chat/completions \
  -H "Authorization: Bearer ***" \
  -H "Content-Type: application/json" \
  -d '{"model":"deepseek-v4-pro","messages":[{"role":"user","content":"你好"}],"stream":true}'

# model list
curl -s http://127.0.0.1:8367/v1/models -H "Authorization: Bearer ***"

# status
curl -s http://127.0.0.1:8367/status
```

## Configuration

See `config.example.json`. Environment variable prefix `LB2A_*`:

| Variable | Description |
|---|---|
| `LB2A_LISTEN` | Listen address |
| `LB2A_API_KEY` | Local auth key |
| `LB2A_AUTH_DIR` | Auth file directory |
| `LB2A_STATE_FILE` | Pool state file |
| `LB2A_HARD_CREDIT` / `LB2A_SOFT_RATE` | Cooldown durations |
| `LB2A_ERR_THRESHOLD` / `LB2A_ERR_COOLDOWN` | Error threshold and cooldown |
| `LB2A_TIMEOUT_SECONDS` | Upstream timeout |
| `LB2A_UPSTREAM_BASE` | Upstream API base URL (required) |
| `LB2A_LOGIN_PORTAL` | Login portal URL for OAuth flow (required for login) |

## Features

- **Multi-account pool** — auto-load auth files from `auths/`, pick highest-credit healthy account per request
- **OpenAI-compatible** — `/v1/chat/completions` (streaming + non-streaming), `/v1/models`, `/status`, `/healthz`
- **OAuth login** — local callback server, browser-based login, auto-save credentials
- **Token refresh** — JWT expiry parsing, proactive refresh 10min before expiry, session death auto-disable
- **Error classification** — hard credit cooldown 12h, 429 soft cooldown 60s, consecutive errors 3→10m, refresh rejected → disable
- **Request-level rotation** — up to 3 account switches per request
- **Scheduler** — daily check-in (+100 credits/account/day, idempotent) + credit refresh, token keepalive
- **Dynamic model list** — fetched from upstream API, cached 1h, falls back to static table

## Known limitations / TODO

- **Streaming errors cannot change status code** — once an SSE response has started, a mid-stream upstream error is passed through as an `event:error` frame rather than an HTTP 4xx. Non-streaming requests return 400 with the upstream reason.
- **Upstream version probe** — the check-in flow reads the current client version from the official update API; if that probe fails it falls back to a built-in version constant, which the slot endpoint may treat as stale.

## License

MIT
