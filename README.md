**English** | [Français](README.fr.md)

> Created by DeepSeek and Z.ai.

# dsh-eyes

Chrome navigation observer + browser **hands** daemon
(webbridge model — REWORK 2026-10-05).

- HTTP+WS endpoint: `http://127.0.0.1:8445` (health, stats, events, alerts, ws)
- v1 routes (open): `GET /health`, `GET /stats`, `GET /events`, `GET /alerts`, `WS /ws`
- v2 routes (Bearer auth): `GET /v2/ping`, `POST /v2/cmd`
- Source: `src/` (Rust, axum 0.7 + tokio + tokio-tungstenite)
- Tests: `tests/` — **124 tests run** (2026-10-21), `cargo test` ; 2 ignored (live Ollama + `ignore` doc-test)
- Chrome extension: `extension/` (MV3, to be loaded via `chrome://extensions` in developer mode)
- Cockpit: `docs/cockpit.html`

## Architecture (REWORK 2026-10-05)

The Chrome extension **already installed** in the user's browser
(id `lmjgnljbpcahodolfncbnchomamnkjoi`) serves both as a **watchman**
(observation of navigations) and as **hands** (execution of the commands
`goto`, `click`, `read`, `fill`, `tabs_create` via `chrome.tabs` and
`chrome.scripting.executeScript`). The daemon no longer spawns a dedicated
Chrome — it talks to the extension over the same WS `ws://127.0.0.1:8445/ws`,
now **bidirectional**.

The CDP spawn + `chromiumoxide` model has been **abandoned** in favour of
this webbridge model. See `~/.superpowers/sdd/2026-10-05-dsh-eyes-v2-hands-solveur/task-2-report.md`
(REWORK section) for the details.

## Routes v2 — `POST /v2/cmd`

```bash
# Example: navigate to a URL
curl -X POST \
  -H "Authorization: Bearer $(cat ~/.dsh-eyes/token)" \
  -H "Content-Type: application/json" \
  -d '{"cmd":"goto","params":{"url":"https://example.com"}}' \
  http://127.0.0.1:8445/v2/cmd
```

Return codes:

- **200** + `{ok:true, result:...}` — the extension executed the command.
- **401** — token missing or invalid.
- **502** + `{ok:false, error:"..."}` — the extension returned `ok:false`
  (selector not found, cross-origin page, etc.).
- **504** + `{ok:false, error:"no_extension_connected"|"extension_timeout_30s"|"extension_disconnected"}`
  — no extension connected, or the extension did not answer within
  30 s.

Supported commands (cf. `extension/background.js`):

- `tabs_create {url, group_title?}` — opens a tab; with `group_title`, navigates the group's existing tab instead of creating one (1 tab/group, window reused) and returns `window_id`/`group_id`.
- `goto {tab_id?, url}` — navigates a tab (existing or new).
- `read {tab_id, max_bytes?, mode?}` — `mode:"text"` (default) returns `document.body.innerText` (200 KB by default); `mode:"outline"` returns the list of visible interactive elements (`tag`, `role`, `text`, `selector`, `x/y/w/h`).
- `find {tab_id?, text?, selector?, limit?}` — **v0.6.1**: lists the visible elements whose text contains `text` (or that match `selector`), with viewport geometry. The link between `read` and `click_native_text`.
- `click {tab_id, selector}` — clicks a CSS element.
- `fill {tab_id, selector, value, direct?}` — fills an input and triggers `input`/`change`. `direct:true` (v0.6.1) uses the native setter in a single injection (<0.3 s); by default it types like a human, character by character.
- `wait {tab_id, ms}` — waits.
- `tab_group {tab_id, title, color?}` — puts the tab into a group.
- `tab_activate {tab_id}` — activates (and focuses) the tab.
- `screenshot {tab_id?, format?}` — captures the viewport (`captureVisibleTab`), dataURL PNG/JPEG.
- `captcha_detect {tab_id?}` — captcha detection + capture.
- `eval {tab_id?, code, arg?, timeout_ms?}` — **executes arbitrary JS in the page (MAIN world)**. `code` = expression (`"document.title"`) or serialized function (`"(a) => a.x + 1"`) receiving `arg` (JSON). Promises are awaited, result sanitized (structure-cloneable). Last resort when CSS selectors cannot reach SPAs (Angular, GA4, Fluent widgets): click by text, read components, inject a visual overlay. E.g. click by text: `"() => { const el=[...document.querySelectorAll('a,button,[role=link]')].find(e=>e.textContent.trim().startsWith('Événements')); if(!el) return 'introuvable'; el.click(); return el.textContent.trim(); }"`
- `click_native {tab_id?, selector}` — **NATIVE click** via `chrome.debugger` + `Input.dispatchMouseEvent` (`isTrusted=true` events): a 3-8 point trajectory from the last known cursor position, pre-click hesitation, spaced press/release. JS only READS the geometry (new-pmu doctrine rule 9: a JS click is identifiable). A visible "debugging" banner during the gesture — accepted.
- `type_native {tab_id?, selector?, text, submit?}` — **native typing** character by character (`Input.dispatchKeyEvent`), dwell 40-160 ms/key. `selector` optional: focus + selection of the existing content (state preparation, not a gesture). `submit: true` adds Enter.
- `scroll_native {tab_id?, dy}` — **native scroll wheel** in 90-135 px steps (30-70 ms apart). Never a JS scroll (signature: 0 wheel events + instant jump).
- `click_native_text {tab_id?, text, exact?}` — native click on an element located **by label** (prefix by default, strict equality if `exact:true`). THE bridge to SPAs without a stable selector (GA4/Angular): JS only searches/reads, the native side clicks.
- `click_native_rect {tab_id?, x, y}` — native click at raw viewport coordinates: the universal building block (eval finds it by any logic, this click executes).
- `screen_pos {tab_id?, selector}` — activates the tab and returns the **screen** geometry of the element (`ecranX/ecranY`) — to be passed to `bin/os-clic.sh`, which moves the **REAL Windows pointer** (SetCursorPos + mouse_event via WSL→PowerShell interop). Exclusive pointer control for ~300 ms; `OS_OFFSET_X/Y` calibration offsets if zoom/DPI is atypical.
- `wait_for {tab_id?, selector|text, timeout_ms?}` — bounded busy wait (250 ms, max 60 s) for a selector OR a text: replaces the arbitrary sleeps of scripts on slow SPAs.

Note v1.0.0 (changes introduced in 0.6.1, now frozen in the major version):
- Commands are **serialized per tab** (`tab_id`): two concurrent calls on the same tab no longer collide on `chrome.debugger.attach`.
- `eval` accepts `timeout_ms` (default 8000, bounded 1000-20000 ms); the gesture guard follows at +5 s (max 24 s).
- `click_native`, `click_native_text` and `click_native_rect` return `verified` (effect observed on URL, scroll, DOM, focus or `aria-*` markers) and `verify_wait_ms`.

### Screen recording (v1.0.0)

Three routes protected by the token:

- `POST /v2/record/start` `{out?, fps?, resolution?, duration?, monitor?, cursor?}` — launches `rec_ecran_cli.py` via the dedicated venv's Windows Python and returns `{out, stop_file}`.
- `POST /v2/record/stop` — creates the sentinel file, waits for the CLI and returns its JSON (`frames`, `duration_s`, `fps`, `resolution`).
- `GET /v2/record/status` — `{recording, out?, elapsed_s?}`.

The CLI lives in `bin/rec_ecran_cli.py` (source) and `%USERPROFILE%\Documents\Mon Enregistreur\rec_ecran_cli.py` (Windows deployment). Dependencies: `mss`, `opencv-python`, `imageio`, `imageio-ffmpeg`, `numpy` in `venv-rec`.

### Screen capture (v1.0.0)

`bin/capture-ecran.sh` captures the physical Windows screen with the cursor drawn:

- `--region x,y,w,h` — a specific area (e.g. `0,0,800,600`).
- `--fenetre [title]` — a window by partial title (e.g. `"Chrome"`); without a title, the active window.
- `--out <path>` — Windows output file (default `%USERPROFILE%\Downloads\ecran-dsh.png`).

Output: `OK WIDTHxHEIGHT cursor mode@(x,y)` then the WSL path. The crop reduces the loss of detail caused by the model's image downscaling.

### Native gesture usage doctrine (v0.4)

Derived from new-pmu (rules 9-10 of 2026-10-12, measured in live A/B):
`el.click()` and `dispatchEvent` produce `isTrusted=false` events
— identifiable by the platforms that watch for them (eBay finding
2026-10-20: anti-bot triggered by JS clicks). The `_native` commands emit
real input events. **Use restricted to OUR consoles and OUR accounts**
(GA4, GSC, Bing, the operator's Chrome) — never to circumvent terms of service or on
accounts in dispute (suspended eBay = by hand).

## Static build (musl)

```bash
rustup target add x86_64-unknown-linux-musl
cargo build --release --target x86_64-unknown-linux-musl
file target/x86_64-unknown-linux-musl/release/dsh-eyes  # ELF … static-pie linked
```

Produced binary: single `statically linked` binary, ~2.7 MB, no dynamic dependency.

## Installation

```bash
mkdir -p ~/.dsh-eyes/bin
cp target/x86_64-unknown-linux-musl/release/dsh-eyes ~/.dsh-eyes/bin/
cp -r extension ~/.dsh-eyes/extension
```

Launch: `~/.dsh-eyes/bin/dsh-eyes` (listens on `127.0.0.1:8445`).

Runtime data written to `~/.dsh-eyes/`:
- `events.jsonl` — log of events received via WS
- `alerts.jsonl` — alerts (localhost_access, private_ip, new_domain, etc.)
- `allowlist.json` — allowed domains (persistence of decisions)
- `token` — Bearer token for /v2 (chmod 600, generated on the 1st run)
- `config.toml` — non-sensitive options

## Captcha solver — two-tier cascade

The solver (`POST /v2/page/solve`) queries the Ollama endpoints in the
order `ollama_tier1_url` → `ollama_tier2_url`, then hands control back
(`human_required`) if both fail. Each tier may require a token:

| `config.toml` key | Meaning |
|---|---|
| `ollama_tier1_url` / `ollama_tier2_url` | Ollama base; `/api/generate` is called |
| `ollama_tier1_token_file` / `ollama_tier2_token_file` | file containing the token sent in `Authorization: Bearer` (absolute path, or resolved under `$DSH_EYES_HOME`). Empty or missing file → tier called without authentication |
| `vision_model` | vision model (default `qwen2.5-vl`) |

The secret never lives in `config.toml`: the token file must be
`chmod 600`. This mechanism makes the IAQ router usable — it answers
`401` to `/api/generate` without a token.

Automated proof (local network required, test ignored by default):

```bash
cargo test --test solver -- --ignored live_iaq_token_accepted
```

## Reloading the Chrome extension after an update

1. Go to `chrome://extensions/`.
2. Developer mode enabled (top right).
3. Find "DSH Eyes" in the list.
4. Click the **⟳ Reload** button (circular icon) under the card.
5. The new version of `background.js` is taken into account — the
   WS connection reopens automatically, the `ExtensionChannel`
   rediscovers it on the next message.

## WS test without a browser (automated proof)

`extension/test-ws-client.js` simulates the extension — it connects to
`/ws`, answers the commands received, and checks the round trip:

```bash
# Start the daemon first
DSH_EYES_HOME=/tmp/dsh-eyes-test ./target/release/dsh-eyes &
DAEMON_PID=$!

# "command-loop" mode: wait for 3 commands from the daemon and answer them
node extension/test-ws-client.js --count 3 &

# In a 3rd terminal, trigger the 3 commands
TOKEN=$(cat /tmp/dsh-eyes-test/token)
for i in 1 2 3; do
  curl -X POST -H "Authorization: Bearer $TOKEN" \
    -H "Content-Type: application/json" \
    -d "{\"cmd\":\"goto\",\"params\":{\"url\":\"https://example.com?n=$i\"}}" \
    http://127.0.0.1:8445/v2/cmd
  echo
done

kill $DAEMON_PID
```

## Overview (inventory)

See `~/.dsh/docs/ecosysteme-local.md` ("DSH Eyes" section).

## Known limitations (v1.0.0)

- **`record` tests**: the 124 tests cover what exists; there is no dedicated test suite for the `/v2/record/*` routes nor for the Python CLI.
- **Audio**: `rec_ecran_cli.py` captures video only; the microphone and the original application's system audio are not integrated.
- **Daemon security**: `/v2/cmd` does not re-apply `is_forbidden_host` (local SSRF bypass possible); `/ws` does not require the token; no rate limit; no version handshake between the extension and the daemon.
- **Ignored doc-test**: `src/vigil.rs:283` is an ` ```ignore ` example, not executed — it does not count as a test.

## License

MIT — see [LICENSE](LICENSE).
