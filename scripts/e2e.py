#!/usr/bin/env python3
"""E2E DSH Eyes — vérifie le démon et le Chrome d'Eddie de bout en bout.

Usage :
  scripts/e2e.py                 # contrôles standards
  scripts/e2e.py --multiframe    # ajoute le test shadow DOM + iframe same-origin

Le script lit le token dans ~/.dsh-eyes/token (ou DSH_TOKEN_FILE), ne
l'affiche jamais, et sort en code 1 si un contrôle échoue.
"""

from __future__ import annotations

import argparse
import json
import os
import threading
import time
import urllib.error
import urllib.request

BASE = os.environ.get("DSH_URL", "http://127.0.0.1:8445")
TOKEN_FILE = os.path.expanduser(os.environ.get("DSH_TOKEN_FILE", "~/.dsh-eyes/token"))
TOKEN = open(TOKEN_FILE, encoding="utf-8").read().strip()

failures: list[str] = []


def call(method: str, path: str, payload=None, timeout: int = 60):
    data = json.dumps(payload).encode() if payload is not None else None
    req = urllib.request.Request(
        BASE + path,
        data=data,
        method=method,
        headers={"Authorization": "Bearer " + TOKEN, "Content-Type": "application/json"},
    )
    try:
        with urllib.request.urlopen(req, timeout=timeout) as r:
            return r.status, json.loads(r.read())
    except urllib.error.HTTPError as e:
        body = e.read()
        try:
            return e.code, json.loads(body)
        except Exception:
            return e.code, {"raw": body.decode(errors="replace")}


def cmd(name: str, params: dict, timeout: int = 60):
    return call("POST", "/v2/cmd", {"cmd": name, "params": params}, timeout)


def check(name: str, cond: bool, detail: str = "") -> bool:
    print(("PASS " if cond else "FAIL ") + name + ((" — " + detail) if detail else ""))
    if not cond:
        failures.append(name)
    return cond


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--multiframe", action="store_true", help="teste shadow DOM + iframe same-origin")
    args = ap.parse_args()

    st, health = call("GET", "/health", timeout=10)
    check("health 1.1.0", st == 200 and health.get("version") == "1.1.0", str(health))

    st, info = call("GET", "/v2/info", timeout=10)
    ext = info.get("extension", {})
    check("extension connectée", st == 200 and ext.get("connected") is True)
    caps = (ext.get("hello") or {}).get("capabilities") or []
    check("handshake version", (ext.get("hello") or {}).get("version") == "1.1.0", str(caps))

    st, r = cmd("tabs_create", {"url": "http://127.0.0.1/"})
    check("SSRF localhost 403", st == 403 and r.get("error") == "forbidden_target", str(r))
    st, r = cmd("tabs_create", {"url": "http://127.0.0.1\\evil.com/"})
    check("SSRF backslash 400", st == 400 and r.get("error") == "invalid_url", str(r))

    st, r = cmd("tabs_create", {"url": "https://www.example.com/"})
    tab = (r.get("result") or {}).get("tab_id")
    check("tabs_create", st == 200 and isinstance(tab, int), str(r.get("error", "")))
    if not isinstance(tab, int):
        print("Arrêt : pas de tab_id")
        return 1

    st, r = cmd("read", {"tab_id": tab, "mode": "outline"})
    check("read outline", st == 200 and isinstance(r.get("result", {}).get("elements"), list), str(r.get("error", "")))

    st, r = cmd("screenshot", {"tab_id": tab, "format": "png"})
    data_url = (r.get("result") or {}).get("data_url", "")
    check("screenshot", st == 200 and len(data_url) > 1000, f"{len(data_url)} chars")

    inject = "() => { const i=document.createElement('input'); i.id='dsh-e2e'; document.body.appendChild(i); return 'ok'; }"
    st, r = cmd("eval", {"tab_id": tab, "code": inject})
    check("eval inject input", st == 200 and r.get("result", {}).get("value") == "ok", str(r))

    st, r = cmd("fill", {"tab_id": tab, "selector": "#dsh-e2e", "value": "secret-e2e"})
    check("fill", st == 200, str(r.get("error", "")))

    st, r = cmd("type_native", {"tab_id": tab, "selector": "#dsh-e2e", "text": "hello"})
    check("type_native", st == 200, str(r.get("error", "")))
    st, r = cmd("eval", {"tab_id": tab, "code": "() => document.querySelector('#dsh-e2e').value"})
    check("type_native valeur", st == 200 and r.get("result", {}).get("value") == "hello", str(r))

    action_id = f"e2e-act-{int(time.time())}"
    act = {
        "action_id": action_id,
        "steps": [
            {"cmd": "read", "params": {"tab_id": tab, "mode": "outline"}},
            {"cmd": "eval", "params": {"tab_id": tab, "code": "() => 1 + 1"}},
        ],
    }
    st, r = call("POST", "/v2/act", act)
    check("act batch", st == 200 and len(r.get("results", [])) == 2 and r.get("cached") is False, str(r.get("error", "")))
    st, r = call("POST", "/v2/act", {"action_id": action_id, "steps": [{"cmd": "read", "params": {"tab_id": tab}}]})
    check("act cache idempotent", st == 200 and r.get("cached") is True, str(r.get("error", "")))

    cancel_id = f"e2e-cancel-{int(time.time())}"
    cancel_act = {
        "action_id": cancel_id,
        "steps": [
            {"cmd": "eval", "params": {"tab_id": tab, "code": "async () => { await new Promise(r => setTimeout(r, 1500)); return 'ok'; }"}},
            {"cmd": "read", "params": {"tab_id": tab, "mode": "outline"}},
        ],
    }
    result: dict = {}

    def run_cancel_act():
        result["act"] = call("POST", "/v2/act", cancel_act, timeout=20)

    t = threading.Thread(target=run_cancel_act)
    t.start()
    time.sleep(0.4)
    call("POST", "/v2/act/cancel", {"action_id": cancel_id})
    t.join(timeout=20)
    st, r = result.get("act", (0, {}))
    check("act annulation 409", st == 409 and r.get("error") == "cancelled" and r.get("step") == 1, str(r))

    st, metrics = call("GET", "/v2/metrics", timeout=10)
    c = metrics.get("commands", {})
    check("métriques", st == 200 and c.get("total", 0) > 0 and c.get("ok", 0) > 0, str(c))

    if args.multiframe or "multiframe" in caps:
        if "multiframe" not in caps:
            print("SKIP multiframe — capacité absente (extension pas rechargée)")
        else:
            inject = """() => {
              const host = document.createElement('div');
              host.id = 'shadow-host';
              host.style.cssText = 'position:fixed;top:8px;left:8px;z-index:2147483647;background:#fff;padding:4px';
              const root = host.attachShadow({mode:'open'});
              root.innerHTML = '<button id="shadow-btn" onclick="window.__shadowClicked=true">Shadow Button</button>';
              document.body.appendChild(host);
              const ifr = document.createElement('iframe');
              ifr.id = 'frame-test';
              ifr.style.cssText = 'position:fixed;top:60px;left:8px;width:320px;height:120px;z-index:2147483647;background:#fff';
              ifr.srcdoc = '<button id="frame-btn" onclick="window.top.__frameClicked=true">Frame Button</button>';
              document.body.appendChild(ifr);
              return 'ok';
            }"""
            st, r = cmd("eval", {"tab_id": tab, "code": inject})
            time.sleep(0.8)
            check("injection shadow+iframe", st == 200, str(r.get("error", "")))

            st, r = cmd("read", {"tab_id": tab, "mode": "outline"})
            els = (r.get("result") or {}).get("elements", [])
            texts = " | ".join(e.get("text", "") for e in els)
            frame_ids = {e.get("frame_id") for e in els}
            check("read outline multi-frame", "Shadow Button" in texts and "Frame Button" in texts, texts[:160])
            check("read frame_id", len(frame_ids) >= 2, str(frame_ids))

            st, r = cmd("find", {"tab_id": tab, "text": "Shadow"})
            check("find shadow", st == 200 and (r.get("result") or {}).get("count", 0) > 0, str(r.get("error", "")))

            st, r = cmd("click_native", {"tab_id": tab, "selector": "#shadow-btn", "focus": True})
            check("click_native shadow", st == 200, str(r.get("error", "")))
            st, r = cmd("eval", {"tab_id": tab, "code": "() => window.__shadowClicked === true"})
            check("marqueur shadow", st == 200 and r.get("result", {}).get("value") is True, str(r))

            st, r = cmd("click_native", {"tab_id": tab, "selector": "#frame-btn", "focus": True})
            check("click_native iframe same-origin", st == 200, str(r.get("error", "")))
            st, r = cmd("eval", {"tab_id": tab, "code": "() => window.__frameClicked === true"})
            check("marqueur iframe", st == 200 and r.get("result", {}).get("value") is True, str(r))

    st, r = cmd("click_native_text", {"tab_id": tab, "text": "Learn more", "focus": True, "human_profile": "presse"})
    check("click_native_text verified", st == 200 and (r.get("result") or {}).get("verified") is True, str(r.get("error", "")))

    print()
    if failures:
        print(f"ÉCHECS ({len(failures)}) : " + ", ".join(failures))
        return 1
    print("E2E OK")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
