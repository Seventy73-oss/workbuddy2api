#!/usr/bin/env python3
"""对比：同批模型在国区(copilot.tencent.com)账号上的可用性。自动定位国区账号。"""
import json
import os
import pathlib
import sys

AUTH_DIR = pathlib.Path(os.environ.get("CODEBUDDY_AUTH_DIR", "/data/auth"))
sys.path.insert(0, os.environ.get("CONVERTER_APP_DIR", "/app"))
import httpx
from converter import CredentialManager

MODELS = json.loads((AUTH_DIR / "models.json").read_text(encoding="utf-8"))["models"]

# 自动找国区账号（含被禁用改名成 .disabled 的，仅读取不改变状态）
cands = sorted(AUTH_DIR.glob("*.info")) + \
        sorted(AUTH_DIR.glob("*.info.disabled"))
target = None
for p in cands:
    cm = CredentialManager(p)
    if not cm.is_intl:
        target = cm
        break
if target is None:
    print("没找到国区账号")
    sys.exit(1)

print(f"国区账号 {target.path.name} | backend={target.backend} | {len(MODELS)} 个模型\n")
headers = target.get_headers()
for m in MODELS:
    body = {"model": m,
            "messages": [{"role": "user", "content": "hi"}],
            "stream": True, "max_tokens": 4}
    try:
        with httpx.Client(timeout=60) as c:
            with c.stream("POST", target.backend + "/v2/chat/completions",
                          headers=headers, json=body) as r:
                raw = b""
                for chunk in r.iter_bytes():
                    raw += chunk
                    if len(raw) > 300:
                        break
                txt = raw.decode("utf-8", "replace")
                if r.status_code == 200 and '"error"' not in txt:
                    print(f"  ✅ {m}")
                else:
                    print(f"  ❌ {m:22s} HTTP {r.status_code} {txt[:150]!r}")
    except Exception as e:
        print(f"  ❌ {m:22s} EXC {type(e).__name__}: {str(e)[:110]}")
