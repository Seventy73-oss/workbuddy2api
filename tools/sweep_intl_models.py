#!/usr/bin/env python3
"""扫描国际版(codebuddy.ai)账号对 models.json 里每个模型的可用性。"""
import json
import os
import pathlib
import sys

AUTH_DIR = pathlib.Path(os.environ.get("CODEBUDDY_AUTH_DIR", "/data/auth"))
sys.path.insert(0, os.environ.get("CONVERTER_APP_DIR", "/app"))
import httpx
from converter import CredentialManager

# 用法：python3 sweep_intl_models.py [账号文件名]
TARGET = sys.argv[1] if len(sys.argv) > 1 else next(
    (p.name for p in sorted(AUTH_DIR.glob("*.info"))), "")
MODELS = json.loads((AUTH_DIR / "models.json").read_text(encoding="utf-8"))["models"]

cm = CredentialManager(AUTH_DIR / TARGET)
headers = cm.get_headers()
kw = {"timeout": 90}
if cm.proxy:
    kw["proxy"] = cm.proxy
print(f"账号 {TARGET} | backend={cm.backend} | proxy={cm.proxy or '直连'} | 共 {len(MODELS)} 个模型\n")

for m in MODELS:
    body = {"model": m,
            "messages": [{"role": "system", "content": "You are a helpful assistant."},
                         {"role": "user", "content": "hi"}],
            "stream": True, "max_tokens": 8, "stream_options": {"include_usage": True}}
    try:
        with httpx.Client(**kw) as c:
            with c.stream("POST", cm.backend + "/v2/chat/completions",
                          headers=headers, json=body) as r:
                got, err, code = None, None, r.status_code
                for line in r.iter_lines():
                    if not line or not line.startswith("data:"):
                        continue
                    d = line[5:].strip()
                    if d == "[DONE]":
                        break
                    try:
                        j = json.loads(d)
                    except Exception:
                        continue
                    if j.get("error"):
                        err = j["error"]
                    got = j.get("model") or got
                if code == 200 and not err:
                    print(f"  ✅ {m:24s} -> 返回 {got}")
                else:
                    msg = json.dumps(err, ensure_ascii=False)[:150] if err else f"HTTP {code}"
                    print(f"  ❌ {m:24s} -> {msg}")
    except Exception as e:
        print(f"  ❌ {m:24s} -> {type(e).__name__}: {str(e)[:120]}")
