#!/usr/bin/env python3
"""测试国际版(codebuddy.ai)账号能否出指定模型。"""
import json
import os
import pathlib
import sys

AUTH_DIR = pathlib.Path(os.environ.get("CODEBUDDY_AUTH_DIR", "/data/auth"))
sys.path.insert(0, os.environ.get("CONVERTER_APP_DIR", "/app"))
import httpx
from converter import CredentialManager

MODEL = sys.argv[1] if len(sys.argv) > 1 else "deepseek-v4.1-flash"
PASSWORD = None

# 用法：python3 test_intl_model.py [模型名] [账号文件名 ...]
FILES = sys.argv[2:] or [p.name for p in sorted(AUTH_DIR.glob("*.info"))]

for fname in FILES:
    p = AUTH_DIR / fname
    if not p.exists():
        print(f"--- {fname}: 不存在，跳过")
        continue
    cm = CredentialManager(p)
    print(f"--- {fname}")
    print(f"    domain={cm.domain} backend={cm.backend} proxy={cm.proxy or '直连'}")
    try:
        headers = cm.get_headers()
    except Exception as e:
        print(f"    凭据加载失败: {e}")
        continue
    body = {
        "model": MODEL,
        "messages": [
            {"role": "system", "content": "You are a helpful assistant."},
            {"role": "user", "content": "Reply with exactly: intl-model-ok"},
        ],
        "stream": True,
        "max_tokens": 32,
        "stream_options": {"include_usage": True},
    }
    kw = {"timeout": 90}
    if cm.proxy:
        kw["proxy"] = cm.proxy
    try:
        with httpx.Client(**kw) as c:
            with c.stream("POST", cm.backend + "/v2/chat/completions",
                          headers=headers, json=body) as r:
                print(f"    HTTP {r.status_code}")
                got_model, usage, err = None, None, None
                parts = []
                raw_head = ""
                for line in r.iter_lines():
                    if not raw_head:
                        raw_head = line[:180]
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
                        err = str(j["error"])[:220]
                    got_model = j.get("model") or got_model
                    if j.get("usage"):
                        usage = j["usage"]
                    for ch in j.get("choices") or []:
                        ctt = (ch.get("delta") or {}).get("content")
                        if ctt:
                            parts.append(ctt)
                print(f"    返回 model = {got_model}")
                print(f"    内容 = {''.join(parts)[:100]!r}")
                print(f"    tokens = {usage.get('total_tokens') if usage else None}")
                if err:
                    print(f"    错误 = {err}")
                elif r.status_code != 200:
                    print(f"    原始 = {raw_head}")
    except Exception as e:
        print(f"    异常 {type(e).__name__}: {str(e)[:200]}")
