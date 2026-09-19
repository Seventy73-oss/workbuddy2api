#!/usr/bin/env python3
"""打印国际版账号对失败模型的原始报错（判断是模型不存在 / 无权限 / 额度问题）。"""
import json
import os
import pathlib
import sys

AUTH_DIR = pathlib.Path(os.environ.get("CODEBUDDY_AUTH_DIR", "/data/auth"))
sys.path.insert(0, os.environ.get("CONVERTER_APP_DIR", "/app"))
import httpx
from converter import CredentialManager

FAILING = ["glm-5.3-flash", "deepseek-v4-pro", "minimax-m3-pay",
           "hy3-preview", "hy3-preview-agent", "hunyuan-t1-vision"]

# 用法：python3 err_intl_models.py [账号文件名 ...]
FILES = sys.argv[1:] or [p.name for p in sorted(AUTH_DIR.glob("*.info"))]

for fname in FILES:
    p = AUTH_DIR / fname
    if not p.exists():
        continue
    cm = CredentialManager(p)
    kw = {"timeout": 60}
    if cm.proxy:
        kw["proxy"] = cm.proxy
    print(f"=== {fname} | proxy={cm.proxy or '直连'} ===")
    try:
        headers = cm.get_headers()
    except Exception as e:
        print("  凭据失败:", e)
        continue
    for m in FAILING:
        body = {"model": m,
                "messages": [{"role": "system", "content": "You are a helpful assistant."},
                             {"role": "user", "content": "hi"}],
                "stream": True, "max_tokens": 4}
        try:
            with httpx.Client(**kw) as c:
                with c.stream("POST", cm.backend + "/v2/chat/completions",
                              headers=headers, json=body) as r:
                    raw = b""
                    for chunk in r.iter_bytes():
                        raw += chunk
                        if len(raw) > 400:
                            break
                    print(f"  {m:22s} HTTP {r.status_code} | {raw.decode('utf-8','replace')[:230]!r}")
        except Exception as e:
            print(f"  {m:22s} EXC {type(e).__name__}: {str(e)[:150]}")
