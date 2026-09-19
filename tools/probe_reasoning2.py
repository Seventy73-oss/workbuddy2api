#!/usr/bin/env python3
"""探测 reasoning_effort=max 在其他模型 / 国区后端上的兼容性。"""
import json
import os
import pathlib
import sys

AUTH_DIR = pathlib.Path(os.environ.get("CODEBUDDY_AUTH_DIR", "/data/auth"))
sys.path.insert(0, os.environ.get("CONVERTER_APP_DIR", "/app"))
import httpx
from converter import CredentialManager

CN_FILE = None
for p in sorted(AUTH_DIR.glob("*.info*")):
    if "disabled" in p.name or True:
        try:
            cm0 = CredentialManager(p)
            if not cm0.is_intl:
                CN_FILE = p
                break
        except Exception:
            continue


def run(cm, model, extra):
    h = cm.get_headers()
    kw = {"timeout": 90}
    if cm.proxy:
        kw["proxy"] = cm.proxy
    body = {"model": model,
            "messages": [{"role": "system", "content": "You are a helpful assistant."},
                         {"role": "user", "content": "Compute 17*23. Reply with the number only."}],
            "stream": True, "max_tokens": 200, "stream_options": {"include_usage": True}}
    body.update(extra)
    try:
        with httpx.Client(**kw) as c:
            with c.stream("POST", cm.backend + "/v2/chat/completions", headers=h, json=body) as r:
                code, err, reason, usage = r.status_code, None, [], None
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
                    if j.get("usage"):
                        usage = j["usage"]
                    for ch in j.get("choices") or []:
                        rc = (ch.get("delta") or {}).get("reasoning_content")
                        if rc:
                            reason.append(rc)
                rt = (usage or {}).get("completion_tokens_details", {}).get("reasoning_tokens")
                if err:
                    return f"❌ HTTP {code} {json.dumps(err, ensure_ascii=False)[:110]}"
                return f"✅ HTTP {code} reasoning字符={len(''.join(reason))} reasoning_tokens={rt}"
    except Exception as e:
        return f"❌ EXC {type(e).__name__}: {str(e)[:90]}"


print("=== 国际版后端：其他模型 + reasoning_effort=max ===")
# 用法：python3 probe_reasoning2.py [国际版账号文件名]
INTL_FILE = sys.argv[1] if len(sys.argv) > 1 else next(
    (p.name for p in sorted(AUTH_DIR.glob("*.info"))), "")
cm = CredentialManager(AUTH_DIR / INTL_FILE)
for m in ["glm-5.3", "glm-5.2", "kimi-k3", "minimax-m3", "hy4-preview", "glm-5v-turbo"]:
    print(f"  {m:16s} max  -> {run(cm, m, {'reasoning_effort': 'max'})}")
    print(f"  {m:16s} 不传 -> {run(cm, m, {})}")

if CN_FILE:
    print(f"\n=== 国区后端（{CN_FILE.name}）：deepseek-v4.1-flash ===")
    cm2 = CredentialManager(CN_FILE)
    print(f"  max  -> {run(cm2, 'deepseek-v4.1-flash', {'reasoning_effort': 'max'})}")
    print(f"  不传 -> {run(cm2, 'deepseek-v4.1-flash', {})}")
