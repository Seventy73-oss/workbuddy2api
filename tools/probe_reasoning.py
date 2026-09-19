#!/usr/bin/env python3
"""探测后端对「思考强度」参数的接受度：deepseek-v4.1-flash。"""
import json
import os
import pathlib
import sys

AUTH_DIR = pathlib.Path(os.environ.get("CODEBUDDY_AUTH_DIR", "/data/auth"))
sys.path.insert(0, os.environ.get("CONVERTER_APP_DIR", "/app"))
import httpx
from converter import CredentialManager

# 用法：python3 probe_reasoning.py [账号文件名]
ACCT = sys.argv[1] if len(sys.argv) > 1 else next(
    (p.name for p in sorted(AUTH_DIR.glob("*.info"))), "")
MODEL = "deepseek-v4.1-flash"

VARIANTS = [
    ("baseline(不传)", {}),
    ("reasoning_effort=max", {"reasoning_effort": "max"}),
    ("reasoning_effort=high", {"reasoning_effort": "high"}),
    ("reasoning_effort=medium", {"reasoning_effort": "medium"}),
    ("reasoning_effort=low", {"reasoning_effort": "low"}),
    ("reasoning_effort=minimal", {"reasoning_effort": "minimal"}),
    ("thinking={type:enabled}", {"thinking": {"type": "enabled"}}),
    ("thinking={type:enabled,budget:8192}", {"thinking": {"type": "enabled", "budget_tokens": 8192}}),
    ("thinking={type:disabled}", {"thinking": {"type": "disabled"}}),
    ("enable_thinking=True", {"enable_thinking": True}),
    ("reasoning={effort:max}", {"reasoning": {"effort": "max"}}),
    ("thinking_budget=8192", {"thinking_budget": 8192}),
]

cm = CredentialManager(AUTH_DIR / ACCT)
headers = cm.get_headers()
kw = {"timeout": 90}
if cm.proxy:
    kw["proxy"] = cm.proxy
print(f"账号 {ACCT} | backend={cm.backend} | proxy={cm.proxy or '直连'} | 模型 {MODEL}\n")

for name, extra in VARIANTS:
    body = {
        "model": MODEL,
        "messages": [{"role": "system", "content": "You are a helpful assistant."},
                     {"role": "user", "content": "Compute 17*23. Reply with the number only."}],
        "stream": True, "max_tokens": 200, "stream_options": {"include_usage": True},
    }
    body.update(extra)
    try:
        with httpx.Client(**kw) as c:
            with c.stream("POST", cm.backend + "/v2/chat/completions",
                          headers=headers, json=body) as r:
                code, err, reason, content, usage = r.status_code, None, [], [], None
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
                        dl = ch.get("delta") or {}
                        if dl.get("reasoning_content"):
                            reason.append(dl["reasoning_content"])
                        if dl.get("content"):
                            content.append(dl["content"])
                rt = (usage or {}).get("completion_tokens_details", {}).get("reasoning_tokens")
                if err:
                    msg = json.dumps(err, ensure_ascii=False)[:130]
                    print(f"  ❌ {name:34s} HTTP {code} | {msg}")
                else:
                    print(f"  ✅ {name:34s} HTTP {code} | reasoning字符={len(''.join(reason)):5d} "
                          f"reasoning_tokens={rt} total={((usage or {}).get('total_tokens'))} "
                          f"| 答案={''.join(content)[:20]!r}")
    except Exception as e:
        print(f"  ❌ {name:34s} EXC {type(e).__name__}: {str(e)[:100]}")
