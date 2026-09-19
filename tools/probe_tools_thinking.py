#!/usr/bin/env python3
"""验证：带 tools 的请求 + reasoning_effort 是否导致上游流中断。"""
import json
import os
import pathlib
import sys

AUTH_DIR = pathlib.Path(os.environ.get("CODEBUDDY_AUTH_DIR", "/data/auth"))
sys.path.insert(0, os.environ.get("CONVERTER_APP_DIR", "/app"))
import httpx
from converter import CredentialManager

# 用法：python3 probe_tools_thinking.py [模型名]  （账号取 CODEBUDDY_ACCOUNT 或第一个 .info）
ACCT = os.environ.get("CODEBUDDY_ACCOUNT") or next(
    (p.name for p in sorted(AUTH_DIR.glob("*.info"))), "")
MODEL = sys.argv[1] if len(sys.argv) > 1 else "deepseek-v4.1-flash"

TOOLS = [{
    "type": "function",
    "function": {
        "name": "get_weather",
        "description": "Get current weather for a city",
        "parameters": {"type": "object", "properties": {"city": {"type": "string"}},
                       "required": ["city"]},
    },
}]
MSGS = [{"role": "system", "content": "You are a helpful assistant."},
        {"role": "user", "content": "What's the weather in Tokyo? Use the tool."}]

cm = CredentialManager(AUTH_DIR / ACCT)
kw = {"timeout": 120}
if cm.proxy:
    kw["proxy"] = cm.proxy
print(f"账号 {ACCT} | backend={cm.backend} | 模型 {MODEL}\n")

for label, extra in [("带tools / 不传effort", {}), ("带tools / effort=max", {"reasoning_effort": "max"}),
                     ("不带tools / effort=max", {"_notools": True})]:
    h = cm.get_headers()
    body = {"model": MODEL, "messages": MSGS, "stream": True, "max_tokens": 400,
            "stream_options": {"include_usage": True}}
    if not extra.get("_notools"):
        body["tools"] = TOOLS
    for k, v in extra.items():
        if not k.startswith("_"):
            body[k] = v
    events, finish, err, done, reason, content = 0, None, None, False, 0, []
    try:
        with httpx.Client(**kw) as c:
            with c.stream("POST", cm.backend + "/v2/chat/completions", headers=h, json=body) as r:
                code = r.status_code
                for line in r.iter_lines():
                    if not line:
                        continue
                    if not line.startswith("data:"):
                        events += 1
                        continue
                    d = line[5:].strip()
                    if d == "[DONE]":
                        done = True
                        continue
                    try:
                        j = json.loads(d)
                    except Exception:
                        continue
                    if j.get("error"):
                        err = j["error"]
                    for ch in j.get("choices") or []:
                        if ch.get("finish_reason"):
                            finish = ch["finish_reason"]
                        dl = ch.get("delta") or {}
                        if dl.get("reasoning_content"):
                            reason += len(dl["reasoning_content"])
                        if dl.get("content"):
                            content.append(dl["content"])
                        if dl.get("tool_calls"):
                            content.append("[tool_call]")
            print(f"  {label:26s} HTTP {code} | done={done} finish={finish} "
                  f"| reasoning={reason} | 正文={' '.join(content)[:60]!r}"
                  + (f" | ⚠️ERROR={json.dumps(err, ensure_ascii=False)[:120]}" if err else ""))
    except Exception as e:
        print(f"  {label:26s} 异常 {type(e).__name__}: {str(e)[:130]}")
