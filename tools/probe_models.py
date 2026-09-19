"""批量探测账号对每个模型的可用性，输出矩阵。

用法：python3 probe_models.py [账号文件名 ...]
不传账号则自动使用 auth 目录下全部 *.info。
环境变量：CODEBUDDY_AUTH_DIR（默认 /data/auth）
"""
import json, os, sys, concurrent.futures
from pathlib import Path
import httpx

AUTH = Path(os.environ.get('CODEBUDDY_AUTH_DIR', '/data/auth'))
SYS = {"role": "system", "content": "You are a helpful assistant."}

def is_intl(d):
    return d.endswith('.codebuddy.ai') or d.endswith('.workbuddy.ai')

def ctx(fn):
    s = json.loads((AUTH / fn).read_text(encoding='utf-8'))
    auth = s.get('auth') or {}; acct = s.get('account') or {}
    domain = auth.get('domain') or 'www.codebuddy.cn'
    h = {"Content-Type": "application/json", "Accept": "application/json",
         "Authorization": "Bearer " + auth.get('accessToken', ''),
         "X-User-Id": acct.get('uid', ''), "X-Enterprise-Id": acct.get('enterpriseId', ''),
         "X-Tenant-Id": acct.get('enterpriseId', ''), "X-Domain": domain,
         "X-Product": "SaaS", "X-IDE-Name": "CodeBuddyIDE",
         "X-Requested-With": "XMLHttpRequest", "User-Agent": "CodeBuddyIDE"}
    if is_intl(domain):
        be = "https://www." + ("workbuddy.ai" if domain.endswith('.workbuddy.ai') else "codebuddy.ai")
        h["Origin"] = be; h["Referer"] = be + "/"; h["User-Agent"] = "CLI/2.63.2 CodeBuddy/2.63.2"
    return s, h, (s.get('backend') or ("https://www.codebuddy.ai" if is_intl(domain) else "https://copilot.tencent.com"))

def probe(fn, model, retries=2):
    s, h, be = ctx(fn)
    body = {"model": model, "messages": [SYS, {"role": "user", "content": "hi"}], "max_tokens": 8, "stream": True}
    kw = {"timeout": 90}
    if s.get('proxy'):
        kw["proxy"] = s['proxy']
    for a in range(retries + 1):
        try:
            with httpx.Client(**kw) as c:
                with c.stream("POST", be + "/v2/chat/completions", headers=h, json=body) as r:
                    if r.status_code != 200:
                        raw = r.read()[:400].decode('utf-8', 'replace')
                        try:
                            j = json.loads(raw); msg = str(j.get('msg', ''))
                        except Exception:
                            msg = raw[:120]
                        if 'service info not found' in msg:
                            return ("ABSENT", msg)
                        if 'authorized users' in msg:
                            return ("GATED", msg)
                        if 'too many requests' in msg or r.status_code == 429:
                            import time; time.sleep(6); continue
                        return ("OTHER", f"{r.status_code}: {msg[:100]}")
                    got = b""
                    for chunk in r.iter_bytes():
                        got += chunk
                        if len(got) > 200: break
            return ("OK", "ok")
        except Exception as e:
            return ("ERR", f"{type(e).__name__}: {str(e)[:90]}")
    return ("OTHER", "rate-limited")

NAMES = ["glm-5.3","glm-5.3-flash","glm-5.2","glm-5.1","glm-5v-turbo","kimi-k3","kimi-k2.7","kimi-k2.6",
 "kimi-k2.5","deepseek-v4-pro","deepseek-v4.1-flash","deepseek-v4-flash","minimax-m3-pay","minimax-m3",
 "minimax-m2.7","hy4-preview","hy3-preview","hy3-preview-agent","hy3","hunyuan-t1-vision","auto",
 "gemini-3.1-pro","gemini-3.5-flash","claude-sonnet-4.6","claude-opus-4.6","claude-opus-5",
 "claude-sonnet-5","claude-opus-4.7","gemini-2.5-pro","gemini-2.5-flash","gemini-3.1-flash-lite",
 "gpt-5.1","gpt-5.4"]

ACCTS = sys.argv[1:] or [p.name for p in sorted(AUTH.glob('*.info'))]
mat = {}
for fn in ACCTS:
    res = {}
    with concurrent.futures.ThreadPoolExecutor(max_workers=3) as ex:
        futs = {ex.submit(probe, fn, m): m for m in NAMES}
        for f in concurrent.futures.as_completed(futs):
            res[futs[f]] = f.result()
    mat[fn] = res

# 列标题用账号名（截断），保持输出对齐
labels = [a[:14] for a in ACCTS]
print("%-26s %s" % ("MODEL", " ".join("%-8s" % x for x in labels)))
for m in NAMES:
    row = [mat[a][m][0] for a in ACCTS]
    if set(row) != {"ABSENT"}:
        print("%-26s %s" % (m, " ".join("%-8s" % x for x in row)))
print("\n--- 完整详情 ---")
for m in NAMES:
    row = [mat[a][m][0] for a in ACCTS]
    if set(row) != {"ABSENT"}:
        detail = mat[ACCTS[0]][m][1][:80] if ACCTS else ""
        print("%-26s %s | %s" % (m, " ".join("%-8s" % x for x in row), detail))
for fn in ACCTS:
    ok = [m for m in NAMES if mat[fn][m][0] == 'OK']
    print("\n%s OK (%d): %s" % (fn, len(ok), ", ".join(ok)))
out = Path(os.environ.get('PROBE_OUT', '/tmp/final_matrix.json'))
out.write_text(json.dumps({a: {m: mat[a][m] for m in NAMES} for a in ACCTS}, ensure_ascii=False, indent=1), encoding='utf-8')
print("DONE ->", out)
