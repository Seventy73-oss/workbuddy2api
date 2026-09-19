"""离线验证按区分发逻辑（不碰真实账号、不发请求）。

用法：python3 test_routing.py
依赖 auth 目录里的 model_regions.json / models.json（可用 config/ 下的示例文件）。
"""
import os
import pathlib
import sys, threading

AUTH_DIR = pathlib.Path(os.environ.get("CODEBUDDY_AUTH_DIR", "/data/auth"))
sys.path.insert(0, os.environ.get("CONVERTER_APP_DIR", "/app"))
import converter as C

# 仅用于构造假账号的标签，不读取真实凭据
CN = 'cn-account.info'
I1 = 'intl-account-1.info'
I2 = 'intl-account-2.info'


class FakeMgr:
    def __init__(self, name, region, cooled=False):
        self.path = str(AUTH_DIR / name)
        self._region = region
        self._cooled = cooled

    @property
    def region(self):
        return self._region

    def is_cooled(self):
        return self._cooled

    def cool_remaining(self):
        return 30.0


def make_pool(mgrs):
    p = C.AccountPool.__new__(C.AccountPool)
    p._lock = threading.Lock()
    p._managers = {}
    p._order = []
    p._idx = 0
    p._affinity = {}
    p._sticky = None
    p._dir_mtime = 0
    p._scan = lambda: None          # 跳过真实目录扫描
    for m in mgrs:
        p._managers[m.path] = m
        p._order.append(m.path)
    return p


def region_of(mgr):
    return mgr.region if mgr is not None else None


fails = []


def check(label, got, want):
    ok = got == want
    print("  %-58s got=%-22s want=%-22s %s" % (label, got, want, "OK" if ok else "FAIL"))
    if not ok:
        fails.append(label)


print("=== 1) model_regions.json 解析 ===")
regions = C.load_model_regions()
print("  ", regions)
check("glm-5.3-flash", sorted(regions.get('glm-5.3-flash', [])), ['cn'])
check("kimi-k3", sorted(regions.get('kimi-k3', [])), ['cn'])
check("deepseek-v4.1-flash", sorted(regions.get('deepseek-v4.1-flash', [])), ['cn', 'intl'])
check("hy4-preview", sorted(regions.get('hy4-preview', [])), ['cn', 'intl'])
check("未列出模型=不限制", C.allowed_regions_for_model('glm-5.3'), None)

print("\n=== 2) 受限模型只落对应区（各 40 次，遍历轮询游标）===")
pool = make_pool([FakeMgr(CN, 'cn'), FakeMgr(I1, 'intl'), FakeMgr(I2, 'intl')])
got = set()
for i in range(40):
    got.add(region_of(pool.pick('glm-%d' % i, 'glm-5.3-flash')))
check("glm-5.3-flash 只落 cn", sorted(got), ['cn'])

got = set()
for i in range(40):
    got.add(region_of(pool.pick('kim-%d' % i, 'kimi-k3')))
check("kimi-k3 只落 cn", sorted(got), ['cn'])

got = set()
for i in range(40):
    got.add(region_of(pool.pick('ds-%d' % i, 'deepseek-v4.1-flash')))
check("deepseek-v4.1-flash 可落 cn+intl", sorted(got), ['cn', 'intl'])

got = set()
for i in range(40):
    got.add(region_of(pool.pick('hy-%d' % i, 'hy4-preview')))
check("hy4-preview 可落 cn+intl", sorted(got), ['cn', 'intl'])

got = set()
for i in range(40):
    got.add(region_of(pool.pick('any-%d' % i, 'glm-5.3')))
check("未列出模型不限制", sorted(got), ['cn', 'intl'])

print("\n=== 3) 会话粘性 + 跨区切换 ===")
pool2 = make_pool([FakeMgr(CN, 'cn'), FakeMgr(I1, 'intl'), FakeMgr(I2, 'intl')])
m1 = pool2.pick('sess-A', 'hy4-preview')
m2 = pool2.pick('sess-A', 'hy4-preview')
check("同会话同模型粘住同账号", m1.path == m2.path, True)

# 手动把会话绑到国际账号，再请求国区独占模型 → 必须改绑国区
intl_mgr = pool2._managers[str(AUTH_DIR / I1)]
pool2._bind_locked('sess-B', intl_mgr)
check("预置绑定=国际区", region_of(pool2._bound_locked('sess-B', None)), 'intl')
m3 = pool2.pick('sess-B', 'kimi-k3')
check("国际绑定遇国区独占模型 → 改绑国区", region_of(m3), 'cn')
check("改绑后绑定已更新", region_of(pool2._bound_locked('sess-B', None)), 'cn')

print("\n=== 4) 目标区没有账号时的行为 ===")
# 只留国际账号（模拟国区账号被移除/停用）
pool3 = make_pool([FakeMgr(I1, 'intl'), FakeMgr(I2, 'intl')])
check("kimi-k3 无国区账号 → None(触发 503)", region_of(pool3.pick('x', 'kimi-k3')), None)
check("hy4-preview 仍有国际账号可用", region_of(pool3.pick('y', 'hy4-preview')), 'intl')

# 国区账号存在但冷却：沿用旧行为（挑冷却最快结束的，而不是直接失败）
pool4 = make_pool([FakeMgr(CN, 'cn', cooled=True), FakeMgr(I1, 'intl')])
check("国区账号冷却时仍选它重试(旧行为)", region_of(pool4.pick('z', 'kimi-k3')), 'cn')
check("冷却账号不会污染国际模型", region_of(pool4.pick('w', 'hy4-preview')), 'intl')

print("\n=== 5) models.json ===")
# models.json 是运行时配置，不同部署内容不同：只校验已配置的模型都被列出。
listed = set(C.load_model_list())
expected = {'glm-5.3-flash', 'kimi-k3', 'deepseek-v4.1-flash', 'hy4-preview'}
missing = expected - listed
check("对外模型清单包含核心模型", sorted(missing), [])

print("\n结果：%s" % ("全部通过" if not fails else "失败 %d 项: %s" % (len(fails), fails)))
sys.exit(1 if fails else 0)
