# workbuddy2api

把 **WorkBuddy / CodeBuddy** 订阅账号，变成一个标准的 **OpenAI / Anthropic / Responses 兼容 API**。

直连腾讯 WorkBuddy 后端，原生支持 function calling、流式 SSE、思考强度（reasoning effort），
内置**多账号轮询池**、**限流冷却与故障转移**、**会话粘性**、**按区分发**，
并附带一个完整的**中文 Web 管理面板**。

> [!WARNING]
> **仅供个人学习与技术研究，禁止任何商业用途。**
>
> 本项目**未获腾讯或任何权利方授权**，与其无任何隶属或合作关系。使用它可能**违反
> WorkBuddy / CodeBuddy 的服务条款**，并可能导致**账号被封禁、订阅费用损失**。
>
> 请仅使用**你自己合法拥有的账号**，风险自负。完整条款见 [免责声明](#免责声明)。

---

## 目录

- [特性](#特性)
- [架构与端口](#架构与端口)
- [快速开始（Docker）](#快速开始docker)
- [本地运行（不用 Docker）](#本地运行不用-docker)
- [配置](#配置)
- [使用](#使用)
- [API 端点](#api-端点)
- [目录结构](#目录结构)
- [常见问题](#常见问题)
- [免责声明](#免责声明)

---

## 特性

**协议兼容**

- `POST /v1/chat/completions` —— OpenAI Chat Completions（含原生 `tools` / `tool_calls`）
- `POST /v1/messages` —— Anthropic Messages（Claude Code 等客户端可直接用）
- `POST /v1/messages/count_tokens` —— Anthropic token 计数
- `POST /v1/responses` —— OpenAI Responses API
- `GET  /v1/models` —— 模型列表

**多账号与稳定性**

- 扫描 auth 目录下所有 `*.info`，自动组成**轮询池**
- 账号被上游 429 / 超额时自动进入**冷却**，到期自动恢复；冷却期间请求自动**故障转移**到其他账号
- **会话粘性**：同一会话固定在同一账号上，命中上游 prompt cache，避免来回切号导致 token 爆炸
- **按区分发**：国区（`copilot.tencent.com`）与国际区（`*.codebuddy.ai`）账号自动分流，
  受限模型只落在支持它的区上
- access token 过期**自动续期**并回写 auth 文件

**Web 管理面板（中文）**

- 导入 / 删除 / 启停账号，查看每个账号的 token 到期与可用性
- 一键探测各模型可用性、查看用量与请求日志
- 设置 / 热更新 API Key（写入文件，无需重启）
- 每日自动签到

---

## 架构与端口

两个进程，由 supervisord（Docker）或 `start.sh`（本地）一起拉起。
**对外只暴露一个端口：3008。**

| 进程 | 作用 | 监听 | 是否对外 |
|---|---|---|---|
| `panel.py` | **Web 面板 + API 入口** | **3008** | ✅ 唯一对外端口 |
| `converter.py` | API 后端（被面板反代） | 127.0.0.1:3009 | ❌ 仅容器内网 |

```
                    ┌────────────────────────────────────────┐
   浏览器  ───────▶ │  panel.py                      :3008   │
   http://host:3008 │  ├─ /          管理面板                │
                    │  └─ /v1/*  ─┐  反代                   │
                    └─────────────┼──────────────────────────┘
                                  │ 127.0.0.1:3009
   客户端  ───────▶  （同一个地址）▼
   http://host:3008/v1        ┌──────────────────────────┐
                              │  converter.py   :3009    │ ──▶ copilot.tencent.com
                              │  多账号池 / 粘性 / 冷却   │
                              └──────────────────────────┘
```

**面板和 API 用同一个地址、同一个端口**：

| 用途 | 地址 |
|---|---|
| 管理面板 | `http://<host>:3008` |
| API Base URL | `http://<host>:3008/v1` |

> 3008 上没有任何写死的 IP 或端口，`PANEL_PORT` 可改。
> API 进程只监听回环，不会单独暴露到宿主机。

---

## 快速开始（Docker）

```bash
git clone https://github.com/<your-name>/workbuddy2api.git
cd workbuddy2api

cp .env.example .env
# 按需修改 .env（端口 / 时区）

docker compose up -d --build
```

打开 **<http://localhost:3008>** 就是管理面板；
API Base URL 也是同一个地址 **<http://localhost:3008/v1>**。

查看日志：

```bash
docker compose logs -f
```

数据（账号凭据、API Key、统计日志）持久化在 `./data/auth`。

---

## 本地运行（不用 Docker）

需要 Python 3.10+。

```bash
pip install -r requirements.txt
bash start.sh
```

或者手动分两个终端：

```bash
# 终端 1 —— API 后端（只监听回环）
cd app
CODEBUDDY_AUTH_DIR=../data/auth API_PORT=3009 \
  python3 converter.py --host 127.0.0.1 --skip-check --desensitize \
  --api-key-file ../data/auth/.api_key

# 终端 2 —— 面板 + 对外入口
export CODEBUDDY_AUTH_DIR="$PWD/../data/auth"
export CONVERTER_BASE="http://127.0.0.1:3009"
export PANEL_PORT=3008
python3 panel.py
```

然后访问 `http://localhost:3008`。

> 在 Windows / macOS 上，如果不指定 `CODEBUDDY_AUTH_DIR`，
> converter 会自动去 WorkBuddy / CodeBuddy 桌面端的默认凭据目录找已登录的账号。

---

## 配置

所有配置都走环境变量（Docker 下写在 `.env`）。

### 端口

| 变量 | 默认 | 说明 |
|---|---|---|
| `PANEL_PORT` | `3008` | **唯一对外端口**：面板 + `/v1` API |
| `API_PORT` | `3009` | 内网 API 端口，仅容器内使用，不对宿主机发布 |
| `CONVERTER_BASE` | `http://127.0.0.1:3009` | 面板反代的目标地址 |
| `PUBLIC_API_BASE` | 空 | 面板展示给客户端的 API 地址。留空 = 同源（默认）。只有走独立域名时才设置 |
| `TZ` | `Asia/Shanghai` | 容器时区 |

### 账号与凭据

| 变量 | 默认 | 说明 |
|---|---|---|
| `CODEBUDDY_AUTH_DIR` | `/data/auth` | 凭据目录，存放 `*.info` 账号文件与 `.api_key` |
| `CODEBUDDY_MODELS_FILE` | `$AUTH_DIR/models.json` | 模型列表文件（示例见 `config/models.example.json`） |
| `CODEBUDDY_MODEL_REGIONS_FILE` | `$AUTH_DIR/model_regions.json` | 模型分区配置（示例见 `config/model_regions.example.json`） |

### 稳定性调优

| 变量 | 默认 | 说明 |
|---|---|---|
| `CODEBUDDY_RATE_COOLDOWN` | `30` | 账号被限流后跳过多少秒 |
| `CODEBUDDY_FAILOVER_COOLDOWN` | `60` | 故障转移冷却秒数 |
| `CODEBUDDY_SESSION_TTL` | `1800` | 会话粘性 TTL（秒），30 分钟无活动解绑 |
| `CODEBUDDY_SESSION_MAX` | `2000` | 粘性会话表上限 |
| `CODEBUDDY_RATE_RETRY` | `3` | 限流后重试次数 |
| `CODEBUDDY_REASONING_EFFORT` | 空 | 全局思考强度：`low` / `medium` / `high` / `max`。留空则用请求里的值 |

### 鉴权

| 变量 | 说明 |
|---|---|
| `CODEBUDDY2OPENAI_KEY` | 客户端 Bearer Key。**推荐改用面板写入 `$AUTH_DIR/.api_key`**，可热更新无需重启 |
| `CODEBUDDY2OPENAI_KEY_FILE` | Key 文件路径（默认 `$AUTH_DIR/.api_key`）。文件不存在或为空 = 不校验 |

> 优先级：`--api-key` > `--api-key-file` > 不校验。
> 生产环境**务必设置 Key**，否则任何能访问到 3008 的人都能白嫖你的账号。

---

## 使用

### 1. 导入账号

打开面板 <http://localhost:3008> → **账号** → **导入**，
粘贴 WorkBuddy / CodeBuddy 桌面端导出的凭据 JSON。

可以一次导入多条（JSON 数组），也可以批量指定代理。

> 凭据格式兼容导出的 `[{access_token, refresh_token, uid, ...}]`。

### 2. 设置 API Key

面板 → **API 密钥** → 新建 Key。生成后**只显示一次**，请立刻保存。

Key 写入 `$AUTH_DIR/.api_key`，converter 热读取，**无需重启**。

### 3. 调用

面板的「总览 → 接口接入」页会直接给出可复制的 curl 命令和当前 Base URL。

**OpenAI 格式**

```bash
curl http://localhost:3008/v1/chat/completions \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer $WB2API_KEY" \
  -d '{
    "model": "deepseek-v4.1-flash",
    "messages": [{"role": "user", "content": "你好"}],
    "stream": true
  }'
```

**Anthropic 格式（Claude Code 等）**

```bash
curl http://localhost:3008/v1/messages \
  -H "Content-Type: application/json" \
  -H "x-api-key: $WB2API_KEY" \
  -d '{
    "model": "deepseek-v4.1-flash",
    "max_tokens": 1024,
    "messages": [{"role": "user", "content": "你好"}]
  }'
```

**OpenAI 客户端 / SDK**

```python
from openai import OpenAI

client = OpenAI(base_url="http://localhost:3008/v1", api_key="<你的 key>")
print(client.chat.completions.create(
    model="deepseek-v4.1-flash",
    messages=[{"role": "user", "content": "你好"}],
).choices[0].message.content)
```

**查看可用模型**

```bash
curl http://localhost:3008/v1/models -H "Authorization: Bearer $WB2API_KEY"
```

---

## API 端点

### API（经面板 3008 反代）

| 方法 | 路径 | 说明 |
|---|---|---|
| GET | `/health` | 健康检查 + 账号池状态 |
| GET | `/v1/models` | 模型列表 |
| POST | `/v1/chat/completions` | OpenAI Chat Completions |
| POST | `/v1/messages` | Anthropic Messages |
| POST | `/v1/messages/count_tokens` | Anthropic token 计数 |
| POST | `/v1/responses` | OpenAI Responses API |

### 面板管理接口（3008）

| 方法 | 路径 | 说明 |
|---|---|---|
| GET | `/` | 管理面板页面 |
| GET | `/api/status` | 运行状态 |
| GET | `/api/accounts` | 账号列表 |
| POST | `/api/accounts/import` | 导入账号 |
| POST | `/api/accounts/{file}/toggle` | 启用 / 停用账号 |
| DELETE | `/api/accounts/{file}` | 删除账号 |
| GET/POST | `/api/key` | 读取 / 设置 API Key |
| GET | `/api/models` | 模型列表 |
| POST | `/api/models/probe` | 探测模型可用性 |
| GET | `/api/usage` | 用量统计 |
| GET | `/api/logs` | 请求日志 |
| GET | `/api/token-stats` | Token 统计 |

> 面板管理接口**没有鉴权**，请勿直接暴露到公网。要远程访问请套一层反向代理 + Basic Auth / VPN。

---

## 目录结构

```
workbuddy2api/
├── app/
│   ├── converter.py            # API 后端：多账号池、协议转换、转发
│   ├── panel.py                # Web 面板 + /v1 反向代理（对外入口）
│   ├── anthropic_adapter.py    # Anthropic Messages <-> Chat 转换
│   ├── responses_adapter.py    # OpenAI Responses <-> Chat 转换
│   ├── responses_projection.py # Responses 投影
│   ├── desensitize.py          # 内容脱敏（缓解上游审核误拦）
│   └── daily_checkin.py        # 每日自动签到
├── ui/
│   └── index.html              # 面板前端
├── tools/                      # 诊断 / 探测脚本（非运行必需）
├── config/                     # 示例配置
├── scripts/crontab.example     # 定时签到示例
├── Dockerfile
├── docker-compose.yml
├── supervisord.conf            # 同时拉起 API + 面板
├── start.sh                    # 本地（非 Docker）启动
├── requirements.txt
├── .env.example
└── LICENSE
```

---

## 常见问题

**Q：为什么 API 也是 3008？**

面板进程同时承担反向代理：`/v1/*` 会被转发到内网 3009 上的 API 后端。
这样你只需要记住一个地址，也少暴露一个端口。API 后端本身只监听回环，外部访问不到。

**Q：面板能打开，但 API 调用返回 401 / 没有账号？**

先确认 auth 目录里已经导入了 `*.info` 账号。面板「账号」页应能看到账号并显示健康状态。
再确认 converter 进程用的是同一个 `CODEBUDDY_AUTH_DIR`。

**Q：返回「所有账号都在冷却中」怎么办？**

说明当前所有账号都被上游限流了。等冷却结束（面板会显示解除时刻），或再导入一个账号。
可以调大 `CODEBUDDY_RATE_COOLDOWN` 减少无效重试。

**Q：某个模型报「无可用账号 / 503」？**

该模型被 `model_regions.json` 限制在特定区（如 `kimi-k3` 只在国区），
而你当前没有该区的可用账号。加一个对应区的账号，或调整 `model_regions.json`。

**Q：Claude Code 连不上？**

Claude Code 走 Anthropic 协议，把 Base URL 指向 `http://<host>:3008`，
模型名用 `/v1/models` 里列出的名称。注意 Claude Code 会带很大的 system prompt，
如果遇到上游审核拦截，保持 `--desensitize` 开启。

**Q：想在前面再套一层 Nginx？**

```nginx
location / {
    proxy_pass http://127.0.0.1:3008;
    proxy_http_version 1.1;
    proxy_set_header Host $host;
    # SSE 流式必须关闭缓冲
    proxy_buffering off;
    proxy_read_timeout 600s;
}
```

这时可以设置 `PUBLIC_API_BASE=https://你的域名`，面板里展示的 Base URL 才会是外部域名。

**Q：端口 3008 被占用了？**

改 `.env` 里的 `PANEL_PORT`，`docker compose up -d` 即可。代码里没有写死的端口。

---

## 免责声明

> **请在下载、安装或使用本项目之前，完整阅读本节。**
> 继续使用即表示你已阅读、理解并同意以下全部条款。如果你不同意其中任何一条，请立即停止使用并删除本项目。

### 1. 项目性质

本项目是一个**技术研究与学习**性质的工具，用于演示协议转换、多账号调度与反向代理等通用工程实践。
它通过逆向分析的方式与第三方服务（腾讯 WorkBuddy / CodeBuddy）的后端接口进行交互。
本项目**不是**上述服务的官方客户端，与腾讯及其关联公司**没有任何隶属、合作、赞助或背书关系**。

### 2. 无授权声明

本项目**未获得**腾讯公司或任何相关权利方的授权、许可或认可。
所有涉及的商标、服务名称、接口与后端服务，其知识产权均归各自权利人所有。
本项目仅包含独立编写的源代码，**不包含**任何来自上述服务的专有代码、二进制文件或商业秘密。

### 3. 使用者的责任与义务

使用者必须自行确保其使用行为合法合规，包括但不限于：

- 遵守 [CodeBuddy](https://www.codebuddy.cn/) / 腾讯云的服务条款、用户协议及可接受使用政策；
- 遵守你所在国家或地区的法律法规；
- 仅使用**你自己合法拥有或已获授权**的账号，不得使用他人账号；
- 不得将本工具用于商业转售、账号出租、代充、薅羊毛、批量注册或任何形式的滥用；
- 不得利用本工具规避付费、攻击服务、干扰服务正常运行或从事其他不正当行为。

### 4. 风险自担

使用本项目**可能违反**第三方服务的服务条款，并可能导致：

- 账号被限流、封禁或永久停用；
- 已支付的订阅费用损失；
- 账号内数据丢失；
- 其他你未预见的后果。

**上述全部风险由使用者自行承担。** 项目作者与贡献者不对任何直接或间接损失负责。

### 5. 免责条款

本项目按**「现状」（AS IS）**提供，不附带任何形式的明示或默示担保，包括但不限于对适销性、特定用途适用性及不侵权的担保。
在任何情况下，作者与贡献者均不对因使用或无法使用本项目而产生的任何索赔、损害或其他责任负责，无论其源于合同、侵权还是其他方式。

### 6. 停止使用

如果你收到权利人的通知，或意识到自己的使用行为可能构成侵权或违约，请**立即停止使用并删除本项目**。

---

## 隐私说明

本仓库**不包含**任何个人信息或真实凭据，包括但不限于：

- 账号、手机号、邮箱、用户名等个人标识；
- access token / refresh token / API Key / 各类密钥；
- 内网 IP、主机名、NAS 路径等部署环境信息；
- 请求日志、签到记录、用量统计等运行数据。

所有敏感配置均通过**环境变量**注入，仓库内只提供 `config/*.example.json` 形式的占位示例。
若你发现仓库中仍残留任何隐私信息，请提交 issue，我们会尽快移除。

---

## License

[MIT](LICENSE)
