# tools —— 诊断 / 探测脚本

这些脚本是开发期用来排查后端行为的，**不是服务运行所必需**的。
它们都会从 `CODEBUDDY_AUTH_DIR`（默认 `/data/auth`）读取账号，
通过 `CONVERTER_APP_DIR`（默认 `/app`）找到 `converter` 模块。

本地跑的时候通常这样用：

```bash
export CODEBUDDY_AUTH_DIR=/path/to/data/auth
export CONVERTER_APP_DIR=../app
python3 test_routing.py
```

| 脚本 | 作用 |
|---|---|
| `test_routing.py` | 离线验证「按区分发 + 会话粘性」逻辑，不碰真实账号、不发请求 |
| `probe_models.py` | 批量探测各账号对每个模型的可用性，输出矩阵 |
| `sweep_cn_models.py` | 扫描国区账号对 `models.json` 里每个模型的可用性 |
| `sweep_intl_models.py` | 扫描国际版账号对每个模型的可用性 |
| `probe_reasoning.py` | 探测后端对 `reasoning_effort` / `thinking` 等参数的接受度 |
| `probe_reasoning2.py` | 同上，覆盖其他模型与国区后端 |
| `probe_tools_thinking.py` | 验证「带 tools + reasoning_effort」是否导致上游流中断 |
| `err_intl_models.py` | 打印失败模型的原始报错，判断是模型不存在 / 无权限 / 额度问题 |
| `test_intl_model.py` | 测试国际版账号能否出指定模型 |

> 这些脚本会**真实消耗账号额度**（除 `test_routing.py` 外），请按需运行。
