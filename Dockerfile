# syntax=docker/dockerfile:1
FROM python:3.12-slim

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1 \
    TZ=Asia/Shanghai \
    CODEBUDDY_AUTH_DIR=/data/auth \
    PANEL_PORT=3008 \
    API_PORT=3009

# supervisord 同时拉起 converter（API）与 panel（Web UI）两个进程
RUN apt-get update \
 && apt-get install -y --no-install-recommends supervisor tini \
 && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

COPY app/ ./
COPY ui/ ./ui/

# 数据卷：账号凭据、API key、统计与请求日志都在这里
VOLUME ["/data/auth"]

# 3008 = 唯一对外入口（Web UI + /v1 API 反代）；3009 只在容器内网监听
EXPOSE 3008 3009

HEALTHCHECK --interval=30s --timeout=5s --start-period=20s --retries=3 \
  CMD python3 -c "import os,urllib.request,sys; p=os.environ.get('API_PORT','3009'); sys.exit(0 if urllib.request.urlopen('http://127.0.0.1:'+p+'/health',timeout=3).status==200 else 1)"

ENTRYPOINT ["/usr/bin/tini", "--"]
CMD ["/usr/bin/supervisord", "-c", "/etc/supervisor/conf.d/supervisord.conf"]
