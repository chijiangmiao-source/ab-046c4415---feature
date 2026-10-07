FROM python:3.11-slim

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    IMAGING_DATA_DIR=/data

WORKDIR /srv

COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

RUN mkdir -p /data
EXPOSE 8080

# 默认启动 API 服务；verify 单次服务在 Compose 中覆盖 command。
CMD ["python", "-m", "app.server"]
