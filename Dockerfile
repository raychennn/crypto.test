FROM python:3.9-slim

WORKDIR /app

# 防止 Python 產生 .pyc 檔案
ENV PYTHONDONTWRITEBYTECODE 1
# 確保 console output 即時顯示
ENV PYTHONUNBUFFERED 1

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

CMD ["python", "main.py"]
