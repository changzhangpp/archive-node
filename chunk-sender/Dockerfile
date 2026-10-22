FROM python:3.11-slim

WORKDIR /app

COPY chunk_sender.py chunk_sender_service.py ./

ENV PYTHONUNBUFFERED=1 \
    SERVICE_HOST=0.0.0.0 \
    SERVICE_PORT=8089

EXPOSE 8089

HEALTHCHECK --interval=30s --timeout=5s --retries=3 \
    CMD python3 -c "import urllib.request; urllib.request.urlopen('http://127.0.0.1:8089/health', timeout=3)"

CMD ["python3", "chunk_sender_service.py"]
