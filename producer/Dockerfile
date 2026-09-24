FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY produce.py .

HEALTHCHECK --interval=10s --timeout=3s --retries=5 \
    CMD python -c "import socket; s=socket.create_connection(('kafka', 9092), 3); s.close()" || exit 1

CMD ["python", "produce.py"]