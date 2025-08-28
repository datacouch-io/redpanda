FROM python:3.11-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY agent_streaming.py .
# DB lives in a writable volume
VOLUME ["/data"]
ENV PYTHONUNBUFFERED=1
CMD ["python", "-u", "agent_streaming.py"]