FROM python:3.11-slim
WORKDIR /app
ENV PYTHONUNBUFFERED=1
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt \
    && adduser --disabled-password --gecos "" app \
    && mkdir -p /data && chown -R app:app /data
COPY agent_service.py .
USER app
VOLUME ["/data"]
EXPOSE 8088
CMD ["python", "-u", "agent_service.py"]