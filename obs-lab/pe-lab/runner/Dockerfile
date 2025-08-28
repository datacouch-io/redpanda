FROM python:3.11-slim
WORKDIR /app
ENV PYTHONUNBUFFERED=1

# install deps
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# copy your runner script
COPY prompt_runner.py .

CMD ["python", "-u", "prompt_runner.py"]