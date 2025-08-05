FROM python:3.10-slim

WORKDIR /app

RUN pip install aioredis
RUN pip install redis
COPY python-analyser.py .

CMD ["python", "python-analyser.py"]


