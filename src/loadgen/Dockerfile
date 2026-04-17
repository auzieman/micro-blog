FROM python:3.12-slim
WORKDIR /app
COPY src/loadgen/requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt
COPY src/loadgen /app/src/loadgen
COPY src/shared /app/src/shared
ENV PYTHONPATH=/app/src/shared
ENV PYTHONUNBUFFERED=1
CMD ["python", "/app/src/loadgen/loadgen.py"]

