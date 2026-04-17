FROM python:3.12-slim
WORKDIR /app
COPY src/projection/requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt
COPY src/projection /app/src/projection
COPY src/shared /app/src/shared
ENV PYTHONPATH=/app/src/shared
ENV PYTHONUNBUFFERED=1
CMD ["python", "/app/src/projection/projection.py"]

