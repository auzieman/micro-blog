FROM python:3.12-slim
WORKDIR /app
COPY src/api/requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt
COPY src/api /app/src/api
COPY src/shared /app/src/shared
ENV PYTHONPATH=/app/src/shared
ENV FLASK_APP=src/api/app.py
ENV PYTHONUNBUFFERED=1
EXPOSE 8080
CMD ["python", "/app/src/api/app.py"]

