FROM python:3.12-slim
WORKDIR /app
COPY src/ui/requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt
COPY src/ui /app/src/ui
COPY src/shared /app/src/shared
ENV PYTHONPATH=/app/src/shared
ENV PYTHONUNBUFFERED=1
EXPOSE 8080
CMD ["python", "/app/src/ui/app.py"]

