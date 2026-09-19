FROM python:3.12-slim

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONPATH=/app \
    API_PORT=8080

WORKDIR /app

COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

RUN chmod +x /app/scripts/entrypoint.sh /app/scripts/migrate.sh /app/scripts/healthcheck.sh

EXPOSE 8080

HEALTHCHECK --interval=5s --timeout=3s --start-period=10s --retries=20 \
    CMD /app/scripts/healthcheck.sh

ENTRYPOINT ["/app/scripts/entrypoint.sh"]
CMD ["python", "run.py"]
