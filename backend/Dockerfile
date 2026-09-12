FROM python:3.13-slim
ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1 AIDA_DATA_DIR=/app/data
WORKDIR /app
COPY backend/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt && useradd --create-home aida
COPY --chown=aida:aida backend/api ./backend/api
COPY --chown=aida:aida backend/core ./backend/core
COPY --chown=aida:aida backend/main.py ./backend/main.py
COPY --chown=aida:aida fixtures ./fixtures
RUN mkdir /app/data && chown aida:aida /app/data
USER aida
EXPOSE 8000
HEALTHCHECK --interval=30s --timeout=5s --start-period=15s CMD python -c "import urllib.request; urllib.request.urlopen('http://127.0.0.1:8000/api/v1/health', timeout=3)"
CMD ["uvicorn", "backend.main:app", "--host", "0.0.0.0", "--port", "8000", "--no-access-log"]
