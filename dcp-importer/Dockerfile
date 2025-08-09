FROM python:3.11-slim
WORKDIR /app
COPY requirements.txt ./
ENV PIP_DISABLE_PIP_VERSION_CHECK=1 PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1
RUN python -m pip install --upgrade pip setuptools wheel &&     pip install --no-cache-dir --prefer-binary -r requirements.txt
COPY main.py ./
EXPOSE 9000
CMD ["sh","-c","uvicorn main:app --host 0.0.0.0 --port ${PORT:-9000}"]
