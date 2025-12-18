FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy source code
COPY . .

# User non-root (Security requirement)
RUN useradd -m appuser && chown -R appuser /app
USER appuser

# Jalankan app dari folder app module
CMD ["python", "main.py"]