FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1

WORKDIR /app

# rocksdict needs Rust toolchain for build if wheel not available; use prebuilt when possible
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential curl pkg-config libssl-dev \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt requirements-dev.txt* ./
RUN pip install --no-cache-dir --upgrade pip \
    && pip install --no-cache-dir -r requirements.txt \
    && pip install --no-cache-dir rocksdict==0.3.28

COPY streamforge ./streamforge
COPY main.py ./
COPY infra ./infra

# Production mode must have real RocksDB — fail fast if rocksdict missing
ENV STORAGE_MODE=production
# Verify rocksdict import at build time
RUN python -c "import rocksdict; print(f'rocksdict {rocksdict.__version__} ready')"

CMD ["python", "-m", "streamforge.workers.worker_process"]
