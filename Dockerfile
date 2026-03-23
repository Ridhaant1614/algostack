FROM python:3.13-slim

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1
ENV PIP_NO_CACHE_DIR=1

WORKDIR /app

# Minimal runtime tools used by AlgoStack
RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
    curl \
    openssh-client \
    tzdata \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt /app/requirements.txt
RUN pip install --upgrade pip && pip install -r /app/requirements.txt

COPY . /app

# Always-on host mode: no third-party tunnel dependency in container
ENV DISABLE_CLOUDFLARE=1
ENV DISABLE_PYNGROK=1
ENV TUNNEL_STABLE_MODE=1
# Reduce BLAS/thread RAM on small containers (Render free tier)
ENV OMP_NUM_THREADS=1
ENV OPENBLAS_NUM_THREADS=1
ENV MKL_NUM_THREADS=1

EXPOSE 8055

# Default lite on cloud; override with AUTOHEALER_PROFILE=render-full + more RAM
CMD ["python", "-X", "utf8", "-u", "autohealer.py", "--profile", "render-lite"]
