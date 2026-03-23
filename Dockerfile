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

EXPOSE 8055

CMD ["python", "autohealer.py"]
