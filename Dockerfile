FROM ghcr.io/astral-sh/uv:alpine
RUN apk add zstd zstd-dev py3-zstandard

WORKDIR /app
COPY pyproject.toml uv.lock /app
RUN uv sync --locked --no-install-project
COPY . /app
RUN uv sync --locked
CMD ["uv", "run", "fastapi", "run", "web.py", "--port", "80"]