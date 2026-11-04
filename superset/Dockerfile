FROM apache/superset:6.1.0

USER root
RUN if command -v uv >/dev/null 2>&1; then \
      uv pip install --python /app/.venv/bin/python clickhouse-connect; \
    else \
      /app/.venv/bin/python -m pip install clickhouse-connect; \
    fi
USER superset