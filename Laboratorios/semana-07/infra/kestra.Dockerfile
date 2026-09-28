FROM kestra/kestra:v1.3.37

USER root

# venv del pipeline, separado del venv interno de Kestra (/app/.venv)
COPY --from=ghcr.io/astral-sh/uv:0.11.7 /uv /usr/local/bin/uv
COPY requirements.txt /tmp/pipeline-requirements.txt
RUN uv venv --python /usr/bin/python3.12 /opt/pipeline-venv \
 && uv pip install --python /opt/pipeline-venv/bin/python -r /tmp/pipeline-requirements.txt
