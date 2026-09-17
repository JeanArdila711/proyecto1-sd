# Una sola imagen para todos los roles de DFSha: DataNode, ControlNode, shell y tests.
# El rol lo decide el `command` de cada servicio en docker-compose.yml.
FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1 \
    PYTHONPATH=/app

WORKDIR /app

# Dependencias primero: esta capa solo se reconstruye si cambia requirements.txt
COPY requirements.txt .
RUN pip install -r requirements.txt

COPY proto/ proto/
COPY scripts/ scripts/
COPY dfsha/ dfsha/
COPY tests/ tests/
COPY conftest.py .

# El código gRPC no se versiona: se genera dentro de la imagen
RUN python scripts/generate_proto.py

# Sin root: los volúmenes se crean con este dueño al montarse por primera vez
RUN useradd --create-home --uid 1000 dfsha \
    && mkdir -p /data /raft /intercambio \
    && chown -R dfsha:dfsha /app /data /raft /intercambio
USER dfsha
