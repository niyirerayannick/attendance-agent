# Standalone outbound worker image for local Coolify on Proxmox.
FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    AGENT_DATA_DIR=/data

WORKDIR /app

RUN groupadd --system attendance && useradd --system --gid attendance --home-dir /app --no-create-home attendance \
    && install -d --owner=attendance --group=attendance /app /data

COPY requirements.txt ./requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

COPY . ./
RUN chown -R attendance:attendance /app /data

USER attendance

# This is a liveness check. `health` reports temporary device/cloud outages but
# exits successfully, so Docker does not restart a healthy queue worker merely
# because the terminal or Starlink is briefly unavailable.
HEALTHCHECK --interval=60s --timeout=45s --start-period=90s --retries=3 \
    CMD ["python", "agent.py", "health"]

CMD ["python", "agent.py", "run"]
