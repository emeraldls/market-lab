ARG MARKETLAB_IMAGE=ghcr.io/emeraldls/market-lab-daemon:v0.0.8
FROM ${MARKETLAB_IMAGE}

USER root

COPY requirements.txt /tmp/marketlab-requirements.txt
RUN python3 -m venv /opt/marketlab-python \
    && /opt/marketlab-python/bin/pip install --no-cache-dir \
        -r /tmp/marketlab-requirements.txt \
    && rm /tmp/marketlab-requirements.txt

ENV MLAB_PYTHON=/opt/marketlab-python/bin/python

USER marketlab:marketlab
