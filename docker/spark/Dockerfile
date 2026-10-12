FROM python:3.11-slim-bookworm
RUN apt-get update && apt-get install -y --no-install-recommends openjdk-17-jre-headless bash libgomp1 tini iproute2 && rm -rf /var/lib/apt/lists/*
RUN python -m venv /opt/flight-spark/venv
COPY scripts/linux/requirements.txt /tmp/requirements.txt
RUN /opt/flight-spark/venv/bin/pip install --no-cache-dir -r /tmp/requirements.txt
ENV PATH="/opt/flight-spark/venv/bin:${PATH}" \
    SPARK_HOME=/opt/flight-spark/venv/lib/python3.11/site-packages/pyspark \
    PYSPARK_PYTHON=/opt/flight-spark/venv/bin/python \
    PYSPARK_DRIVER_PYTHON=/opt/flight-spark/venv/bin/python \
    SPARK_EXTRA_PACKAGES=com.microsoft.azure:synapseml_2.12:1.0.15 \
    SPARK_EXTRA_REPOSITORIES=https://mmlspark.blob.core.windows.net/maven \
    SPARK_IVY_SETTINGS=/opt/flight-spark/ivysettings.xml
COPY scripts/linux/ivysettings.xml /opt/flight-spark/ivysettings.xml
COPY docker/spark/entrypoint.sh /opt/flight-spark/entrypoint.sh
WORKDIR /workspace
ENTRYPOINT ["/usr/bin/tini", "--", "bash", "/opt/flight-spark/entrypoint.sh"]
