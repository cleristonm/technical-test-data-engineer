FROM apache/airflow:2.7.1

USER root
RUN apt-get update && apt-get install -y --no-install-recommends build-essential && apt-get clean && rm -rf /var/lib/apt/lists/*

COPY scripts/entrypoint.sh /tmp/entrypoint.sh
# Fix line endings and make script executable (works in both Linux and Windows)
RUN sed -i 's/\r$//' /tmp/entrypoint.sh && chmod +x /tmp/entrypoint.sh
USER airflow

COPY requirements.txt /opt/airflow/requirements.txt

RUN pip install --no-cache-dir -r /opt/airflow/requirements.txt

ENTRYPOINT ["/bin/bash", "/tmp/entrypoint.sh"]