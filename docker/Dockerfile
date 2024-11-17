FROM apache/airflow:2.7.1

USER root
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    build-essential \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

COPY scripts/entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

USER airflow

COPY requirements.txt /opt/airflow/requirements.txt
RUN pip install --no-cache-dir -r /opt/airflow/requirements.txt
CMD ["uvicorn", "src.moovitamix_fastapi.main:app", "--host", "0.0.0.0", "--port", "8000", "--reload"]


ENTRYPOINT ["/entrypoint.sh"]
