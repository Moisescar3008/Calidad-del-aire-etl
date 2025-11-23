FROM apache/airflow:2.10.2

# Instalar dependencias adicionales (opcional)
USER root
RUN apt-get update && apt-get install -y \
    nano vim --no-install-recommends \
    && apt-get clean

USER airflow

# Para instalar librerías de Python
COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt
