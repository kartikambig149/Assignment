FROM python:3.11-slim
RUN apt-get update && apt-get install -y build-essential libpq-dev curl
WORKDIR /opt/airflow
COPY requirements.txt ./
RUN pip install -r requirements.txt
COPY entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh
ENTRYPOINT ["/entrypoint.sh"]
