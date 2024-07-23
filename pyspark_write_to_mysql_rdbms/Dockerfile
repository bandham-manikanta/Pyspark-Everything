
FROM apache/airflow:2.9.3

WORKDIR /

# Copy project files
COPY . /

RUN pip install -r requirements.txt

RUN chown -R airflow:airflow /opt/airflow/logs
RUN chmod -R 755 /opt/airflow/logs

# Switch to airflow user
USER airflow