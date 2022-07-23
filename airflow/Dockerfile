FROM apache/airflow:2.3.3
COPY lib.txt /lib.txt
RUN pip install --user --upgrade pip
RUN pip install --no-cache-dir --user -r /lib.txt