FROM python:3.12-slim
WORKDIR /app
RUN pip install --no-cache-dir confluent-kafka==2.6.1 pyarrow==18.1.0 boto3==1.35.80
COPY archiver.py archive_status.py ./
CMD ["python", "-u", "archiver.py"]