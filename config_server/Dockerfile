FROM python:3.12.1-alpine3.19

COPY ./requirements.txt ./requirements.txt
COPY configs configs
COPY config_server_rabbitmq.py .

RUN pip install --no-cache-dir --upgrade -r ./requirements.txt

CMD ["python", "config_server_rabbitmq.py"]