FROM python:3.12.1-alpine3.19

COPY ./requirements.txt ./requirements.txt
COPY services_server_rabbitmq.py .

RUN pip install --no-cache-dir --upgrade -r ./requirements.txt

CMD ["python", "services_server_rabbitmq.py"]