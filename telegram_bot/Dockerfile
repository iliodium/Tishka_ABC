FROM python:3.12.1-alpine3.19

COPY ./requirements.txt ./requirements.txt
COPY main.py .
COPY ./keyboards.py .

RUN pip install --no-cache-dir --upgrade -r ./requirements.txt

CMD ["python", "main.py"]