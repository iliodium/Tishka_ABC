import requests, time
from datetime import timedelta, datetime, date



url = 'https://statistics-api.wildberries.ru/api/v1/supplier/stocks?dateFrom={}'
url = url.format(datetime.strftime(datetime.now(), "%Y-%m-%d"))

headers_temp_wb = lambda token: {
    "Content-Type": "application/json",
    "Authorization": token,
}
TOKEN_WB='eyJhbGciOiJFUzI1NiIsImtpZCI6IjIwMjMxMjI1djEiLCJ0eXAiOiJKV1QifQ.eyJlbnQiOjEsImV4cCI6MTcyMjQ2NDg1MCwiaWQiOiJkMTM1NDU2OS04M2FmLTQ5NTktODdmMC0xNTdjNDAwNzQ0M2EiLCJpaWQiOjI4NDI5NjgzLCJvaWQiOjI2NTY2MSwicyI6MzYsInNpZCI6ImI0OWM4ODI5LTUzZWEtNGViMi05ZDc5LWUyZTg4M2EyYjY0OCIsInQiOmZhbHNlLCJ1aWQiOjI4NDI5NjgzfQ.Yn8-PtLz9mzfJMWFcnogFcGpdzy767uooer-fIkK--TxxiBxBNZv66YtL6lmYxkviPYnPBWx_7i2P5YBISkfFA'
HEADERS = headers_temp_wb(TOKEN_WB)

TIME_SLEEP = 1

def make_request_to_wb(url: str, method: str, json: dict = dict()):
    method = {
        'GET': requests.get,
        'POST': requests.post,
    }[method]

    response = method(url, headers=HEADERS, json=json)
    if response.status_code not in (200, 429, 500):
        raise Exception(response, response.content)
    else:
        while response.status_code in (429, 500):
            if response.status_code not in (429, 500):
                raise Exception(response, response.content)
            response = method(url, headers=HEADERS, json=json)
            time.sleep(TIME_SLEEP)

    return response

print(url)
r =make_request_to_wb(url=url, method='GET')
print(r.content)