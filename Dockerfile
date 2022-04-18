FROM python:3.9

ENV HOME=/code

RUN apt-get update -y && apt-get upgrade -y && apt-get install postgresql-client -y
RUN pip install --upgrade pip
RUN addgroup web && adduser web --home $HOME --ingroup web
RUN mkdir /var/log/elk_service/ && chown -R web:web /var/log/elk_service

WORKDIR $HOME

COPY ./requirements.txt .
RUN pip install -r requirements.txt
COPY ./postgres_to_es .

USER web

ENTRYPOINT ["python", "./load_data_to_es.py"]
