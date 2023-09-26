FROM python:3.9.18-bookworm

RUN mkdir -p /usr/app/movies_app/app/
RUN mkdir -p /usr/app/movies_app/config/
RUN mkdir -p /usr/app/movies_app/out/data/
RUN mkdir -p /usr/app/movies_app/out/logs/

WORKDIR /usr/app/movies_app

COPY requirements.txt ./

RUN pip3 install --upgrade pip 

RUN pip3 install -r requirements.txt

WORKDIR /usr/app/movies_app/

COPY ./app ./app

COPY ./config ./config

ENTRYPOINT ["python3", "app/main.py"]





