FROM ubuntu:bionic

RUN apt-get update && apt-get install -y \
    python3 python3-pip \
    fonts-liberation libappindicator3-1 libasound2 libatk-bridge2.0-0 \
    libnspr4 libnss3 lsb-release xdg-utils libxss1 libdbus-glib-1-2 \
    curl unzip wget \
    xvfb

RUN python3 -m pip install -U pip
RUN pip3 install elasticsearch requests boto3 pandas pdfminer elasticsearch-dsl
RUN export LC_ALL=C.UTF-8 && \
    export LANG=C.UTF-8

ENV LANG C.UTF-8
ENV LC_ALL C.UTF-8
ENV PYTHONUNBUFFERED=1

# copy the content of the local src directory to the working directory
ENV APP_HOME /usr/src/app/src
WORKDIR /usr/src/app

ADD src /$APP_HOME
RUN mkdir output

# command to run on container start
CMD ["python3", "-m", "src"]
