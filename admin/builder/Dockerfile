FROM amazonlinux:latest

WORKDIR /home
COPY requirements.txt requirements.txt
RUN mkdir layer
RUN yum install -y python3
RUN pip3 install -t layer -r requirements.txt
