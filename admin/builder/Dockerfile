FROM amazonlinux:latest

WORKDIR /home
COPY . .
RUN yum install -y python3
RUN python3 -m pip install --upgrade pip
RUN pip3 install -r requirements.txt
