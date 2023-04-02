#UBUNTU AS BASE
FROM ubuntu:focal

##UPDATE
RUN apt-get update -y
RUN apt-get install nano
RUN apt-get update -y

##PYTHON
RUN apt-get install -y python3-pip python3-dev
RUN pip3 install --upgrade pip
COPY requirements-container.txt /requirements.txt
RUN pip3 install -r requirements.txt

##JAVA
RUN DEBIAN_FRONTEND=noninteractive TZ=Etc/UTC apt-get -y install tzdata
RUN apt install -y openjdk-8-jdk

##COPY IN FILES
COPY test.py /test.py
CMD python3 /test.py