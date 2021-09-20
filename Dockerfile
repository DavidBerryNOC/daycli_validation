# Docker python image
FROM python:3
# locationn to put code and data
WORKDIR /usr/src/app
# update
RUN apt-get update
RUN apt-get install -y vim
#RUN git clone https://github.com/wmo-im/BUFR4.git -b issue51
# once working convert go git repo and clone from there rather than copying
ADD requirements.txt .
ADD app.py .
ADD bufr_message.py .
RUN mkdir templates
RUN mkdir tables
ADD ./templates/upload.html ./templates/
ADD ./tables/* ./tables
RUN pip install -r requirements.txt
EXPOSE 5000
ENV FLASK_APP=app
#RUN flask run