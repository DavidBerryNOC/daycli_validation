# Docker python image
FROM python:3
# location to put code and data
WORKDIR /usr/src/app
# update
RUN apt-get update
RUN apt-get install -y vim
RUN apt-get install -y bash
# RUN git clone https://github.com/wmo-im/BUFR4.git -b issue51
# get python code from github
RUN git clone https://github.com/david-i-berry/daycli_validation.git .
# install requirements
RUN pip install -r requirements.txt
# now get tables from repo
# expose port 5000 for flask
EXPOSE 5000
ENTRYPOINT ["python"]
CMD ["app.py"]
