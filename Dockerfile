# Docker python image
FROM python:3
# locationn to put code and data
WORKDIR /usr/src/app
# update
RUN apt-get update
RUN apt-get install -y vim
# get python code from github
RUN git clone https://github.com/DavidBerryNOC/daycli_validation.git .
# install requirements
RUN pip install -r requirements.txt
# Update table
RUN curl https://raw.githubusercontent.com/wmo-im/BUFR4/issue51/txt/BUFRCREX_CodeFlag_en.txt > ./tables/BUFRCREX_CodeFlag_en.txt
RUN curl https://raw.githubusercontent.com/wmo-im/BUFR4/issue51/txt/BUFRCREX_TableB_en.txt > ./tables/BUFRCREX_TableB_en.txt
RUN curl https://raw.githubusercontent.com/wmo-im/BUFR4/issue51/txt/BUFR_TableC_en.txt > ./tables/BUFR_TableC_en.txt
RUN curl https://raw.githubusercontent.com/wmo-im/BUFR4/issue51/txt/BUFR_TableD_en.txt > ./tables/BUFR_TableD_en.txt
# expose port 5000 for flask
EXPOSE 5000
ENTRYPOINT ["python"]
CMD ["app.py"]
