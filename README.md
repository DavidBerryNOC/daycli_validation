Instructions 
============

1) Clone repository:
```
git clone https://github.com/DavidBerryNOC/daycli_validation.git
```
3) From repository directory, create new docker image (note use of --no-cache
as issues with caching repository).
```
docker build --no-cache -t dayclim .
```
3) Run image using docker desktop and set port to 5000

``
docker run -d -p 5000:5000 dayclim
``

4) Visit:

    http://localhost:5000/upload

and upload uncompressed BUFR file

5) If successful, decoded data from BUFR file should be shown as webpage,
including downloadable CSV file. 