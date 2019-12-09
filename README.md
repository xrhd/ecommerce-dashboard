# Demo on Ecommerce Dashboard 
![schema](doc/schema.jpg)

## dependeies:
 - [docker](https://www.docker.com/)
 - [python](https://www.python.org/)
 - `sudo apt-get install python3-psycopg2`
 - `pip install -r requirements.txt`

## dataset
 - [Amazon product co-purchasing network metadata](https://snap.stanford.edu/data/amazon-meta.html)

## postgres docker setup [(link)](https://hackernoon.com/dont-install-postgres-docker-pull-postgres-bee20e200198)
 - `mkdir -p $HOME/docker/volumes/postgres`

 - `docker run --rm   --name pg-docker -e POSTGRES_PASSWORD=docker -d -p 5432:5432 -v $HOME/docker/volumes/postgres:/var/lib/postgresql/data  postgres`

## connect to pg docker [(link)](http://www.postgresqltutorial.com/postgresql-python/connect/)
 
 - edit credentials/database.ini

 ## How to run
 - start posgres docker
 - edit your credentials
 - finaly run: `jupyter lab main.ipynb` 