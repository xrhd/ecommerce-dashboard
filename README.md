# Demo on Ecommerce Dashboard 
![schema](doc/schema.jpg)

## dependenies:
 - `sudo apt-get install python3-psycopg2`

## dataset
 - input dataset [(link)](https://snap.stanford.edu/data/amazon-meta.html)

## postgres docker setup [(link)](https://hackernoon.com/dont-install-postgres-docker-pull-postgres-bee20e200198)
 - `mkdir -p $HOME/docker/volumes/postgres`

 - `docker run --rm   --name pg-docker -e POSTGRES_PASSWORD=docker -d -p 5432:5432 -v $HOME/docker/volumes/postgres:/var/lib/postgresql/data  postgres`

## connect to pg docker [(link)](http://www.postgresqltutorial.com/postgresql-python/connect/)