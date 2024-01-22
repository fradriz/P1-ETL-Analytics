# P1-ETL-Analytics

## Pre-requisites
* Docker / Docker Compose
* [Python Virtualenv & Packages](environment/virtual_environment.md)

## Steps
### Start the virtualenv with the packages installed
Run: `source p1env/bin/activate` (where `p1env` is the name of the virtualenv)

### Docker compose
From the terminal, go to the [_environment_](environment) folder and run:
```shell
$ docker compose up -d
[+] Building 0.0s (0/0)                                                                                                                                                   docker:desktop-linux
[+] Running 4/4
 ✔ Network environment_default         Created                                                                                                                                            0.1s
 ✔ Container environment-pgadmin-1     Started                                                                                                                                            0.1s
 ✔ Container environment-pgdatabase-1  Started                                                                                                                                            0.1s
 ✔ Container environment-metabase-1    Started                                                                                                                                          0.0s
```
We should see three services started: Metabase, Postgres and Pgadmin UI.
```
(p1env) ➜  environment git:(main) ✗ docker ps
CONTAINER ID   IMAGE               COMMAND                  CREATED          STATUS          PORTS                           NAMES
babc029c934c   metabase/metabase   "/app/run_metabase.sh"   12 seconds ago   Up 11 seconds   0.0.0.0:3000->3000/tcp          environment-metabase-1
d1095a325a66   postgres:13         "docker-entrypoint.s…"   12 seconds ago   Up 11 seconds   0.0.0.0:5432->5432/tcp          environment-pgdatabase-1
c52637a12467   dpage/pgadmin4      "/entrypoint.sh"         12 seconds ago   Up 11 seconds   443/tcp, 0.0.0.0:8080->80/tcp   environment-pgadmin-1
```

### Connect to the services
After the build and installations, we should have __PGADMIN__ in the port 8080 and Metabase in the port 3000.

Open a browser and:
* Open PGADMIN: http://localhost:8080 - admin@admin.com/root as user/passwd (we still need to add a new server)
  * Add new server:
    * General > Name: __MyDB__
    * Connections > Host name/address: __pgdatabase__ 
    * Connections > Maintenance database: __my_data_db__ (name is in the docker file `POSTGRES_DB`)
    * Connections > Username = __root__ / Password = __root__
* Open Metabase: http://localhost:3000/setup
  * Follow the instructions and create and remember the passwd: __root1234__ (remember it!)
  * Connect to _PostgreSQL_
  * Display Name: __MyData__
  * Host: __pgdatabase__
  * Port: __5432__
  * Database name: __my_data_db__
  * Username: __root__ (docker file `POSTGRES_USER`)
  * Password: __root__ (docker file `POSTGRES_PASSWORD`)

From now own, we have the connections between the DB and the Visualization tool.

## Dataset
Using the [World Development Data from the WorldBank](https://datacatalog.worldbank.org/search/dataset/0037712/World-Development-Indicators)

Check the [API page too](https://datahelpdesk.worldbank.org/knowledgebase/articles/889386-developer-information-overview)
```
The World Development Indicators (WDI) is the primary World Bank collection of development indicators, compiled from officially-recognized international sources. 
It presents the most current and accurate global development data available, and includes national, regional and global estimates.
```

- OBS I: I have downloaded, unzip and compress again some files with bzip2: `bzip2 WDIData.csv`
- OBS II: Spark can split the compress data with bzip2 (not with zip or gzip)

### Close docker compose
After finish, shut down the dockers containers by running:
```shell
$ docker compose down
[+] Running 4/4
 ✔ Container environment-metabase-1    Removed                                                                                                                                            0.8s
 ✔ Container environment-pgadmin-1     Removed                                                                                                                                            1.3s
 ✔ Container environment-pgdatabase-1  Removed                                                                                                                                            0.2s
 ✔ Network environment_default         Removed
```

If the volumes folder in the [docker-compose](environment/docker-compose.yaml) file is preserved, then the data and relations created previously will be there for the next session. 
Meaning: everything is preserved despite shutting down the containers.

# PENDING
* [Postgres UPSERT](https://www.postgresqltutorial.com/postgresql-tutorial/postgresql-upsert/)

Link: https://spark.apache.org/docs/latest/sql-data-sources-jdbc.html
