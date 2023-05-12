# Russian Flights 🛫🛬 Data Graph ETL Pipeline

<img src="./assets/datastaxgse0.png" width="900" height="180">
<img src="./assets/tp-crew.png" width="900" height="500">

## Description

A graph Extract Transform Load (ETL) pipeline that moves relational data about flight bookings in Russia from a PostgreSQL database into a Datastax Enterprise (DSE) graph database. The goal of the project is to demonstrate migrating data from a relational database management system RDBMS into a graph in order to leverage the power and benefits of a graph database as well as graph analytics.

## ERD Model

![ERD Schema](./assets/bookings_erd.png)

## Graph Schema language (GSL)

![graph Schema](./assets/flights_schema_model.png)

## Required Installations

- java-jdk 1.8
- maven
- docker desktop

## Project stack

* Docker compose: for container orchestration
    - postgreSQL database container
    - datastax enterprise server/ cluster (spark, cassandra, graph, dse fs)
    - datastax studo
* Scala maven project
    - FlightGraphLoaderV: writes graph vertices
    - FlightGraphLoaderE: writes graph edges

## How to run

Use make file: run "make" on command line while in root of project director /filights-graphETL/ <br>
Alternatively: run specific commands from make file.

- dse_compose_up ➡️ start dse server
- postgres_compose_up ➡️ start postgres server
- maven_build ➡️ build project to create jar
- copy_files ➡️ copy schema and jar files to dse cluster
- create_schema ➡️ create flights schema
- build_vertices ➡️ builds flights vertices
- build_edges ➡️ builds flights edges

### Datasource reference

[postgrespro](https://postgrespro.com/docs/postgrespro/10/demodb-bookings)