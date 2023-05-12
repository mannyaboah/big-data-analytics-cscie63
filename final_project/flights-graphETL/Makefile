#----------------------------------------------------------------------------#
# 						Make file for flights-graphETL						 #
#----------------------------------------------------------------------------# 

dse_home = "/opt/dse"
dse_container_name = "dse"
postgres_container_name = "postgres"
uber_jar = "flights-graphETL-1.0-SNAPSHOT-with-dependencies.jar"

# build and run the entire ETL process
all: dse_compose_up postgres_compose_up maven_build copy_files postgres_copy_files postgres_build create_schema build_vertices build_edges

# bring up DSE server
dse_compose_up:
	@docker-compose -f ./docker/dse/docker-compose.yaml up -d

# bring up postgres database server
postgres_compose_up:
	@docker-compose -f ./docker/postgres/docker-compose.yaml up -d

# copy database build scripts to postgres container
postgres_copy_files:
	@docker cp ../demo-small-en-20170815.sql $(postgres_container_name):/

# build the database
postgres_build:
	@docker exec -it $(postgres_container_name) psql -U postgres -f demo-small-en-20170815.sql

# build the uber jar
maven_build:
	@mvn clean package -DskipTests

# bring down the DSE server
compose_down:
	@docker-compose down

# bring down the postgres server
postgres_compose_down:
	@docker-compose -f ../docker-compose-postgres.yaml down

# copy files to DSE container
copy_files:
	@docker cp ./src/main/resources/schema/flights_schema.groovy $(dse_container_name):$(dse_home)
	@docker cp ./target/flights-graphETL-1.0-SNAPSHOT-with-dependencies.jar $(dse_container_name):$(dse_home)

# remove uber jar from DSE container
remove_jar:
	@docker exec -it $(dse_container_name) rm $(uber_jar)

# create schema in DSE graph
create_schema:
	@docker exec -it $(dse_container_name) dse gremlin-console -e flights_schema.groovy

# build flights vertices in DSE graph
build_vertices:
	@docker exec -it $(dse_container_name) dse spark-submit --class edu.harvard.jobs.FlightGraphLoaderV --master local[2] $(uber_jar)

# build flights edges in DSE graph
build_edges:
	@docker exec -it $(dse_container_name) dse spark-submit --class edu.harvard.jobs.FlightGraphLoaderE --master local[2] $(uber_jar)

