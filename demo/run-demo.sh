#!/usr/bin/env bash
# start Postgres on 54320 port. --rm for remove container after stop
echo Starting Postgres...
docker run --rm --name postgres-store -e POSTGRES_PASSWORD=postgres -d -p 54320:5432 postgres:9.6
echo Postgres started on 54320 port, user/password postgres/postgres

# build apps
echo Building apps
mvn -f write-part/pom.xml clean package
mvn -f read-part/pom.xml clean package
echo Complete

# run apps
echo Runnins apps. Output available in 'nohup.out'
nohup java -jar write-part/target/write-part-1.0.jar > nohup.write-part.out 2>&1 &

nohup java -jar read-part/target/read-part-1.0.jar > nohup.read-part.out 2>&1 &
echo Applications started. Check out http://localhost:8080/stock