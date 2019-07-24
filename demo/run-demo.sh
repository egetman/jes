#!/usr/bin/env bash

docker run --rm --name postgres-store -e POSTGRES_PASSWORD=postgres -d -p 54320:5432 postgres:9.6
