#!/bin/sh

docker compose -f docker-compose-linux.yml down
docker compose -f docker-compose-linux.yml pull
docker compose -f docker-compose-linux.yml up -d
docker image prune --force

exec "$@"