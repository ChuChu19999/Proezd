#!/bin/sh

export DOCKER_BUILDKIT=1
export COMPOSE_DOCKER_CLI_BUILD=1

docker compose build --no-cache --progress=plain > build_log.txt 2>&1
docker compose push

exec "$@"