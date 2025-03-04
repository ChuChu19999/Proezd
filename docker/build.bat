@echo off
docker compose down
docker compose build --no-cache
docker compose push