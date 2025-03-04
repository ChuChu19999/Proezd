#!/bin/sh

if [ "$DATABASE" = "postgres" ]
then
    echo "Waiting for postgres..."

    echo "PostgreSQL started"
fi

python manage.py collectstatic --noinput

exec "$@"