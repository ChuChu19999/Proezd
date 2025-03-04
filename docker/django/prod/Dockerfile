###########
# BUILDER #
###########

# pull official base image
FROM harbor.gd-urengoy.gazprom.ru/docker_hub/python:3.11-slim as builder

# set work directory
WORKDIR /usr/src/app

# set environment variables
ENV PYTHONDONTWRITEBYTECODE 1
ENV PYTHONUNBUFFERED 1

# install dependencies
COPY ./docker/django/root.crt /usr/local/share/ca-certificates/
RUN update-ca-certificates
RUN rm -rf /etc/apt/sources.list && rm -rf /etc/apt/sources.list.d/*
RUN echo "deb [trusted=yes] https://repo.gd-urengoy.gazprom.ru/repository/debian-bookworm/ bookworm main contrib non-free non-free-firmware" > /etc/apt/sources.list

# install dependencies
COPY ../../../backend/requirements.txt .
RUN pip wheel --no-cache-dir --no-deps --wheel-dir /usr/src/app/wheels -r requirements.txt

#########
# FINAL #
#########

# pull official base image
FROM harbor.gd-urengoy.gazprom.ru/docker_hub/python:3.11-slim

# install dependencies
COPY ./docker/django/root.crt /usr/local/share/ca-certificates/
RUN update-ca-certificates
RUN rm -rf /etc/apt/sources.list && rm -rf /etc/apt/sources.list.d/*
RUN echo "deb [trusted=yes] https://repo.gd-urengoy.gazprom.ru/repository/debian-bookworm/ bookworm main contrib non-free non-free-firmware" > /etc/apt/sources.list

# create directory for the app user
RUN mkdir -p /home/app

# create the app user
RUN addgroup --system app && useradd -g app app

# create the appropriate directories
ENV HOME=/home/app
ENV APP_HOME=/home/app/web
RUN mkdir $APP_HOME
RUN mkdir $APP_HOME/backend_static
RUN mkdir $APP_HOME/media
WORKDIR $APP_HOME

COPY --from=builder /usr/src/app/wheels /wheels

RUN pip install --no-cache /wheels/* -i https://repo.gd-urengoy.gazprom.ru/repository/pypi-org-proxy/simple --trusted-host repo.gd-urengoy.gazprom.ru
RUN pip install gunicorn==21.2.0 -i https://repo.gd-urengoy.gazprom.ru/repository/pypi-org-proxy/simple --trusted-host repo.gd-urengoy.gazprom.ru

# copy entrypoint.prod.sh
COPY ./docker/django/prod/entrypoint.prod.sh .
RUN sed -i 's/\r$//g'  $APP_HOME/entrypoint.prod.sh
RUN chmod +x  $APP_HOME/entrypoint.prod.sh

# copy project
COPY ../../../backend $APP_HOME

# chown all the files to the app user
RUN chown -R app:app $APP_HOME

# change to the app user
USER app

# run entrypoint.prod.sh
ENTRYPOINT ["/home/app/web/entrypoint.prod.sh"]