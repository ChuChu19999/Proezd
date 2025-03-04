# Настройка Nginx
FROM harbor.gd-urengoy.gazprom.ru/docker_hub/nginx:1.25.2-alpine

RUN rm -rf /etc/localtime
RUN ln -s /usr/share/zoneinfo/Asia/Yekaterinburg /etc/localtime
RUN echo "Asia/Yekaterinburg" > /etc/timezone

# nginx conf
ADD docker/nginx/dev/django.conf /etc/nginx/sites-enabled/default
ADD docker/nginx/nginx.conf /etc/nginx/nginx.conf

COPY docker/nginx/custom_50x.html /usr/share/nginx/html/custom_50x.html

# Настройка SSL сертификатов
COPY ./backend/root.crt /usr/local/share/ca-certificates/root.crt
COPY ./backend/root.crt /usr/share/ca-certificates/root.crt
RUN update-ca-certificates

EXPOSE 80 443