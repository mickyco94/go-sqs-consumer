FROM localstack/localstack-full:0.12.7

ENV HOSTNAME="localstack"

RUN apk update && apk add dos2unix
RUN apk add figlet # needed for ASCII art ^^
RUN pip install awscli-local

WORKDIR /opt/code/localstack

COPY provision.sh .

RUN dos2unix provision.sh
RUN chmod +x provision.sh
RUN mkdir /docker-entrypoint-initaws.d && ln -s /opt/code/localstack/provision.sh /docker-entrypoint-initaws.d/provision.sh

HEALTHCHECK --interval=10s --timeout=60s --retries=60 --start-period=60s CMD ./provision.sh