## Builder
ARG GOLANG_VERSION=1.21.3
FROM golang:${GOLANG_VERSION}-bullseye as build

# cache mounts below may already exist and owned by root
USER root

RUN apt update && apt install --yes gcc g++ libssl-dev pkg-config cmake && rm -rf /var/lib/apt/lists/*

COPY . /ceresmeta
WORKDIR /ceresmeta

RUN make build

## CeresMeta
FROM ubuntu:20.04

RUN useradd -m -s /bin/bash ceres

RUN apt update && \
    apt install --yes curl gdb iotop cron vim less net-tools && \
    apt clean

COPY --from=build /ceresmeta/bin/ceresmeta-server /usr/bin/ceresmeta-server
RUN chmod +x /usr/bin/ceresmeta-server

COPY ./docker/entrypoint.sh /entrypoint.sh
COPY ./config/example-standalone.toml /etc/ceresmeta/ceresmeta.toml

COPY ./docker/tini /tini
RUN chmod +x /tini

ARG USER ceres

ENTRYPOINT ["/tini", "--", "/entrypoint.sh"]
