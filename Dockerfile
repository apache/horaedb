## Builder
ARG RUST_VERSION=1.59.0
FROM rust:${RUST_VERSION}-slim-bullseye as build

# cache mounts below may already exist and owned by root
USER root

RUN apt update && apt install --yes git gcc g++ libssl-dev pkg-config cmake protobuf-compiler && rm -rf /var/lib/apt/lists/*

COPY . /horaedb
WORKDIR /horaedb

RUN make build

## CeresDB
FROM ubuntu:20.04

RUN useradd -m -s /bin/bash horae

RUN apt update && \
    apt install --yes curl gdb iotop cron vim less net-tools && \
    apt clean

ENV RUST_BACKTRACE 1

COPY --from=build /horaedb/target/release/horaedb-server /usr/bin/horaedb-server
RUN chmod +x /usr/bin/horaedb-server

COPY ./docker/entrypoint.sh /entrypoint.sh
COPY ./docs/minimal.toml /etc/horaedb/horaedb.toml

COPY ./docker/tini /tini
RUN chmod +x /tini

ARG USER horae

ENTRYPOINT ["/tini", "--", "/entrypoint.sh"]
