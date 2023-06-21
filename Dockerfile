## Builder
ARG RUST_VERSION=1.59.0
FROM rust:${RUST_VERSION}-slim-bullseye as build

# cache mounts below may already exist and owned by root
USER root

RUN apt update && apt install --yes git gcc g++ libssl-dev pkg-config cmake protobuf-compiler && rm -rf /var/lib/apt/lists/*

WORKDIR /ceresdb
COPY . .

RUN make build

## CeresDB
FROM ubuntu:22.10

RUN useradd -m -s /bin/bash ceres

RUN apt update && \
    apt install --yes curl gdb iotop cron vim less net-tools && \
    apt clean

ENV RUST_BACKTRACE 1

COPY --from=build /ceresdb/target/release/ceresdb-server /usr/bin/ceresdb-server
RUN chmod +x /usr/bin/ceresdb-server

COPY ./docker/entrypoint.sh /entrypoint.sh
COPY ./docs/minimal.toml /etc/ceresdb/ceresdb.toml

COPY ./docker/tini /tini
RUN chmod +x /tini

ARG USER ceres

ENTRYPOINT ["/entrypoint.sh" ]

