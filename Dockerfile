ARG RUST_VERSION=1.59.0
FROM rust:${RUST_VERSION}-slim-bullseye as build

# cache mounts below may already exist and owned by root
USER root

RUN apt update && apt install --yes gcc g++ libssl-dev pkg-config cmake && rm -rf /var/lib/apt/lists/*

# Build ceresdb
COPY . /ceresdb
WORKDIR /ceresdb

RUN make build

FROM ubuntu:20.04
# create admin user
ARG USER=admin
ARG PASS="1q2w3s"
RUN useradd -m -s /bin/bash $USER && echo "$USER:$PASS" | chpasswd

COPY --from=build /ceresdb/target/release/ceresdb-server /usr/bin/ceresdb-server

RUN apt update && apt install --yes curl gdb iotop cron supervisor python vim less net-tools

ENV RUST_BACKTRACE 1

COPY ./docker/entrypoint.py /entrypoint.py
COPY ./docker/supervisor/supervisord.conf /etc/supervisor/supervisord.conf
COPY ./docker/supervisor/conf.d /etc/supervisor/conf.d/
COPY ./configs/ceresdb.toml /usr/bin/

RUN mkdir -p /etc/ceresdb
COPY ./configs/ceresdb.toml /etc/ceresdb/

RUN chmod +x /usr/bin/ceresdb-server

COPY ./docker/tini /tini
RUN chmod +x /tini
ENTRYPOINT ["/tini", "--", "/entrypoint.py"]
