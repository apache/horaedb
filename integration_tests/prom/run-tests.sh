#!/usr/bin/env bash

VERSION=prometheus-2.43.0.linux-amd64
wget "https://github.com/prometheus/prometheus/releases/download/v2.43.0/${VERSION}.tar.gz"

tar xvf prometheus*.tar.gz
nohup ./${VERSION}/prometheus --config.file ./prometheus.yml &
sleep 5

python ./remote-query.py
