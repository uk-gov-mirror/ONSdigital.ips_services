#!/usr/bin/env bash

export DB_SERVER=127.0.0.1
export DB_USER_NAME=ips
export DB_PASSWORD=ips
export DB_NAME=ips

waitress-serve --listen=*:5000 --threads=4 ips.app:app
