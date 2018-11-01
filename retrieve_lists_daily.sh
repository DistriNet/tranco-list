#!/usr/bin/env bash

DATEVAR="date +%Y%m%d"

APP_ROOT="/data"

cd /tmp

wget https://s3.amazonaws.com/alexa-static/top-1m.csv.zip -O i_alexa_$(${DATEVAR}).csv.zip
unzip -p i_alexa_$(${DATEVAR}).csv.zip | cut -d "," -f 1,2 --output-delimiter "," > ${APP_ROOT}/source/alexa/alexa_$(${DATEVAR}).csv
rm i_alexa_$(${DATEVAR}).csv.zip

wget http://downloads.majestic.com/majestic_million.csv -O i_majestic_$(${DATEVAR}).csv
tail -n +2 i_majestic_$(${DATEVAR}).csv | cut -d "," -f 1,3 --output-delimiter "," > ${APP_ROOT}/source/majestic/majestic_$(${DATEVAR}).csv
rm i_majestic_$(${DATEVAR}).csv

wget https://s3-us-west-1.amazonaws.com/umbrella-static/top-1m.csv.zip -O i_umbrella_$(${DATEVAR}).csv.zip
unzip -p i_umbrella_$(${DATEVAR}).csv.zip | cut -d "," -f 1,2 --output-delimiter "," > ${APP_ROOT}/source/umbrella/umbrella_$(${DATEVAR}).csv
rm i_umbrella_$(${DATEVAR}).csv.zip

wget https://ak.quantcast.com/quantcast-top-sites.zip -O i_quantcast_$(${DATEVAR}).zip
unzip -p i_quantcast_$(${DATEVAR}).zip | tail -n +7 | grep -v "Hidden profile" | cut -d $'\t' -f 1,2 --output-delimiter "," > ${APP_ROOT}/source/quantcast/quantcast_$(${DATEVAR}).csv
rm i_quantcast_$(${DATEVAR}).zip

cd ~

python3 generate_domain_parts.py ${APP_ROOT} $(${DATEVAR})