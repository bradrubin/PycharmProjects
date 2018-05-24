#!/usr/bin/env bash

unzip Export*.zip -d /Users/bsrubin/Website/NPRoadTrip
source activate pyspark
python NPRT.py
rm -rf Export*.zip
