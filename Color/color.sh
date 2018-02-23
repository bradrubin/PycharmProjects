#!/usr/bin/env bash

hadoop fs -rm -skipTrash testOut/*
rm map/*
rm transform/*

PYSPARK_PYTHON=/opt/miniconda3/envs/pyspark/bin/python \
   spark-submit \
   --conf spark.yarn.appMasterEnv.PYSPARK_PYTHON=/opt/miniconda3/envs/pyspark/bin/python \
   --master yarn-cluster \
   --executor-memory 6G \
   --num-executors 15 \
   --executor-cores 11 \
   --driver-memory 6G \
   --files /home/brad/brad.keytab \
   color.py \
   colorOut \
   testOut \
   15

hadoop fs -get testOut/M* map
hadoop fs -get testOut/T* transform

rm colorMap.mp4
rm colorTransform.mp4
cat map/*.png | ffmpeg -f image2pipe -r 10 -i - -c:v libx264 -pix_fmt yuv420p colorMap.mp4
cat transform/*.png | ffmpeg -f image2pipe -r 10 -i - -c:v libx264 -pix_fmt yuv420p -vf "scale=trunc(iw/2)*2:trunc(ih/2)*2" colorTransform.mp4