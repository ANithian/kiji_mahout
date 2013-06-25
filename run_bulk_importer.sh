#!/bin/bash
export KIJI_CLASSPATH=$PWD/target/*
#kiji bulk-import --importer="org.hokiesuns.kiji.RatingsBulkLoader" --input="format=text file=./input/ch4/gl_10m.csv" --output="format=kiji nsplits=1 table=kiji://.env/default/product_ratings file=./hfiles"
kiji bulk-import --importer="org.hokiesuns.kiji.RatingsBulkLoader" --input="format=text file=./input/ch2/gl_100k.csv" --output="format=kiji nsplits=1 table=kiji://.env/default/product_ratings file=./hfiles"
#kiji bulk-import --importer="org.hokiesuns.kiji.RatingsBulkLoader" --input="format=text file=./input/ch2/input_sample.csv" --output="format=kiji nsplits=1 table=kiji://.env/default/product_ratings file=./hfiles"
