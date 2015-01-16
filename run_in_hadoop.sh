#!/bin/bash

WORKING_DIR=`pwd`

# echo $WORKING_DIR

cd ~/hadoop-1.0.3

echo "copy input data to HFS"
bin/hadoop fs -rmr input/*
bin/hadoop fs -put "$WORKING_DIR/input/output_transactions_337100.bin" input

echo "remove previous output"
bin/hadoop fs -rmr output*

echo "run the jar into hadoop"
bin/hadoop jar "$WORKING_DIR/target/bitcoin-1.jar" heigvd.bda.labs.graph.Bitcoin 8 input output 5

echo "retrieve the output result"
rm -r $WORKING_DIR/outputSORTED
bin/hadoop fs -get outputSORTED "$WORKING_DIR/outputSORTED"

cd $WORKING_DIR
