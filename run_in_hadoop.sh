#!/bin/bash

WORKING_DIR=`pwd`

# echo $WORKING_DIR

cd ~/hadoop-1.0.3

echo "copy input data to HFS"
bin/hadoop fs -put "$WORKING_DIR/input/sample_transactions.bin" input

echo "remove previous output"
bin/hadoop fs -rmr output

echo "run the jar into hadoop"
bin/hadoop jar "$WORKING_DIR/target/bitcoin-1.jar" heigvd.bda.labs.graph.Bitcoin 1 input output

echo "retrieve the output result"
rm -r $WORKING_DIR/output
bin/hadoop fs -get output "$WORKING_DIR/output"

cd $WORKING_DIR
