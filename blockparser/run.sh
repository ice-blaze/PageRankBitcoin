#!/usr/bin/env bash
make && time ./parser dumpDBA --output output_transactions.bin --nbtransactionmax 350
#--height 151000
