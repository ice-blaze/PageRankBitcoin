#!/usr/bin/env bash
make && time ./parser dumpDBA --output output_transactions.bin
# --height 1000
