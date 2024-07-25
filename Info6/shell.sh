#!/usr/bin/env bash

cat Info6/part-r-00000 | sort -n -k 2,2 -t$'\t' | tail -1