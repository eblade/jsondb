#!/bin/bash

for file in $1/*/*; do cat $file | json_verify -q || echo $file; done
