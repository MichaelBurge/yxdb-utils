#!/bin/sh

sudo docker run -v $PWD:/mnt/pwd -w /mnt/pwd yxdb-utils yxdb2csv "$@"
