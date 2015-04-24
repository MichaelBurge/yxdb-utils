#!/bin/sh

sudo docker run -v $PWD:/mnt/pwd -w /mnt/pwd yxdb2csv csv2yxdb "$@"
