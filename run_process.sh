#!/bin/bash
rm -rf logs/
rm -rf sdfs/
rm -rf outdata/
rm -rf sdfs_models/

pkill python3

cd commands/process
go run process.go