#!/bin/bash
rm -rf logs/
rm -rf sdfs/
rm -rf outdata/

cd commands/process
go run process.go