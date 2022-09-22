#!/bin/bash
cd commands
cd coordinator/
go run coordinator.go &
cd ../process
go run process.go &
cd ../..