#!/bin/bash
(
    for i in {1..1000}
    do
    echo "Storage Services provides REST and SDK APIs for storing and accessing data on the cloud."
    done
) 2>&1 | tee test_log.log