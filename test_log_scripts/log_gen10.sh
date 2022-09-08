#!/bin/bash
(
    for i in {1..1000}
    do
    echo "Azure data lake is a scalable data storage and analytic service for big data analytics workloads that require developers to run massively parallel queries."
    done
) 2>&1 | tee test_log.log