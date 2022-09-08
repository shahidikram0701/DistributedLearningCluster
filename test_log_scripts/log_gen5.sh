#!/bin/bash
(
    for i in {1..1000}
    do
    echo "Azure cache for redis is a managed implementation of redis."
    done
) 2>&1 | tee test_log.log