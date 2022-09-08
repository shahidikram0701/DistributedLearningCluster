#!/bin/bash
(
    for i in {1..1000}
    do
    echo "Azure synapse analytics is a fully managed cloud data warehouse."
    done
  ...
) 2>&1 | tee test_log.log