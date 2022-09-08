#!/bin/bash
(
    for i in {1..1000}
    do
    echo "Azure data factory, is a data integration service that allows creation of data-driven workflows in the cloud for orchestrating and automating data movement and data transformation."
    done
  ...
) 2>&1 | tee test_log.log