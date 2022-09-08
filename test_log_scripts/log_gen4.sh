#!/bin/bash
(
    for i in {1..1000}
    do
    echo "Cosmos db is a nosql database service that implements a subset of the sql select statement on json documents."
    done
  ...
) 2>&1 | tee test_log.log