#!/bin/bash
(
    for i in {1..1000}
    do
    printf '%s|%s|%d|%s|%s' \
        "20130101" \
        $(printf '%02d:%02d:%02d' $((RANDOM % 3 + 9)) $((RANDOM % 60)) $((RANDOM % 60)) ) \
        $((RANDOM % 2000 + 3000)) \
        "$status" \
        "$data"
    echo "Azure cache for redis is a managed implementation of redis."
    
    printf '%s|%s|%d|%s|%s' \
        "20130101" \
        $(printf '%02d:%02d:%02d' $((RANDOM % 3 + 9)) $((RANDOM % 60)) $((RANDOM % 60)) ) \
        $((RANDOM % 2000 + 3000)) \
        "$status" \
        "$data"
    echo "Azure functions are used in serverless computing architectures where subscribers can execute code as an event driven function-as-a-service (faas) without managing the underlying server resources."
    done
) 2>&1 | tee test_log.log