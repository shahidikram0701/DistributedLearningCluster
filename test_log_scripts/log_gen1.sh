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
    echo "Storage Services provides REST and SDK APIs for storing and accessing data on the cloud."
    
    printf '%s|%s|%d|%s|%s' \
        "20130101" \
        $(printf '%02d:%02d:%02d' $((RANDOM % 3 + 9)) $((RANDOM % 60)) $((RANDOM % 60)) ) \
        $((RANDOM % 2000 + 3000)) \
        "$status" \
        "$data"
    echo "Azure automation provides a way for users to automate the manual, long-running, error-prone, and frequently repeated tasks that are commonly performed in a cloud and enterprise environment."
    done
)