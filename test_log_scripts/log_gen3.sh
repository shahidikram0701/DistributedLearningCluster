#!/bin/bash
(
    for i in {1..2300}
    do
    printf '%s|%s|%d|%s|%s' \
        "20130101" \
        $(printf '%02d:%02d:%02d' $((RANDOM % 3 + 9)) $((RANDOM % 60)) $((RANDOM % 60)) ) \
        $((RANDOM % 2000 + 3000)) \
        "$status" \
        "$data"
    echo "Azure search provides text search and a subset of odata's structured filters using rest or sdk apis."
    printf '%s|%s|%d|%s|%s' \
        "20130101" \
        $(printf '%02d:%02d:%02d' $((RANDOM % 3 + 9)) $((RANDOM % 60)) $((RANDOM % 60)) ) \
        $((RANDOM % 2000 + 3000)) \
        "$status" \
        "$data"
    echo "You can automate processes using runbooks or automate configuration management using desired state configuration."
    done
)