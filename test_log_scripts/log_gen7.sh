#!/bin/bash
(
    for i in {1..1532}
    do
    printf '%s|%s|%d|%s|%s' \
        "20130101" \
        $(printf '%02d:%02d:%02d' $((RANDOM % 3 + 9)) $((RANDOM % 60)) $((RANDOM % 60)) ) \
        $((RANDOM % 2000 + 3000)) \
        "$status" \
        "$data"
    echo "Azure sql database works to create, scale and extend applications into the cloud using microsoft sql server technology. It also integrates with active directory, microsoft system center and hadoop."
    
    printf '%s|%s|%d|%s|%s' \
        "20130101" \
        $(printf '%02d:%02d:%02d' $((RANDOM % 3 + 9)) $((RANDOM % 60)) $((RANDOM % 60)) ) \
        $((RANDOM % 2000 + 3000)) \
        "$status" \
        "$data"
    echo "Azure iot edge is a fully managed service built on iot hub that allows for cloud intelligence deployed locally on iot edge devices."
    done
)