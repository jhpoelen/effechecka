#!/bin/bash

# used to deploy effechecka into a to a marathon deployment 
if (( $# == 1 )); then
    curl -i -H 'Content-Type: application/json' -d@$1 http://localhost:8082/v2/apps
else
    echo "please provide marathon config json you'd like to deploy like:"
    ls -1 *.json | xargs -L 1 echo
fi
