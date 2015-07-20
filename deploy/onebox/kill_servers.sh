#!/bin/bash

for i in $(ps -ef | grep "gunir_main" | grep -v "grep "| awk '{print $2}'); do
    echo "Shutdown server at pid :$i"
    kill -9 $i
done
