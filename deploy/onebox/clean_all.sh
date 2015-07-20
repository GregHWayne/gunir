#!/bin/bash

rm -rf master_data master_log master_log_* leafnode_log_* stemnode_log_*
rm -rf result_output job_cache 
rm -rf leafnode_* stemnode_*
rm -rf gunir.flag gunir_main client*
rm -rf *.stderr

if [ x$1 = x"all" ]
then
    rm -rf core.* *.out
fi
