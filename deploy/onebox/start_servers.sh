#!/bin/bash
source env.sh

# build link for binary
echo -en $PRINT_GREEN"Prepare binary links ... "$PRINT_END
ln -sf $BLADE_ROOT_DIR/blade-bin/gunir/gunir_main
#ln -sf $BLADE_ROOT_DIR/gunircli
echo -e $PRINT_RED"done"$PRINT_END

# kill all servers before restart them
./kill_servers.sh

# prepare flag parameters
gen_flag > $flag_file

# prepare common environment
add_dir $result_output_dir
add_dir $job_cache_dir

# start master server
echo -en $PRINT_GREEN"Start master server ... "$PRINT_END
for ((i = 0, port=$master_port;  i < $master_number; ++i, ++port)); do
    echo -en $PRINT_GREEN#$i ".. "$PRINT_END 
    add_dir ${master_log}_$i
    add_dir ${master_working_dir}
    ./gunir_main $args --gunir_role=master \
        --gunir_master_port=$port \
        --log_dir=${master_log}_$i \
        --logtostderr &> master.$i.stderr &
done
echo -e $PRINT_RED"done"$PRINT_END
sleep 1

# start leafnode server
echo -en $PRINT_GREEN"Start leafnode server ... "$PRINT_END
for ((i = 0, port=$leafnode_port;  i < $leafnode_number; ++i, ++port)); do
    echo -en $PRINT_GREEN#$i ".. "$PRINT_END
    add_dir ${leafnode_log}_$i
    add_dir ${leafnode_working_dir}_$i
    ./gunir_main $args --gunir_role=leafnode \
        --gunir_leafnode_port=$port \
        --log_dir=${leafnode_log}_$i \
        --logtostderr &> leafnode.$i.stderr &
done
echo -e $PRINT_RED"done"$PRINT_END

# start stemnode server
echo -en $PRINT_GREEN"Start stemnode server ... "$PRINT_END
for ((i = 0, port=$stemnode_port;  i < $stemnode_number; ++i, ++port)); do
    echo -en $PRINT_GREEN#$i ".. "$PRINT_END
    add_dir ${stemnode_log}_$i
    add_dir ${stemnode_working_dir}_$i
    ./gunir_main $args --gunir_role=stemnode \
        --gunir_stemnode_port=$port \
        --log_dir=${stemnode_log}_$i \
        --logtostderr &> stemnode.$i.stderr &
done
echo -e $PRINT_RED"done"$PRINT_END

# all servers are started
echo -e $PRINT_RED"All servers are ready ... enjoy!"$PRINT_END
