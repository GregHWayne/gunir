
#### identity ####
user=$USER
role=$USER

#### print environment ####
PRINT_GREEN='\033[40;32m'
PRINT_YELLOW='\033[40;33m'
PRINT_WHITE='\033[40;37m'
PRINT_RED='\033[40;31m'
PRINT_END='\033[0m'

#### server setting ####
#ip=127.0.0.1
local_ip=`/sbin/ifconfig  | grep 'inet addr:'| grep -v '127.0.0.1' | cut -d: -f2 | awk '{ print $1}'`
ip=$local_ip
master_port=11000
stemnode_port=22000
leafnode_port=23000
monitor_port=8911
flag_file=./gunir.flag

leafnode_number=10
stemnode_number=3

#### log setting ####
v=20
master_log=master_log
stemnode_log=stemnode_log
leafnode_log=leafnode_log
master_working_dir=master_data
stemnode_working_dir=stemnode_data
leafnode_working_dir=leafnode_data
master_number=1

#### output setting ####
result_output_dir=result_output/
job_cache_dir=job_cache/

#### help function ####

function add_dir() {
    if [ -d $1 ]; then
        return 0
    fi
    mkdir $1
}

function gen_flag() {
    echo --gunir_master_addr=$ip
    echo --gunir_master_port=$master_port
    echo --gunir_leafnode_num=$max_leafnode_num
    echo --gunir_query_output_dir=$result_output_dir
    echo --gunir_job_cache_dir=$job_cache_dir
    echo --v=25
    echo --log_dir=./
}

#### prepare configuration ####
#gen_flag > $flag_file
args=--flagfile=$flag_file

function _find_project_root()
{
    local dir
    dir=$PWD;
    while [ "$dir" != "/" ]; do
        if [ -f "$dir/BLADE_ROOT" ]; then
            echo "$dir"
            return 0
        fi;
        dir=`dirname "$dir"`
    done
    return 1
}

BLADE_ROOT_DIR=`_find_project_root`


