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

BLADE_ROOT=`_find_project_root`
TOOL=$BLADE_ROOT/blade-bin/gunir/sdk/parse_log_main
$TOOL --gunir_table_name=test --gunir_input_files=./testdata/test.dat --gunir_output_dir=./test/
