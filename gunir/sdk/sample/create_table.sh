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
TOOL=$BLADE_ROOT/blade-bin/gunir/sdk/create_table
$TOOL --gunir_table_name=csv_test --gunir_input_files=./testdata/test.dat --gunir_input_schema=./testdata/test.csv --gunir_input_message=test --gunir_output_dir=./csv_test/ --logtostderr
