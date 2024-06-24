python3 -m pip install --upgrade pip
pip3 install -r requirements.txt

cfg_file="config.txt"
rebuild_tpch=$(grep 'rebuild_tpch' "$cfg_file" | cut -d '=' -f2 | tr -d ' ')

cd resource
resource_dir=$(pwd)

if [ "$rebuild_tpch" = "True" ]; then
    rm -rf tpch tpch-kit
fi
if [ ! -d "tpch" ]; then
    mkdir tpch
fi
if [ ! -d "tpch-kit" ]; then
    git clone https://github.com/gregrahn/tpch-kit.git;
    cd tpch-kit/dbgen;
    make MACHINE=LINUX DATABASE=POSTGRESQL
    ./dbgen -s 10 -vf
    mv *.tbl ../../tpch
    cd "$resource_dir"    
fi
python3 tpch.py