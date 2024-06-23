pip3 install -r requirements.txt

cd resource
resource_dir=$(pwd)
if [ ! -d "tpch" ]; then
    mkdir tpch
fi
if [ ! -d "tpch-kit" ]; then
    git clone https://github.com/gregrahn/tpch-kit.git;
    cd tpch-kit/dbgen;
    make MACHINE=LINUX DATABASE=POSTGRESQL
    ./dbgen -s 1 -vf
    mv *.tbl ../../tpch
    cd "$resource_dir"    
fi
python3 tpch.py