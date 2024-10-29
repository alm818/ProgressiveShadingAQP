python3 -m pip install --upgrade pip
export SKLEARN_ALLOW_DEPRECATED_SKLEARN_PACKAGE_INSTALL=True
pip3 install -r requirements.txt

cfg_file="config.txt"
rebuild_data=$(grep 'rebuild_data' "$cfg_file" | cut -d '=' -f2 | tr -d ' ')

conan profile detect --force
if [ -d build ]; then
    rm -r build
fi
conan install . --output-folder=. --build=missing --settings=build_type=Release

cd resource
resource_dir=$(pwd)

if [ "$rebuild_data" = "True" ]; then
    rm -rf tpch tpch-kit
fi
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

python3 postgres.py
python3 tpch.py