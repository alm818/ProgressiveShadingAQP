clear
# Check if the number of arguments is exactly 2
if ! ([ "$#" -eq 2 ] || [ "$#" -eq 3 ]); then
    exit 1
fi
cd build/Release/generators
cmake -DCMAKE_BUILD_TYPE=Release -DCMAKE_TOOLCHAIN_FILE=conan_toolchain.cmake ../../..
cmake --build . --config Release
cd ../../..
rm -rf out
if [ "$#" -eq 2 ]; then
    build/Release/generators/main "$1" "$2"
else
    build/Release/generators/main "$1" "$2" "$3"
fi
rm -rf out