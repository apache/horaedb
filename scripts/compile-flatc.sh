wget 'https://github.com/google/flatbuffers/archive/refs/tags/v24.3.25.tar.gz'

tar xzf v24.3.25.tar.gz
cd flatbuffers-24.3.25
cmake -G "Unix Makefiles"
make
make install
ldconfig
flatc --version
cd -
rm -rf flatbuffers-24.3.25