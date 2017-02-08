#!/usr/bin/env bash
set -x -e

##############################
# download & build dependency
##############################

WORK_DIR=`pwd`
DEPS_SOURCE=`pwd`/thirdsrc
DEPS_PREFIX=`pwd`/thirdparty
DEPS_CONFIG="--prefix=${DEPS_PREFIX} --disable-shared --with-pic"
FLAG_DIR=`pwd`/.build

export PATH=${DEPS_PREFIX}/bin:$PATH
mkdir -p ${DEPS_PREFIX} ${DEPS_SOURCE} ${FLAG_DIR}

cd ${DEPS_SOURCE}

#boost 1.57.0
if [ ! -d "${DEPS_PREFIX}/boost_1_57_0" ]; then
    wget https://raw.githubusercontent.com/lylei9/boost_1_57_0/master/boost_1_57_0.tar.gz
    tar zxf boost_1_57_0.tar.gz
    rm -rf ${DEPS_PREFIX}/boost_1_57_0
    mv boost_1_57_0 ${DEPS_PREFIX}/boost_1_57_0
fi

# protobuf 2.6.1
# you should run sudo apt-get install autoconf automake libtool curl make g++ unzip
if [ ! -f "${DEPS_PREFIX}/lib/libprotobuf.a" ] \
    || [ ! -d "${DEPS_PREFIX}/include/google/protobuf" ]; then
    wget https://github.com/google/protobuf/releases/download/v2.6.1/protobuf-2.6.1.tar.gz
    tar zxf protobuf-2.6.1.tar.gz
    cd protobuf-2.6.1
    autoreconf -ivf
    ./configure ${DEPS_CONFIG}
    make -j4
    make install
    cd -
fi

# snappy
if [ ! -f "${DEPS_PREFIX}/lib/libsnappy.a" ] \
    || [ ! -f "${DEPS_PREFIX}/include/snappy.h" ]; then
    wget https://github.com/google/snappy/releases/download/1.1.3/snappy-1.1.3.tar.gz
    tar zxf snappy-1.1.3.tar.gz
    cd snappy-1.1.3
    ./configure ${DEPS_CONFIG}
    make -j4
    make install
    cd -
fi

# sofa-pbrpc
if [ ! -f "${DEPS_PREFIX}/lib/libsofa-pbrpc.a" ] \
    || [ ! -d "${DEPS_PREFIX}/include/sofa/pbrpc" ]; then
    rm -rf sofa-pbrpc

    git clone --depth=1 https://github.com/baidu/sofa-pbrpc.git sofa-pbrpc
    cd sofa-pbrpc
    sed -i '/BOOST_HEADER_DIR=/ d' depends.mk
    sed -i '/PROTOBUF_DIR=/ d' depends.mk
    sed -i '/SNAPPY_DIR=/ d' depends.mk
    echo "BOOST_HEADER_DIR=${DEPS_PREFIX}/boost_1_57_0" >> depends.mk
    echo "PROTOBUF_DIR=${DEPS_PREFIX}" >> depends.mk
    echo "SNAPPY_DIR=${DEPS_PREFIX}" >> depends.mk
    echo "PREFIX=${DEPS_PREFIX}" >> depends.mk
    make -j4
    make install
    cd -
fi

# common
if [ ! -f "${DEPS_PREFIX}/lib/libcommon.a" ]; then
    rm -rf common
    git clone -b cpp11 https://github.com/baidu/common
    cd common
    sed -i 's/^PREFIX=.*/PREFIX=..\/..\/thirdparty/' config.mk
    sed -i '/^INCLUDE_PATH=*/s/$/ -I..\/..\/thirdparty\/boost_1_57_0/g' Makefile
    make -j4
    make install
    cd -
fi

cd ${WORK_DIR}

echo "PBRPC_PATH=./thirdparty" > depends.mk
echo "PROTOBUF_PATH=./thirdparty" >> depends.mk
echo "PROTOC_PATH=./thirdparty/bin/" >> depends.mk
echo 'PROTOC=$(PROTOC_PATH)protoc' >> depends.mk
echo "PBRPC_PATH=./thirdparty" >> depends.mk
echo "BOOST_PATH=./thirdparty/boost_1_57_0" >> depends.mk
#echo "GFLAG_PATH=./thirdparty" >> depends.mk
#echo "GTEST_PATH=./thirdparty" >> depends.mk
echo "COMMON_PATH=./thirdparty" >> depends.mk
#echo "TCMALLOC_PATH=./thirdparty" >> depends.mk