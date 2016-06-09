#!/bin/bash
set -e

if [[ $TRAVIS_OS_NAME == 'linux' ]]; then

    # install packages
    apt-get -y install lcov mpich zlib1g-dev libssl-dev libgtest-dev

    # install gtest
    cd /usr/src/gtest
    sudo cmake .
    sudo make
    sudo mv libgtest* /usr/lib/
    cd $TRAVIS_BUILD_DIR

fi

if [[ $TRAVIS_OS_NAME == 'osx' ]]; then

    #install packages
    brew update
    brew install openssl lcov doxygen mpich

    #install gtest
    wget https://github.com/google/googletest/archive/release-1.7.0.tar.gz
    tar xf release-1.7.0.tar.gz
    cd googletest-release-1.7.0
    cmake .
    make
    mv include/gtest /usr/include/gtest
    mv libgtest_main.a libgtest.a /usr/lib/
    cd $TRAVIS_BUILD_DIR
    export CXX=g++-4.9

fi

# install lcov to coveralls conversion + upload tool
gem install coveralls-lcov
   
# build
make -j 4
