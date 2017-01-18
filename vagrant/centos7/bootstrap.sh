#!/usr/bin/env bash

# Make sure the package information is up-to-date
sudo yum update

# Install Cmake, Git, MPICH, zlib, OpenSSL, Gtest and LZ4
#sudo yum install cmake git mpich libmpich2-dev zlib1g-dev libssl-dev libgtest-dev liblz4-dev
sudo yum -y install cmake git wget vim
sudo yum -y install gcc-c++
sudo yum -y install mpich-3.2 mpich-3.2-devel
sudo yum -y install zlib zlib-devel
sudo yum -y install openssl-devel

# The following packages are in EPEL (http://fedoraproject.org/wiki/EPEL)
sudo yum -y install epel-release
sudo yum -y install lz4 lz4-devel 

# Install Zstandard
cd /home/vagrant
wget -N https://github.com/facebook/zstd/archive/v1.0.0.tar.gz # only download if newer
tar xf v1.0.0.tar.gz
cd zstd-1.0.0
sudo make install PREFIX='/usr'

# Install Blosc
cd /home/vagrant
git clone https://github.com/Blosc/c-blosc 
cd c-blosc
mkdir build
cd build
cmake -DCMAKE_INSTALL_PREFIX='/usr' ..
cmake --build .
sudo cmake --build . --target install

# Install Google Test
cd /home/vagrant
wget -N https://github.com/google/googletest/archive/release-1.8.0.tar.gz
tar xf release-1.8.0.tar.gz
cd googletest-release-1.8.0/googletest
sudo cmake -DBUILD_SHARED_LIBS=ON .
sudo make
sudo cp -a include/gtest /usr/include
sudo cp -a libgtest_main.so libgtest.so /usr/lib/

# Check out TileDB
if [ ! -d /home/vagrant/TileDB ]; then
    git clone https://github.com/Intel-HLS/TileDB /home/vagrant/TileDB
    chown -R vagrant /home/vagrant/TileDB
fi
