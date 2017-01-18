#!/usr/bin/env bash

# Make sure the package information is up-to-date
apt-get update

# Install Cmake, Git, MPICH, zlib, OpenSSL, Gtest and LZ4
sudo apt-get -y install cmake git mpich libmpich2-dev zlib1g-dev libssl-dev libgtest-dev liblz4-dev

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
cd /usr/src/gtest
sudo cmake .
sudo make
sudo mv libgtest* /usr/lib/

# Check out TileDB
if [ ! -d /home/vagrant/TileDB ]; then
    git clone https://github.com/Intel-HLS/TileDB /home/vagrant/TileDB
    chown -R vagrant /home/vagrant/TileDB
fi
