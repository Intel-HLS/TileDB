# The MIT License
#
# Copyright (c) 2016 MIT and Intel Corp.
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in
# all copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
# THE SOFTWARE.

echo "Sparse - 10K"
sudo ~/workspace/clean_caches.sh
sudo ~/workspace/clean_caches.sh
./random_sparse_query.sh 0 45910000 130000000 136000000

echo "Sparse - 100K"
sudo ~/workspace/clean_caches.sh
sudo ~/workspace/clean_caches.sh
./random_sparse_query.sh 0 53900000 130000000 136000000

echo "Sparse - 1M"
sudo ~/workspace/clean_caches.sh
sudo ~/workspace/clean_caches.sh
./random_sparse_query.sh 0 92320400 130000000 136000000

echo "Dense - 10K"
sudo ~/workspace/clean_caches.sh
sudo ~/workspace/clean_caches.sh
./random_sparse_query.sh 63380000 64900000 114000000 122400000

echo "Dense - 100K"
sudo ~/workspace/clean_caches.sh
sudo ~/workspace/clean_caches.sh
./random_sparse_query.sh 62000000 64900000 114000000 122400000

echo "Dense - 1M"
sudo ~/workspace/clean_caches.sh
sudo ~/workspace/clean_caches.sh
./random_sparse_query.sh 99000000 100000000 116000000 132600000

echo "Dense - 10M"
sudo ~/workspace/clean_caches.sh
sudo ~/workspace/clean_caches.sh
./random_sparse_query.sh 48300000 85227100 115778980 122337000
