#!/bin/bash
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

# Description: This script runs random queries on both dense and sparse
#							 regions of the sparse array in SciDB. The tablename is
#              specified in line 63 of this script. The queries
#              are run on sprase region with result size = 10K, 100K, 1M
#							 and on dense region with result size = 10K, 100K, 1M, 10M

X0=$1
X1=$2
Y0=$3
Y1=$4

CPU=$5

RANGE=20000

#RANDOM=7

# divide 320 by CPU to get this value
NQUERIES=20
echo $NQUERIES

echo $X0 $X1 $Y0 $Y1

#sum=0
for ((i=0;i<$NQUERIES;i++))
do
	offset=$(echo "($RANDOM%$RANGE)-($RANGE/2)" | bc)
	#echo $offset
	X0_off=$(echo "$offset+$X0" | bc)
	X1_off=$(echo "$offset+$X1" | bc)
	Y0_off=$(echo "$offset+$Y0" | bc)
	Y1_off=$(echo "$offset+$Y1" | bc)
	if [ "$X0_off" -lt "0" ]; then
		X0_off=0
	fi
	if [ "$Y0_off" -lt "0" ]; then
		Y0_off=0
	fi
	#echo $X0_off $X1_off $Y0_off $Y1_off
	time iquery -naq "consume(between(sparse_1000000x1000000,$X0_off,$Y0_off,$X1_off,$Y1_off));"
	#sum=$(echo "$sum+$x" | bc -l)
done
#echo "Avg: " $(echo "$sum/$NQUERIES" | bc -l)
