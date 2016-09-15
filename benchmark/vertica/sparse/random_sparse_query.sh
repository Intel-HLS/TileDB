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

X0=$1
X1=$2
Y0=$3
Y1=$4
CPU=$5
RANGE=20000

SQLFILE=./qxx$CPU.sql
TABLE=vessel_traffic

# Divide it by CPU keeping total n queries = 320
NQUERIES=20

echo $X0 $X1 $Y0 $Y1

sum=0
for ((i=0;i<$NQUERIES;i++))
do
	offset=$(echo "($RANDOM%$RANGE)-($RANGE/2)" | bc)
	temptable="t1xx$CPU"
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
	rm -f $SQLFILE
	echo "drop table $temptable;" >> $SQLFILE
	echo "\\timing" >> $SQLFILE
	echo "select X,Y INTO TEMP TABLE $temptable ON COMMIT PRESERVE ROWS from $TABLE where x>=$X0_off and x<$X1_off and y>=$Y0_off and y<$Y1_off;" >> $SQLFILE
	x=`vsql -f $SQLFILE | grep "First" | awk '{print $6}'`
	sum=$(echo "$sum+$x" | bc -l)
done
echo "Avg: " $(echo "$sum/$NQUERIES" | bc -l)

