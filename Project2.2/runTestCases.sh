#!/bin/bash
rm output1.txt
./test.out < tc1.txt  >> output1.txt
echo "*********************************" >> output1.txt
./test.out < tc2.txt  >> output1.txt
echo "*********************************" >> output1.txt
./test.out < tc3.txt  >> output1.txt

echo "*********************************" >> output1.txt
./test.out < tc4.txt  >> output1.txt
echo "*********************************" >> output1.txt
./test.out < tc5.txt  >> output1.txt
echo "*********************************" >> output1.txt
./test.out < tc6.txt  >> output1.txt
