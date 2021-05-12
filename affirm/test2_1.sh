#! /bin/bash

data_size=40000

echo "=========================================================="
echo "                    throughput test                       "
echo "=========================================================="
echo "-------------------------------"
echo "<<< Plaintext lantency test >>>"
echo "-------------------------------"

protocol='Plaintext'
for matched_size in 1 10 20 50 100
do
	java -jar ./TestBuild.jar ./cli-configure.json $protocol 4 $data_size $matched_size
	java -jar ./TestQuery.jar ./cli-configure.json $protocol 4 $data_size $matched_size
	java -jar ./TestQuery.jar ./cli-configure.json $protocol 4 $data_size $matched_size
	java -jar ./TestQuery.jar ./cli-configure.json $protocol 4 $data_size $matched_size
done


echo "----------------------------"
echo "<<< AFFIRM lantency test >>>"
echo "----------------------------"

protocol='AFFIRM'
for matched_size in 1 10 20 50 100
do
        java -jar ./TestBuild.jar ./cli-configure.json $protocol 4 $data_size $matched_size
        java -jar ./TestQuery.jar ./cli-configure.json $protocol 4 $data_size $matched_size
        java -jar ./TestQuery.jar ./cli-configure.json $protocol 4 $data_size $matched_size
        java -jar ./TestQuery.jar ./cli-configure.json $protocol 4 $data_size $matched_size
done

