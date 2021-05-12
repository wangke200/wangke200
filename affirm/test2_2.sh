#! /bin/bash

data_size=40000
matched_size=8000

echo "=========================================================="
echo "                       latency test                       "
echo "=========================================================="
echo "-------------------------------"
echo "<<< Plaintext lantency test >>>"
echo "-------------------------------"
for node in 1 2 3 4 5 6
do
	java -jar ./TestBuild.jar ./cli-configure.json Plaintext $node $data_size $matched_size
	java -jar ./TestQuery.jar ./cli-configure.json Plaintext $node $data_size $matched_size
	java -jar ./TestQuery.jar ./cli-configure.json Plaintext $node $data_size $matched_size
	java -jar ./TestQuery.jar ./cli-configure.json Plaintext $node $data_size $matched_size
	echo ""
done


echo "----------------------------"
echo "<<< AFFIRM lantency test >>>"
echo "----------------------------"

for node in 1 2 3 4 5 6
do
	java -jar ./TestBuild.jar ./cli-configure.json AFFIRM $node $data_size $matched_size
	java -jar ./TestQuery.jar ./cli-configure.json AFFIRM $node $data_size $matched_size
	java -jar ./TestQuery.jar ./cli-configure.json AFFIRM $node $data_size $matched_size
	java -jar ./TestQuery.jar ./cli-configure.json AFFIRM $node $data_size $matched_size
	echo ""
done




