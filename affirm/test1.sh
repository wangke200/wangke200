node=4
matched=100 #useless


echo "================================"
echo "           Build test"
echo "================================"

echo "java -jar TestBuild.jar [Protocol] [nodeNum] [dataSize] [matchedNum] [blockSize]"

echo "-------------------------"
echo ">>>  Plaintext Build  <<<"
echo "-------------------------"
for total in 10000 20000 40000 80000 160000
do
	java -jar ./TestBuild.jar ./cli-configure.json Plaintext $node $total $matched
done



echo "-------------------------"
echo ">>>    AFFIRM Build   <<<"
echo "-------------------------"
for total in 10000 20000 40000 80000 160000
do
        java -jar ./TestBuild.jar ./cli-configure.json AFFIRM $node $total $matched
done




