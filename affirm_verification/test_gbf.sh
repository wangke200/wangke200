#! /bin/bash
echo "run test_gbf.jar with args [totalSize] [repeatNum] [testTime]"

totalSize=10000
repeatNum=100
testTime=10
echo "======================================================="
echo ">>> test build cost, from totalSize 10000 to 160000 <<<"
echo "======================================================="

for i in 10000 20000 40000 80000 160000
do
	java -jar ./test_gbf.jar ${i} ${repeatNum} ${testTime}
done


totalSize=36000
repeatNum=100
matchNum=100
testTime=10
max=10

echo "==================================================================="
echo ">>> test verify cost, matchNum from 100 to 5000 <<<"
echo "==================================================================="
for i in 100 1000 2000 3000 4000 5000
do
	java -jar ./test_gbf.jar ${totalSize} ${i}  ${testTime}
done

echo "================="
echo ">>> test done <<<"
echo "================="
