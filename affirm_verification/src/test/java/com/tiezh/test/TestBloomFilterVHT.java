package com.tiezh.test;

import com.google.common.base.Charsets;
import com.google.common.hash.Funnel;
import com.google.common.hash.PrimitiveSink;
import com.tiezh.hash.BloomFilterStrategiesUtil;
import com.tiezh.hash.BloomFilterUtil;
import com.tiezh.hash.BloomFilterVHT;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public class TestBloomFilterVHT {

    public static void main(String[] args) throws Exception {


        byte[] sk = "mysecretkey".getBytes();
        String key = "key";
        int totalSize = 160000;
        double fpp = 0.000001;
        int idOffset = 1;
        int valBase = 60;
        int repeatNum = 1000;
        int testpow = 5;        //测试次数。第一次创建对象时要加载类对象，需要进行多轮测试直到稳定
        int mergedSize = totalSize / repeatNum;

        // 设置参数
        // 参数： [totoalSize] [fpp] [repeatNum] [mergeSize]
        if(args.length >= 1){
            totalSize = Integer.parseInt(args[0]);
        }
        if(args.length >= 2){
            fpp = Double.parseDouble(args[1]);
            if(fpp <= 0 || fpp >= 1){
                System.out.println("Illegal argument: the 2th arg [fpp] should be in (0,1).");
                return;
            }
        }
        if(args.length >= 3) {
            repeatNum = Integer.parseInt(args[2]);
            if (repeatNum < 1 || repeatNum > totalSize) {
                System.out.println("Illegal argument: the 3th arg [repeatNum] should be in [1, " + totalSize + "]");
                return;
            }
        }
        if(args.length >= 4) {
            mergedSize = Integer.parseInt(args[3]);
            if (mergedSize < 1 || mergedSize > (totalSize / repeatNum)) {
                System.out.println("Illegal argument: the 4th arg [mergeSize] should be in [1, " + (totalSize / repeatNum) + "]");
                return;
            }
        }

        //重新检查mergedSize，如果不符合则设置为合法的最大值
        if(mergedSize < 1 || mergedSize > (totalSize / repeatNum)){
            mergedSize = totalSize / repeatNum;
        }

        //计算bloom filter的两个参数，bit长度和hash function 个数
        long optimalNumOfBits = BloomFilterUtil.optimalNumOfBits(totalSize, fpp);
        long optimalNumOfHashFunctions = BloomFilterUtil.optimalNumOfHashFunctions(totalSize, optimalNumOfBits);


        Logger logger = LoggerFactory.getLogger(Test.class);
        logger.info(">> Test BloomFilter VHT <<");
        logger.info("insertions: " + totalSize);
        logger.info("fpp: " + fpp);
        logger.info("optimalNumOfBits: " + optimalNumOfBits);
        logger.info("optimalNumOfHashFunctions: " + optimalNumOfHashFunctions);
        logger.info("repeatNum: " + repeatNum);
        logger.info("mergedSize: " + mergedSize);

        // 测试对象
        LinkedList<Student> data = Student.generateDatas(totalSize, idOffset, valBase, repeatNum);

        //测试hash策略
        ArrayList<BloomFilterUtil.Strategy> strategies = new ArrayList<>();
        strategies.add(new BloomFilterStrategiesUtil.MURMUR128_MITZ_32());
        strategies.add(new BloomFilterStrategiesUtil.MURMUR128_MITZ_64());
        strategies.add(new BloomFilterStrategiesUtil.MURMURWITHKEY128_MITZ_32(sk));
        strategies.add(new BloomFilterStrategiesUtil.MURMURWITHKEY128_MITZ_64(sk));;
        strategies.add(new BloomFilterStrategiesUtil.HMACSHA256_MITZ_32(sk));
        strategies.add(new BloomFilterStrategiesUtil.HMACSHA256_MITZ_64(sk));

        //定义过滤规则funnel（不处理）
        Funnel<Student> funnel = new Funnel<Student> (){
            @Override
            public void funnel(Student from, PrimitiveSink into) {
                into.putString(Integer.toString(from.id), Charsets.UTF_8)
                        .putString(Integer.toString(from.score), Charsets.UTF_8);
            }
        };

        for(BloomFilterUtil.Strategy strategy : strategies) {
            long etime, stime;
            long addcost, verifycost, mergecost;

            for(int t = 0; t < testpow; t++){

                BloomFilterVHT<Integer, Student> vht =
                        new BloomFilterVHT<>(funnel,totalSize, fpp, strategy);

                // add
                stime = System.currentTimeMillis();
                for(Student student : data){
                    vht.add(student.score, student);
                }
                etime = System.currentTimeMillis();
                addcost = etime - stime;

                //merge
                Set<Integer> mergedKeys = new HashSet<>();
                Set<Integer> allKeys = vht.keySet();
                //选择mergeSize个key进行合并
                Iterator<Integer> iterator = allKeys.iterator();
                for(int i = 0; i < mergedSize; i++){
                    mergedKeys.add(iterator.next());
                }
                stime = System.currentTimeMillis();
                BloomFilterUtil<Student> mergebf =
                        (BloomFilterUtil<Student>) vht.getMergedHash(mergedKeys);
                etime = System.currentTimeMillis();
                mergecost = etime - stime;


                //verify
                stime = System.currentTimeMillis();
                BloomFilterUtil<Student> verifybf = BloomFilterUtil.create(funnel, totalSize, fpp, strategy);
                Collection<Student> verifyData = vht.getMergedValues(mergedKeys);
                for(Student student : verifyData){
                    verifybf.put(student);
                }
                boolean verify = true;

                long[] mergeHashLongs = mergebf.getBitsArray();
                long[] verifyHashLongs = verifybf.getBitsArray();
                for(int i = 0; i < verifyHashLongs.length; i++){
                    if(mergeHashLongs[i] != verifyHashLongs[i]){
                        verify = false;
                        break;
                    }
                }
                etime = System.currentTimeMillis();
                verifycost = etime - stime;



                logger.info("[" + strategy.getClass().getSimpleName() + "],"
                        + " add cost: " + addcost + "ms,"
                        + " merge cost: " + mergecost + "ms,"
                        + " verify cost: " + verifycost + "ms,"
                        + " verify result: " + verify);

            }
        }
    }
}
