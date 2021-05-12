package com.tiezh.test;

import com.google.common.base.Charsets;
import com.google.common.hash.Funnel;
import com.google.common.hash.PrimitiveSink;
import com.tiezh.hash.BloomFilterStrategiesUtil;
import com.tiezh.hash.BloomFilterUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.LinkedList;

public class TestBloomFilter {
    public static void main(String[] args) throws Exception {

        byte[] sk = "mysecretkey".getBytes();
        String key = "key";
        int totalSize = 160000;
        double fpp = 0.000001;
        int idOffset = 1;
        int valBase = 60;
        int repeatNum = 1000;
        int testpow = 5;        //测试次数。第一次创建对象时要加载类对象，需要进行多轮测试直到稳定

        // 设置参数
        // 参数： [totoalSize] [fpp] [repeatNum] [mergedSize]
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


        //计算bloom filter的两个参数，bit长度和hash function 个数
        long optimalNumOfBits = BloomFilterUtil.optimalNumOfBits(totalSize, fpp);
        long optimalNumOfHashFunctions = BloomFilterUtil.optimalNumOfHashFunctions(totalSize, optimalNumOfBits);


        Logger logger = LoggerFactory.getLogger(Test.class);
        logger.info(">> Test BloomFilterUtil <<");
        logger.info("insertions: " + totalSize);
        logger.info("fpp: " + fpp);
        logger.info("optimalNumOfBits: " + optimalNumOfBits);
        logger.info("optimalNumOfHashFunctions: " + optimalNumOfHashFunctions);
        logger.info("repeatNum: " + repeatNum);

        // 测试对象
        LinkedList<Student> data = Student.generateDatas(totalSize, idOffset, valBase, repeatNum);
        ArrayList<BloomFilterUtil.Strategy> strategies = new ArrayList<BloomFilterUtil.Strategy>();
        strategies.add(new BloomFilterStrategiesUtil.MURMUR128_MITZ_32());
        strategies.add(new BloomFilterStrategiesUtil.MURMUR128_MITZ_64());
        strategies.add(new BloomFilterStrategiesUtil.MURMURWITHKEY128_MITZ_32(sk));
        strategies.add(new BloomFilterStrategiesUtil.MURMURWITHKEY128_MITZ_64(sk));
//        strategies.add(new BloomFilterStrategiesUtil.SHA1_MITZ_32());
//        strategies.add(new BloomFilterStrategiesUtil.SHA1_MITZ_64());
//        strategies.add(new BloomFilterStrategiesUtil.SHA256_MITZ_32());
//        strategies.add(new BloomFilterStrategiesUtil.SHA256_MITZ_64());
//        strategies.add(new BloomFilterStrategiesUtil.HMACSHA1_MITZ_32(key));
//        strategies.add(new BloomFilterStrategiesUtil.HMACSHA1_MITZ_64(key));
        strategies.add(new BloomFilterStrategiesUtil.HMACSHA256_MITZ_32(sk));
        strategies.add(new BloomFilterStrategiesUtil.HMACSHA256_MITZ_64(sk));

        //funnel
        Funnel<Student> funnel = new Funnel<Student>() {
            public void funnel(Student from, PrimitiveSink into) {
                into.putString(Integer.toString(from.id), Charsets.UTF_8)
                        .putString(Integer.toString(from.score), Charsets.UTF_8);
            }
        };

        //测试每一种hash策略
        for(BloomFilterUtil.Strategy strategy : strategies) {

            for (int t = 0; t < testpow; t++) {
                long stime, etime;
                long putcost, judgecost;

                BloomFilterUtil<Student> bf = BloomFilterUtil.create(funnel, totalSize, fpp, strategy);

                // put
                stime = System.currentTimeMillis();
                for(Student student : data){
                    bf.put(student);
                }
                etime = System.currentTimeMillis();
                putcost = etime - stime;

                logger.info("[" + strategy.getClass().getSimpleName() + "],"
                        + " put cost: " + putcost + "ms");
            }
        }




//        String msg = "hello world!";
//        String key = "mykey";
//
//        String digist = Hashing.hmacSha256(key.getBytes()).hashObject(msg, new Funnel<String>() {
//            public void funnel(String from, PrimitiveSink into) {
//                into.putString(from, Charsets.UTF_8);
//            }
//        }).toString();
//        System.out.println(digist);

    }
}
