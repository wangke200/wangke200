package com.tiezh.test;

import com.google.common.base.Charsets;
import com.google.common.hash.Funnel;
import com.google.common.hash.PrimitiveSink;
import com.tiezh.hash.BloomFilterStrategiesUtil;
import com.tiezh.hash.BloomFilterUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;

public class TestBloomFilterMerge {
    public static void main(String[] args) {

        Logger logger = LoggerFactory.getLogger(Test.class);
        logger.info(">> Test BloomFilter Merge <<");

        int expectedInsertions = 200000;
        double fpp = 0.0000001;
        byte[] key = "mykey".getBytes();
        int testpow = 5;

        long optimalNumOfBits = BloomFilterUtil.optimalNumOfBits(expectedInsertions, fpp);
        long optimalNumOfHashFunctions = BloomFilterUtil.optimalNumOfHashFunctions(expectedInsertions, optimalNumOfBits);

        logger.info("insertions: " + expectedInsertions);
        logger.info("fpp: " + fpp);
        logger.info("optimalNumOfBits: " + optimalNumOfBits);
        logger.info("optimalNumOfHashFunctions: " + optimalNumOfHashFunctions);

        ArrayList<BloomFilterUtil.Strategy> strategies = new ArrayList<BloomFilterUtil.Strategy>();
//        strategies.add(new BloomFilterStrategiesUtil.MURMUR128_MITZ_32());
//        strategies.add(new BloomFilterStrategiesUtil.MURMUR128_MITZ_64());
        strategies.add(new BloomFilterStrategiesUtil.MURMURWITHKEY128_MITZ_32(key));
        strategies.add(new BloomFilterStrategiesUtil.MURMURWITHKEY128_MITZ_64(key));
//        strategies.add(new BloomFilterStrategiesUtil.SHA1_MITZ_32());
//        strategies.add(new BloomFilterStrategiesUtil.SHA1_MITZ_64());
//        strategies.add(new BloomFilterStrategiesUtil.SHA256_MITZ_32());
//        strategies.add(new BloomFilterStrategiesUtil.SHA256_MITZ_64());
//        strategies.add(new BloomFilterStrategiesUtil.HMACSHA1_MITZ_32(key));
//        strategies.add(new BloomFilterStrategiesUtil.HMACSHA1_MITZ_64(key));
        strategies.add(new BloomFilterStrategiesUtil.HMACSHA256_MITZ_32(key));
        strategies.add(new BloomFilterStrategiesUtil.HMACSHA256_MITZ_64(key));


        //测试每一种hash策略
        for(BloomFilterUtil.Strategy strategy : strategies) {

            for (int t = 0; t < testpow; t++) {

                Funnel<String> funnel = new Funnel<String>() {
                    public void funnel(String from, PrimitiveSink into) {
                        into.putString(from, Charsets.UTF_8);
                    }
                };
                BloomFilterUtil<String> bf1 = BloomFilterUtil.create(funnel, expectedInsertions, fpp, strategy);
                BloomFilterUtil<String> bf2 = BloomFilterUtil.create(funnel, expectedInsertions, fpp, strategy);

                long stime, etime;
                long putcost, judgecost, mergecost;

                //put数据
                stime = System.currentTimeMillis();
                for(int i = 0; i < expectedInsertions / 2; i++){
                    bf1.put(Integer.toString(i));
                    bf2.put(Integer.toString(i + expectedInsertions / 2));
                }
                etime = System.currentTimeMillis();
                putcost = etime - stime;

                //合并bloom filter
                BloomFilterUtil<String> bf3 = null;
                stime = System.currentTimeMillis();
                try {
                    bf3 = BloomFilterUtil.merge(bf1, bf2);
                } catch (Exception e) {
                    e.printStackTrace();
                }
                etime = System.currentTimeMillis();
                mergecost = etime - stime;

                //测试假阳率
                int countfp = 0;
                stime = System.currentTimeMillis();
                for(int i = expectedInsertions; i < expectedInsertions * 2; i++){
                    if(bf3.mightContain(Integer.toString(i))){
                        countfp++;
                    }
                }
                etime = System.currentTimeMillis();
                judgecost = etime - stime;

                logger.info("[" + strategy.getClass().getSimpleName() + "],"
                        + " error num: " + countfp + "/" + expectedInsertions + ","
                        + " put cost: " + putcost + "ms,"
                        + " merge cost: " + mergecost + "ms,"
                        + " judge cost: " + judgecost + "ms");
            }
        }
    }
}
