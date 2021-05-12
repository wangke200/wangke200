package com.tiezh.test;

import com.google.common.base.Charsets;
import com.google.common.hash.Funnel;
import com.google.common.hash.Funnels;
import com.google.common.hash.PrimitiveSink;
import com.tiezh.gbf.GarbledBloomFilter;
import com.tiezh.gbf.GarbledBloomFilterStrategies;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import pku.Accumulator;

;import java.math.BigInteger;
import java.util.*;

public class TestGBF {
    public static void main(String[] args) {
        byte[] sk = "mysecretkey".getBytes();
        int totalSize = 160000;
        int repeatNum = 1000;
        int idOffset = 1;
        int valBase = 60;
        int testTime = 5;        //测试次数。第一次创建对象时要加载类对象，需要进行多轮测试直到稳定

        // 设置参数
        // 参数： [totalSize] [repeatNum] [testTime]
        if (args.length >= 1) {
            totalSize = Integer.parseInt(args[0]);
        }
        if (args.length >= 2) {
            repeatNum = Integer.parseInt(args[1]);
            if (repeatNum < 1 || repeatNum > totalSize) {
                System.out.println("Illegal argument: the 2th arg [repeatNum] should be in [1, " + totalSize + "]");
                return;
            }
        }
        if(args.length >= 3) {
            testTime = Integer.parseInt(args[2]);
            if (testTime < 1) {
                System.out.println("Illegal argument: the 4th arg [testTime] should be more than 1");
                return;
            }
        }


        //strategy 为HMac256
        GarbledBloomFilter.Strategy strategy = new GarbledBloomFilterStrategies.HMACSHA256_MITZ_64(sk);

        //string funnel
        Funnel<CharSequence> strFunnel = Funnels.stringFunnel(Charsets.UTF_8);

        //student funnel
        Funnel<Student> stuFunnel = new Funnel<Student>() {
            @Override
            public void funnel(Student from, PrimitiveSink into) {
                into.putString(from.toString(), Charsets.UTF_8);
            }
        };

        // new 一个GBF对象
        GarbledBloomFilter<String, Student> gbf = GarbledBloomFilter.create(totalSize, strFunnel, stuFunnel, strategy);


        Logger logger = LoggerFactory.getLogger(TestGBF.class);
        logger.info(">> Test Garbled Bloom Filter <<");
        logger.info("insertions: " + totalSize);
        logger.info("repeatNum: " + repeatNum);
        logger.info("numHashFunctions: " + gbf.getNumHashFunctions());
        logger.info("GBF size: " + gbf.getSize());
        logger.info("testTime: " + testTime);


        // 测试对象
        List<Student> data = Student.generateDatas(totalSize, idOffset, valBase, repeatNum);
        //建立明文索引
        Student.Index index = Student.createIndex(data);

        //为了安全建立索引，将数据拆分成(cv,cvVals,cr,crVals)
        List<String> ids = Student.getIdList(data);
        List<String> scores = Student.getScoreList(data);
        LinkedList<String> ages = Student.getAgeList(data);
        String cvName = "score";
        String crName = "age";


        long stime, etime;
        long addcost = 0, verifycost = 0;

        //建立验GBF表
        stime = System.currentTimeMillis();
        Set<Integer> scoreSet = index.scoreSet();
        for(Integer score : scoreSet){
            Set<Student> stuSet = index.getThroughInv(score);
            gbf.put(Integer.toString(score), stuSet);
        }
        etime = System.currentTimeMillis();
        addcost = etime - stime;



        boolean verifyResult = true;

        for(int i = 0; i < testTime; i++){
            String cvVal = scores.get(i);
            int score = Integer.parseInt(cvVal);

            //通过明文索引模拟搜索结果
            Set<Student> result = index.getThroughInv(score);

            stime = System.nanoTime();

            //GBF 验证
            boolean pass = gbf.contain(cvVal, result);
            etime = System.nanoTime();
            verifycost += etime - stime;
            verifyResult &= pass;

        }

        logger.info("[" + strategy.getClass().getSimpleName() + "] add cost: " + addcost + "ms,"
                + " verify: " + verifyResult + ","
                + " verify cost: " + (verifycost / 1000000) + "ms ");

    }
}
