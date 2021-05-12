package com.tiezh.test;

import com.google.common.base.Charsets;
import com.google.common.hash.Funnel;
import com.google.common.hash.PrimitiveSink;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import pku.Accumulator;

import java.math.BigInteger;
import java.security.NoSuchAlgorithmException;
import java.util.*;

public class TestRSAAcc {
    public static void  main(String[] args) throws NoSuchAlgorithmException {
        byte[] sk = "mysecretkey".getBytes();
        String key = "key";
        int totalSize = 160000;
        int repeatNum = 100;
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
                System.out.println("Illegal argument: the 3th arg [testTime] should be more than 1");
                return;
            }
        }

        Logger logger = LoggerFactory.getLogger(TestRSAAcc.class);
        logger.info(">> Test RSA Accumulator <<");
        logger.info("insertions: " + totalSize);
        logger.info("repeatNum: " + repeatNum);
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

        //funnel
        Funnel<Student> funnel = new Funnel<Student>() {
            public void funnel(Student from, PrimitiveSink into) {
                into.putString(Integer.toString(from.id), Charsets.UTF_8)
                        .putString(Integer.toString(from.score), Charsets.UTF_8);
            }
        };


        long etime, stime;
        long addcost = 0, verifycost = 0;

        //建立验RSA accumulator表，每一个‘cv||v’对应一个accumulator
        stime = System.currentTimeMillis();
        Set<Integer> scoreSet = index.scoreSet();
        Map<String, Accumulator> accMap = new HashMap<>();
        for(Integer score : scoreSet){
            Accumulator acc = new Accumulator();
            accMap.put(cvName + "||" + score, acc);
        }
        //对RSA acc表中行累加
        for(int i = 0; i < ids.size(); i++){
            String r = ids.get(i);
            String crVal = ages.get(i);
            String rv = r + "||" + crName + "||" + crVal;

            String cvVal = scores.get(i);
            Accumulator acc = accMap.get(cvName + "||" + cvVal);
            acc.add(rv);
        }
        etime = System.currentTimeMillis();
        addcost += etime - stime;


        boolean verifyResult = true;

        for(int i = 0; i < testTime; i++){
            String cvVal = scores.get(i);
            int score = Integer.parseInt(cvVal);

            //通过明文索引模拟搜索结果
            Set<Student> result = index.getThroughInv(score);
            List<String> rvs = Student.getRVList(result);

            stime = System.nanoTime();

            //RSA accumulator 验证
            Accumulator acc = accMap.get(cvName + "||" + cvVal);
            BigInteger A = acc.getA();
            BigInteger N = acc.getN();

            BigInteger proof = acc.proveMembership(rvs);
            List<BigInteger> nonces = acc.getNonce(rvs);

            boolean pass = Accumulator.verifyMembership(A, rvs, nonces, proof, N);
            etime = System.nanoTime();
            verifycost += etime - stime;
            verifyResult &= pass;

        }

        logger.info("[RSA accumulator] add cost: " + addcost + "ms,"
                + " verify: " + verifyResult + ","
                + " verify cost: " + (verifycost / 1000000) + "ms ");


    }
}
