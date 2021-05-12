package com.tiezh.test;

import com.google.common.base.Charsets;
import com.google.common.hash.Funnel;
import com.google.common.hash.PrimitiveSink;
import com.tiezh.affirm.AFFIRMVHT;
import com.tiezh.hash.MultiSetHash;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public class TestAFFIRM {
    public static void  main(String[] args) {
        byte[] sk = "mysecretkey".getBytes();
        String key = "key";
        int totalSize = 160000;
        int repeatNum = 1000;
        int matchedNum = 10 * repeatNum;
        int idOffset = 1;
        int valBase = 60;
        int testTime = 5;        //测试次数。第一次创建对象时要加载类对象，需要进行多轮测试直到稳定
        int updateNum = 0;

        // 设置参数
        // 参数： [totalSize] [repeatNum] [matchedNum] [testTime] [updateNum]
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
        if (args.length >= 3) {
            matchedNum = Integer.parseInt(args[2]);
            if (matchedNum  % repeatNum != 0 || matchedNum > totalSize) {
                System.out.println("Illegal argument: the 3th arg [matchedNum]"
                        + " should be multiple of [repeatNum] and smaller than [totalSize]");
                return;
            }
        }
        if(args.length >= 4) {
            testTime = Integer.parseInt(args[3]);
            if (testTime < 1) {
                System.out.println("Illegal argument: the 4th arg [testTime] should be more than 1");
                return;
            }
        }

        if(args.length >= 5) {
            updateNum = Integer.parseInt(args[3]);
            if (testTime < 0) {
                System.out.println("Illegal argument: the 5th arg [updateNum] should be not a negative");
                return;
            }
        }

        Logger logger = LoggerFactory.getLogger(TestAFFIRM.class);
        logger.info(">> Test AFFIRM VHT <<");
        logger.info("insertions: " + totalSize);
        logger.info("repeatNum: " + repeatNum);
        logger.info("matchedNum: " + matchedNum);
        logger.info("testTime: " + testTime);
        logger.info("updateNum: " + updateNum);

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
        long addcost = 0, verifycostExt = 0, verifycostRng = 0;

        AFFIRMVHT affirmVHT = AFFIRMVHT.create("mykey");
        stime = System.currentTimeMillis();
        affirmVHT.build(ids, "score", scores, "age", ages);
        etime = System.currentTimeMillis();
        addcost += etime - stime;

        boolean verifyResultExt = true;
        boolean verifyResultRng = true;

        for(int i = 0; i < testTime; i++){
            String cvVar = scores.get(i);
            int score = Integer.parseInt(cvVar);

            //Ext模式，通过明文索引模拟搜索结果
            Set<Student> result = index.getThroughInv(score);
            List<String> rs = Student.getIdList(result);
            LinkedList<String> crVals = Student.getAgeList(result);

            stime = System.nanoTime();
            //Ext模式，产生验证令牌
            AFFIRMVHT.GetToken token = affirmVHT.getToken(cvName, cvVar);
            //Ext模式，通过令牌获取证明信息
            MultiSetHash multiSetHash = affirmVHT.getMH(token);

            //Ext模式，利用明文搜索结果和证明信息进行验证
            boolean pass = affirmVHT.verify(rs, crName, crVals, multiSetHash.getBytes());
            etime = System.nanoTime();
            verifycostExt += etime - stime;
            verifyResultExt &= pass;

            //Rng模式，通过明文索引模拟搜索结果
            result = new HashSet<>();
            score = valBase;
            for(int j = 0; j < (matchedNum / repeatNum); j++){
                result.addAll(index.getThroughInv(score));
                score++;
            }


            rs = Student.getIdList(result);
            crVals = Student.getAgeList(result);

            stime = System.nanoTime();
            //Rng模式，产生验证令牌
            List<AFFIRMVHT.GetToken> tokens = affirmVHT.getToken(cvName, cvVar, true);
            //Ext模式，通过令牌获取证明信息
            multiSetHash = affirmVHT.getMH(tokens);
            //Ext模式，利用明文搜索结果和证明信息进行验证
            pass = affirmVHT.verify(rs, crName, crVals, multiSetHash.getBytes());
            etime = System.nanoTime();
            verifycostRng += etime - stime;
            verifyResultRng &= pass;
        }

        logger.info("[HMAC256,SHA256] add cost: " + addcost + "ms,"
                + " ext verify: " + verifyResultExt + ","
                + " ext verify cost: " + (verifycostExt / 1000000) + "ms, "
                + " rng verify: " + verifyResultRng + ","
                + " rng verify cost: " + (verifycostRng / 1000000) + "ms ");


    }
}
