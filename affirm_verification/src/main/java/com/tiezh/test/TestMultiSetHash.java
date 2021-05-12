package com.tiezh.test;

import com.google.common.base.Charsets;
import com.google.common.hash.Funnel;
import com.google.common.hash.PrimitiveSink;
import com.tiezh.hash.MultiSetHash;
import com.tiezh.hash.MultiSetHashStrategies;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

public class TestMultiSetHash {
    public static void  main(String[] args){
        byte[] sk = "mysecretkey".getBytes();
        String key = "key";
        int totalSize = 160000;
        int idOffset = 1;
        int valBase = 60;
        int repeatNum = 1000;
        int testpow = 5;        //测试次数。第一次创建对象时要加载类对象，需要进行多轮测试直到稳定

        // 设置参数
        // 参数： [totoalSize] [repeatNum]
        if(args.length >= 1){
            totalSize = Integer.parseInt(args[0]);
        }
        if(args.length >= 2) {
            repeatNum = Integer.parseInt(args[1]);
            if (repeatNum < 1 || repeatNum > totalSize) {
                System.out.println("Illegal argument: the 2th arg [repeatNum] should be in [1, " + totalSize + "]");
                return;
            }
        }

        Logger logger = LoggerFactory.getLogger(TestMultiSetHash.class);
        logger.info(">> Test MultiSetHash <<");
        logger.info("insertions: " + totalSize);
        logger.info("repeatNum: " + repeatNum);

        // 测试对象
        List<Student> data = Student.generateDatas(totalSize, idOffset, valBase, repeatNum);

        //strategies
        ArrayList<MultiSetHash.Strategy> strategies = new ArrayList<MultiSetHash.Strategy>();
        strategies.add(new MultiSetHashStrategies.MURMUR128());
        strategies.add(new MultiSetHashStrategies.MURMUR128WITHKEY(sk));
//        strategies.add(new MultiSetHashStrategies.SHA256());
        strategies.add(new MultiSetHashStrategies.HMACSHA256(sk));

        //funnel
        Funnel<Student> funnel = new Funnel<Student>() {
            public void funnel(Student from, PrimitiveSink into) {
                into.putString(Integer.toString(from.id), Charsets.UTF_8)
                        .putString(Integer.toString(from.score), Charsets.UTF_8);
            }
        };

        for(MultiSetHash.Strategy strategy : strategies){
            long etime, stime;
            long addcost, verifycost;

            for(int t = 0; t < testpow; t++){

                MultiSetHash<Student> multiSetHash = MultiSetHash.create(funnel, strategy);
                stime = System.currentTimeMillis();
                for(Student student : data){
                    multiSetHash.add(student);
                }
                etime = System.currentTimeMillis();
                addcost = etime - stime;


                logger.info("[" + strategy.getClass().getSimpleName() + "],"
                        + " add cost: " + addcost + "ms");

            }

        }
    }
}
