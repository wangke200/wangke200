package com.tiezh.test;

import com.google.common.base.Charsets;
import com.google.common.hash.Funnel;
import com.google.common.hash.PrimitiveSink;
import com.tiezh.hash.MultiSetHash;
import com.tiezh.hash.MultiSetHashStrategies;
import com.tiezh.hash.MultiSetHashVHT;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public class TestMultiSetHashVHT {

    public static void main(String[] args) {

        byte[] sk = "mysecretkey".getBytes();
        String key = "key";
        int totalSize = 160000;
        int idOffset = 1;
        int valBase = 60;
        int repeatNum = 1000;
        int testpow = 5;        //测试次数。第一次创建对象时要加载类对象，需要进行多轮测试直到稳定
        int mergedSize = totalSize / repeatNum;

        // 设置参数
        // 参数： [totoalSize] [fpp] [repeatNum] [mergedSize]
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
        if(args.length >= 3) {
            mergedSize = Integer.parseInt(args[2]);
            if (mergedSize < 1 || mergedSize > (totalSize / repeatNum)) {
                System.out.println("Illegal argument: the 3th arg [mergeSized] should be in [1, " + (totalSize / repeatNum) + "]");
                return;
            }
        }

        //重新检查mergedSize，如果不符合则设置为合法的最大值
        if(mergedSize < 1 || mergedSize > (totalSize / repeatNum)){
            mergedSize = totalSize / repeatNum;
        }


        Logger logger = LoggerFactory.getLogger(TestMultiSetHashVHT.class);
        logger.info(">> Test MultiSetHash VHT <<");
        logger.info("insertions: " + totalSize);
        logger.info("repeatNum: " + repeatNum);
        logger.info("mergeSize: " + mergedSize);

        // 测试对象
        List<Student> data = Student.generateDatas(totalSize, idOffset, valBase, repeatNum);

        //测试hash策略
        ArrayList<MultiSetHash.Strategy> strategies = new ArrayList<>();
        strategies.add(new MultiSetHashStrategies.MURMUR128());
        strategies.add(new MultiSetHashStrategies.MURMUR128WITHKEY(sk));
//        strategies.add(new MultiSetHashStrategies.SHA256());
        strategies.add(new MultiSetHashStrategies.HMACSHA256(sk));

        //定义过滤规则funnel（不处理）
        Funnel<Student> funnel = new Funnel<Student> (){
            @Override
            public void funnel(Student from, PrimitiveSink into) {
                into.putString(Integer.toString(from.id), Charsets.UTF_8)
                    .putString(Integer.toString(from.score), Charsets.UTF_8);
            }
        };

        for(MultiSetHash.Strategy strategy : strategies) {
            long etime, stime;
            long addcost, verifycost, mergecost;

            for(int t = 0; t < testpow; t++){
                MultiSetHashVHT<Integer, Student> vht = new MultiSetHashVHT<>(funnel, strategy);

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
                MultiSetHash<Student> mergeHash = (MultiSetHash<Student>) vht.getMergedHash(mergedKeys);
                etime = System.currentTimeMillis();
                mergecost = etime - stime;


                //verify
                stime = System.currentTimeMillis();
                MultiSetHash<Student> verifyHash = MultiSetHash.create(funnel, strategy);
                Collection<Student> verifyData = vht.getMergedValues(mergedKeys);
                for(Student student : verifyData){
                    verifyHash.add(student);
                }
                boolean verify = true;

                byte[] mergeHashBytes = mergeHash.getBytes();
                byte[] verifyHashBytes = verifyHash.getBytes();
                for(int i = 0; i < mergeHashBytes.length; i++){
                    if(mergeHashBytes[i] != verifyHashBytes[i]){
                        System.out.println("verify error: " + i + "th block false.");
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

//            //合并
//            Set<Integer> keySet = vht.keySet();
//            for(int ){}
        }

    }
}
