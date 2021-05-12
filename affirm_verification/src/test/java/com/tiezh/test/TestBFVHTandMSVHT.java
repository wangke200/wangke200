package com.tiezh.test;

import com.google.common.base.Charsets;
import com.google.common.hash.Funnel;
import com.google.common.hash.PrimitiveSink;
import com.tiezh.hash.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.*;

public class TestBFVHTandMSVHT {
    public static void main(String[] args) throws Exception {

        byte[] sk = "mysecretkey".getBytes();
        String key = "key";
        String csvPath = "./doc_4000.csv";
        double fpp = 0.0000001;
        int testpow = 5;        //测试次数。第一次创建对象时要加载类对象，需要进行多轮测试直到稳定
        int mergedSize = 0;

        // 设置参数
        // 参数：[csvPath] [fpp] [mergeSize]

        if(args.length == 0){
            System.out.println("need args: [csvPath] [fpp] [mergeSize]");
            return;
        }

        if (args.length >= 1) {
            csvPath = args[0];
            File file = new File(csvPath);
            if(!file.exists()) {
                System.out.println("File not found: [" + csvPath + "]");
                return;
            }
            if(file.isDirectory()){
                System.out.println("Need not a directory: [" + csvPath + "]");
                return;
            }
        }

        //读取测试数据（提前读取，因为mergedSize需要数据大小判断是否合法）
        System.out.println("generate documents and index from " + csvPath + "...");
        List<Document> documents = Document.listFromCSV(csvPath);
        Document.Index index = Document.createIndex(documents);
        System.out.println("documents and index done");

        if(args.length >= 2){
            fpp = Double.parseDouble(args[1]);
            if(fpp <= 0 || fpp >= 1){
                System.out.println("Illegal argument: the 2th arg [fpp] should be in (0,1).");
                return;
            }
        }
        if(args.length >= 3) {
            mergedSize = Integer.parseInt(args[2]);
            int keywordNum = index.keywordSet().size();
            if(mergedSize < 1 || mergedSize > keywordNum){
                System.out.println("Illegal argument: the 3th arg [mergedSize] should be in [1," + keywordNum + "].");
                return;
            }
        }


        //计算bloom filter的两个参数，bit长度和hash function 个数
        int documentNum = documents.size();
        long optimalNumOfBits = BloomFilterUtil.optimalNumOfBits(documentNum, fpp);
        long optimalNumOfHashFunctions = BloomFilterUtil.optimalNumOfHashFunctions(documentNum, optimalNumOfBits);


        Logger logger = LoggerFactory.getLogger(Test.class);
        logger.info(">> Test VHT with Document <<");
        logger.info("insertions: " + documentNum);
        logger.info("fpp: " + fpp);
        logger.info("optimalNumOfBits: " + optimalNumOfBits);
        logger.info("optimalNumOfHashFunctions: " + optimalNumOfHashFunctions);
        logger.info("mergedSize: " + mergedSize);

        // BloomFilter hash策略
        ArrayList<BloomFilterUtil.Strategy> bfStrategies = new ArrayList<BloomFilterUtil.Strategy>();
        bfStrategies.add(new BloomFilterStrategiesUtil.MURMUR128_MITZ_32());
        bfStrategies.add(new BloomFilterStrategiesUtil.MURMUR128_MITZ_64());
        bfStrategies.add(new BloomFilterStrategiesUtil.MURMURWITHKEY128_MITZ_32(sk));
        bfStrategies.add(new BloomFilterStrategiesUtil.MURMURWITHKEY128_MITZ_64(sk));
        bfStrategies.add(new BloomFilterStrategiesUtil.HMACSHA256_MITZ_32(sk));
        bfStrategies.add(new BloomFilterStrategiesUtil.HMACSHA256_MITZ_64(sk));

        //funnel
        Funnel<Document> funnel = new Funnel<Document>() {
            public void funnel(Document from, PrimitiveSink into) {
                into.putString(from.getId(), Charsets.UTF_8);
                for(String kw : from.getKeywords())
                    into.putString(kw, Charsets.UTF_8);
            }
        };

        //测试每一种hash策略
        for(BloomFilterUtil.Strategy strategy : bfStrategies) {

            for (int t = 0; t < testpow; t++) {
                long stime, etime;
                long addcost, verifycost;
                long mergecost;

                BloomFilterVHT<String, Document> vht =
                        new BloomFilterVHT<>(funnel, documentNum, fpp, strategy);

                // add
                stime = System.currentTimeMillis();
                for(Document doc : documents){
                    Set<String> keywords = doc.getKeywords();
                    for(String kw : keywords){
                        vht.add(kw, doc);
                    }
                }
                etime = System.currentTimeMillis();
                addcost = etime - stime;

                //merge
                Set<String> mergedKeys = new HashSet<>();
                Set<String> allKeys = vht.keySet();
                //选择mergeSize个key进行合并
                Iterator<String> iterator = allKeys.iterator();
                for(int i = 0; i < mergedSize; i++){
                    mergedKeys.add(iterator.next());
                }
                stime = System.currentTimeMillis();
                BloomFilterUtil<Document> mergebf =
                        (BloomFilterUtil<Document>) vht.getMergedHash(mergedKeys);
                etime = System.currentTimeMillis();
                mergecost = etime - stime;


                //verify
                stime = System.currentTimeMillis();
                BloomFilterUtil<Document> verifybf = BloomFilterUtil.create(funnel, documentNum, fpp, strategy);
                Collection<Document> verifyData = vht.getMergedValues(mergedKeys);
                for(Document doc : verifyData){
                    verifybf.put(doc);
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

                logger.info("BloomFilter VHT with [" + strategy.getClass().getSimpleName() + "],"
                        + " add cost: " + addcost + "ms,"
                        + " merge cost: " + mergecost + "ms,"
                        + " verify cost: " + verifycost + "ms,"
                        + " verify result: " + verify);
            }
        }


        // MultiSetHash hash策略
        ArrayList<MultiSetHash.Strategy> msStrategies = new ArrayList<MultiSetHash.Strategy>();
        msStrategies.add(new MultiSetHashStrategies.MURMUR128());
        msStrategies.add(new MultiSetHashStrategies.MURMUR128WITHKEY(sk));
        msStrategies.add(new MultiSetHashStrategies.HMACSHA256(sk));

        //测试每一种hash策略
        for(MultiSetHash.Strategy strategy : msStrategies) {
            long etime, stime;
            long addcost, verifycost, mergecost, nomergeverifycost;

            for(int t = 0; t < testpow; t++){
                MultiSetHashVHT<String, Document> vht = new MultiSetHashVHT<>(funnel, strategy);

                // add
                stime = System.currentTimeMillis();
                for(Document doc : documents){
                    Set<String> keywords = doc.getKeywords();
                    for(String kw : keywords){
                        vht.add(kw, doc);
                    }
                }
                etime = System.currentTimeMillis();
                addcost = etime - stime;

                //optimal merge
                Set<String> mergedKeys = new HashSet<>();
                Set<String> allKeys = vht.keySet();
                //选择mergeSize个key进行合并
                Iterator<String> iterator = allKeys.iterator();
                for(int i = 0; i < mergedSize; i++){
                    mergedKeys.add(iterator.next());
                }
                stime = System.currentTimeMillis();
                //测试数据为Document这种可以包含多个关键字的类型时，要考虑MultiSetHash的重复问题。
                //这种情况下不应使用getMergedHash()函数，而应使用getMergedRepairHash()
//                MultiSetHash<Document> mergeHash = (MultiSetHash<Document>) vht.getMergedHash(mergedKeys);
                MultiSetHash<Document> mergeHash = (MultiSetHash<Document>) vht.getMergedRepairHash(mergedKeys);
                etime = System.currentTimeMillis();
                mergecost = etime - stime;


                //verify
                stime = System.currentTimeMillis();
                MultiSetHash<Document> verifyHash = MultiSetHash.create(funnel, strategy);
                Collection<Document> verifyData = vht.getMergedValues(mergedKeys);
                for(Document doc : verifyData){
                    verifyHash.add(doc);
                }

                byte[] mergeHashBytes = mergeHash.getBytes();
                byte[] verifyHashBytes = verifyHash.getBytes();
                boolean verify = isBytesEqual(mergeHashBytes, verifyHashBytes);
                etime = System.currentTimeMillis();
                verifycost = etime - stime;



                //no merged verify
                stime = System.currentTimeMillis();
                boolean nomergeverify = true;
                stime = System.currentTimeMillis();
                for(String mk : mergedKeys){
                    verifyHash = MultiSetHash.create(funnel, strategy);
                    verifyData = vht.getValues(mk);
                    for(Document doc : verifyData){
                        verifyHash.add(doc);
                    }
                    MultiSetHash<Document> hash = (MultiSetHash<Document>) vht.getHash(mk);
                    byte[] hashBytes = hash.getBytes();
                    verifyHashBytes = verifyHash.getBytes();
                    if(!isBytesEqual(hashBytes, verifyHashBytes)){
                        nomergeverify = false;
                        break;
                    }
                }
                etime = System.currentTimeMillis();
                nomergeverifycost = etime - stime;


                logger.info("MultiSetHash VHT with [" + strategy.getClass().getSimpleName() + "],"
                        + " add cost: " + addcost + "ms,"
                        + " merge cost: " + mergecost + "ms,"
                        + " verify cost: " + verifycost + "ms,"
                        + " verify result: " + verify + ","
                        + " no merge verify cost: " + nomergeverifycost + "ms,"
                        + " no merge verify result: " + nomergeverify);
            }
        }
    }

    public static boolean isBytesEqual(byte[] bytes1, byte[] bytes2){
        if(bytes1.length != bytes2.length)
            return false;
        boolean verify = true;
        for(int i = 0; i < bytes1.length; i++){
            if(bytes1[i] != bytes2[i]){
                System.out.println("verify error: " + i + "th block false.");
                verify = false;
                break;
            }
        }
        return verify;
    }
}