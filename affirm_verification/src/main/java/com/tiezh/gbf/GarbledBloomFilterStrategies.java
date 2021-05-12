package com.tiezh.gbf;


import com.google.common.hash.Funnel;
import com.google.common.hash.Funnels;
import com.google.common.hash.Hashing;
import com.google.common.primitives.Longs;
import com.tiezh.hash.ApacheBase64Util;

import java.security.SecureRandom;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLongArray;

public class GarbledBloomFilterStrategies {

    /** strategy接口，定义GBF中哈希函数的行为 */
    public static abstract class HASH_FUNCTION_WITHKEY implements GarbledBloomFilter.Strategy {
        protected byte[] key;

        /** 随机数发生器 */
        protected static SecureRandom secureRandom = new SecureRandom();

        @Override
        public boolean equals(Object obj) {
            if(this == obj)
                return true;
            if(!(obj instanceof HASH_FUNCTION_WITHKEY))
                return false;
            if(!this.getClass().equals(obj.getClass()))
                return false;
            int len = ((HASH_FUNCTION_WITHKEY) obj).key.length;
            if(len != this.key.length)
                return false;
            for(int i = 0; i < len; i++){
                if(key[i] != ((HASH_FUNCTION_WITHKEY) obj).key[i])
                    return false;
            }
            return true;
        }
    }


    /** HmacSHA256 来实现两个64bit的hash函数 */
    public static class HMACSHA256_MITZ_64 extends GarbledBloomFilterStrategies.HASH_FUNCTION_WITHKEY {
        public HMACSHA256_MITZ_64(byte[] key){
            this.key = key;
        }

        @Override
        public <K, V> void put(K key, Funnel<? super K> kFunnel, Set<V> values, Funnel<? super V> vFunnel,
                               int numHashFunctions, Map<String, byte[]> grabledValues, List<byte[]> gbf, int bytesLen) {
            int size = gbf.size();

            //将集合object进行哈希, sha256
            byte[] valsHash = new byte[bytesLen];
            for(V val : values){
                byte[] hash = Hashing.sha256().hashObject(val, vFunnel).asBytes();
                xor(valsHash, hash);
            }


            //System.out.println("GarbledBloomFilterStrategies.put: valsHash: " + ApacheBase64Util.encode2String(valsHash));

            byte[] bytes = Hashing.hmacSha256(this.key).hashObject(key, kFunnel).asBytes();
            long hash1 = lowerEight(bytes);
            long hash2 = upperEight(bytes);
            String base64 = ApacheBase64Util.encode2String(bytes);

            long combinedHash = hash1;
            for (int i = 0; i < numHashFunctions; i++) {
                // Make the combined hash positive and indexable
                // 找到需要插入的位置
                int pos = (int) ((combinedHash & Integer.MAX_VALUE) % size);
                byte[] gbfBytes = gbf.get(pos);
                //若gbfBytes为null，还未初始化
                if(gbfBytes == null){
                    //产生一个随机数
                    byte[] randBytes = new byte[bytesLen];
                    secureRandom.nextBytes(randBytes);
                    // value = value xor gbf[index]
                    xor(valsHash, randBytes);
                    gbf.set(pos, randBytes);
                    //确定头一个位置用于存放value
                }else{  //若gbf已经初始化，并赋值
                    xor(valsHash, gbfBytes);
                }
                combinedHash += hash2;
            }

            //将valsHash放入grabledValues
            byte[] valsHashCopy = new byte[bytesLen];
            System.arraycopy(valsHash, 0, valsHashCopy, 0, bytesLen);
            grabledValues.put(base64, valsHashCopy);

//            System.out.println("GarbledBloomFilterStrategies.put: valsHash xor random: "
//                    + ApacheBase64Util.encode2String(valsHash));

            //对gbf中其他为null的位置设置为随机数
            for(int i = 0; i < size; i++){
                if(gbf.get(i) == null){
                    byte[] randomBytes = new byte[bytesLen];
                    secureRandom.nextBytes(randomBytes);
                    gbf.set(i, randomBytes);
                }
            }
        }

        @Override
        public <K, V> boolean mightContain(K key, Funnel<? super K> kFunnel, Set<V> values, Funnel<? super V> vFunnel,
                int numHashFunctions, Map<String, byte[]> grabledValues, List<byte[]> gbf, int bytesLen) {
            int size = gbf.size();

            //将集合object进行哈希, sha256，记录咋valsHash1
            byte[] valsHash1 = new byte[bytesLen];
            for(V val : values){
                byte[] hash = Hashing.sha256().hashObject(val, vFunnel).asBytes();
                xor(valsHash1, hash);
            }


            //从GBF中提取valsHash2
            byte[] bytes = Hashing.hmacSha256(this.key).hashObject(key, kFunnel).asBytes();
            long hash1 = lowerEight(bytes);
            long hash2 = upperEight(bytes);
            String base64 = ApacheBase64Util.encode2String(bytes);

            long combinedHash = hash1;

            byte[] valsHash2 = new byte[bytesLen];
            for (int i = 0; i < numHashFunctions; i++) {
                // Make the combined hash positive and indexable
                // 找到需要插入的位置
                int pos = (int) ((combinedHash & Integer.MAX_VALUE) % size);
                byte[] gbfBytes = gbf.get(pos);
                xor(valsHash2, gbfBytes);
                combinedHash += hash2;
            }

            xor(valsHash2, grabledValues.get(base64));

//            System.out.println("GarbledBloomFilterStrategies.mightContain: valsHash1 = "
//                    + ApacheBase64Util.encode2String(valsHash1));
//            System.out.println("GarbledBloomFilterStrategies.mightContain: valsHash2 = "
//                    + ApacheBase64Util.encode2String(valsHash2));

            //检查是否 value1 == value2
            for(int i = 0; i < bytesLen; i++){
                if(valsHash1[i] != valsHash2[i])
                    return false;
            }
            return true;
        }
    }


    static long lowerEight(byte[] bytes) {
        return Longs.fromBytes(
                bytes[7], bytes[6], bytes[5], bytes[4], bytes[3], bytes[2], bytes[1], bytes[0]);
    }

    static long upperEight(byte[] bytes) {
        return Longs.fromBytes(
                bytes[15], bytes[14], bytes[13], bytes[12], bytes[11], bytes[10], bytes[9], bytes[8]);
    }

    static void xor(byte[] bytes1, byte[] bytes2){
        int len = (bytes1.length <= bytes2.length)? bytes1.length : bytes2.length;
        for(int i = 0; i < len; i++){
            bytes1[i] = (byte) (bytes1[i] ^ bytes2[i]);
        }
    }
}
