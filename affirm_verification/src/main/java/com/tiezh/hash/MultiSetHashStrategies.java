package com.tiezh.hash;

import com.google.common.hash.Funnel;
import com.google.common.hash.Hashing;
import com.google.common.primitives.Ints;


public class MultiSetHashStrategies {

    public static abstract class HASH_FUNCTION implements MultiSetHash.Strategy{
        @Override
        public boolean equals(Object obj) {
            return this.getClass().equals(obj.getClass());
        }
    }

    public static abstract class HASH_FUNCTION_WITHKEY implements MultiSetHash.Strategy {
        protected byte[] key;

        @Override
        public boolean equals(Object obj) {
            if(this == obj)
                return true;
            if(!(obj instanceof BloomFilterStrategiesUtil.HASH_FUNCTION_WITHKEY))
                return false;
            if(!this.getClass().equals(obj.getClass()))
                return false;
            int len = ((BloomFilterStrategiesUtil.HASH_FUNCTION_WITHKEY) obj).key.length;
            if(len != this.key.length)
                return false;
            for(int i = 0; i < len; i++){
                if(key[i] != ((BloomFilterStrategiesUtil.HASH_FUNCTION_WITHKEY) obj).key[i])
                    return false;
            }
            return true;
        }

        public byte[] getKey(){return this.key;}
    }

    /** MURMUR128 */
    public static class MURMUR128 extends HASH_FUNCTION{
        // 128-bit
        static final int SIZE = 16;

        public <T> byte[] hash(T object, Funnel<? super T> funnel) {
            return Hashing.murmur3_128().hashObject(object, funnel).asBytes();
        }

        public int getSize(){return SIZE;}
    }


    public static class SHA256 extends HASH_FUNCTION{

        static final int SIZE = 32;

        public <T> byte[] hash(T object, Funnel<? super T> funnel) {
            return Hashing.sha256().hashObject(object, funnel).asBytes();
        }

        public int getSize() {
            return SIZE;
        }
    }


    public static class MURMUR128WITHKEY extends HASH_FUNCTION_WITHKEY{
        int intkey;

        // 128-bit
        static final int SIZE = 16;

        public MURMUR128WITHKEY(byte[] key){
            this.key = key;
            intkey = Ints.fromByteArray(key);
        }

        public <T> byte[] hash(T object, Funnel<? super T> funnel) {
            return Hashing.murmur3_128(intkey).hashObject(object, funnel).asBytes();
        }

        public int getSize() {
            return SIZE;
        }
    }

    public static class HMACSHA256 extends HASH_FUNCTION_WITHKEY{
        // 256-bit
        static final int SIZE = 32;

        public HMACSHA256(byte[] key){
            this.key = key;
        }

        public <T> byte[] hash(T object, Funnel<? super T> funnel) {
            return Hashing.hmacSha256(key).hashObject(object, funnel).asBytes();
        }

        public int getSize() {
            return SIZE;
        }
    }

}
