package com.tiezh.hash;

import com.google.common.hash.*;
import com.google.common.primitives.Ints;
import com.google.common.primitives.Longs;

import java.util.concurrent.atomic.AtomicLongArray;

public class BloomFilterStrategiesUtil{

    private static final int LONG_ADDRESSABLE_BITS = 6;

    public static abstract class HASH_FUNCTION implements BloomFilterUtil.Strategy{
        @Override
        public boolean equals(Object obj) {
            return this.getClass().equals(obj.getClass());
        }
    }

    public static abstract class HASH_FUNCTION_WITHKEY implements BloomFilterUtil.Strategy {
        protected byte[] key;

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


    /** HmacSHA1 来实现两个32bit的hash函数 */
    public static class HMACSHA1_MITZ_32 extends HASH_FUNCTION_WITHKEY{
        public HMACSHA1_MITZ_32(byte[] key){
            this.key = key;
        }

        public <T> boolean put(T object, Funnel<? super T> funnel, int numHashFunctions, AtomicLongArray bits) {
            long bitSize = bits.length() * Long.SIZE;
            long hash64 = Hashing.hmacSha1(this.key).hashObject(object, funnel).asLong();
            int hash1 = (int) hash64;
            int hash2 = (int) (hash64 >>> 32);

            boolean bitsChanged = false;
            for (int i = 1; i <= numHashFunctions; i++) {
                int combinedHash = hash1 + (i * hash2);
                // Flip all the bits if it's negative (guaranteed positive number)
                if (combinedHash < 0) {
                    combinedHash = ~combinedHash;
                }
                bitsChanged |= set(bits, combinedHash % bitSize);
            }
            return bitsChanged;
        }

        public <T> boolean mightContain(T object, Funnel<? super T> funnel, int numHashFunctions, AtomicLongArray bits) {
            long bitSize = bits.length() * Long.SIZE;
            long hash64 = Hashing.hmacSha1(this.key).hashObject(object, funnel).asLong();
            int hash1 = (int) hash64;
            int hash2 = (int) (hash64 >>> 32);

            for (int i = 1; i <= numHashFunctions; i++) {
                int combinedHash = hash1 + (i * hash2);
                // Flip all the bits if it's negative (guaranteed positive number)
                if (combinedHash < 0) {
                    combinedHash = ~combinedHash;
                }
                if (!get(bits, combinedHash % bitSize)) {
                    return false;
                }
            }
            return true;
        }
    }


    /** HmacSHA1 来实现两个64bit的hash函数 */
    public static class HMACSHA1_MITZ_64 extends HASH_FUNCTION_WITHKEY{
        public HMACSHA1_MITZ_64(byte[] key){
            this.key = key;
        }

        public <T> boolean put(T object, Funnel<? super T> funnel, int numHashFunctions, AtomicLongArray bits) {
            long bitSize = bits.length() * Long.SIZE;
            byte[] bytes = Hashing.hmacSha1(this.key).hashObject(object, funnel).asBytes();
            long hash1 = lowerEight(bytes);
            long hash2 = upperEight(bytes);

            boolean bitsChanged = false;
            long combinedHash = hash1;
            for (int i = 0; i < numHashFunctions; i++) {
                // Make the combined hash positive and indexable
                bitsChanged |= set(bits, (combinedHash & Long.MAX_VALUE) % bitSize);
                combinedHash += hash2;
            }
            return bitsChanged;
        }

        public <T> boolean mightContain(T object, Funnel<? super T> funnel, int numHashFunctions, AtomicLongArray bits) {
            long bitSize = bits.length() * Long.SIZE;
            byte[] bytes = Hashing.hmacSha1(this.key).hashObject(object, funnel).asBytes();
            long hash1 = lowerEight(bytes);
            long hash2 = upperEight(bytes);

            long combinedHash = hash1;
            for (int i = 0; i < numHashFunctions; i++) {
                // Make the combined hash positive and indexable
                if (!get(bits, (combinedHash & Long.MAX_VALUE) % bitSize)) {
                    return false;
                }
                combinedHash += hash2;
            }
            return true;
        }
    }

    /** HmacSHA256 来实现两个32bit的hash函数 */
    public static class HMACSHA256_MITZ_32 extends HASH_FUNCTION_WITHKEY{
        public HMACSHA256_MITZ_32(byte[] key){
            this.key = key;
        }

        public <T> boolean put(T object, Funnel<? super T> funnel, int numHashFunctions, AtomicLongArray bits) {
            long bitSize = bits.length() * Long.SIZE;
            long hash64 = Hashing.hmacSha256(this.key).hashObject(object, funnel).asLong();
            int hash1 = (int) hash64;
            int hash2 = (int) (hash64 >>> 32);

            boolean bitsChanged = false;
            for (int i = 1; i <= numHashFunctions; i++) {
                int combinedHash = hash1 + (i * hash2);
                // Flip all the bits if it's negative (guaranteed positive number)
                if (combinedHash < 0) {
                    combinedHash = ~combinedHash;
                }
                bitsChanged |= set(bits, combinedHash % bitSize);
            }
            return bitsChanged;
        }

        public <T> boolean mightContain(T object, Funnel<? super T> funnel, int numHashFunctions, AtomicLongArray bits) {
            long bitSize = bits.length() * Long.SIZE;
            long hash64 = Hashing.hmacSha256(this.key).hashObject(object, funnel).asLong();
            int hash1 = (int) hash64;
            int hash2 = (int) (hash64 >>> 32);

            for (int i = 1; i <= numHashFunctions; i++) {
                int combinedHash = hash1 + (i * hash2);
                // Flip all the bits if it's negative (guaranteed positive number)
                if (combinedHash < 0) {
                    combinedHash = ~combinedHash;
                }
                if (!get(bits, combinedHash % bitSize)) {
                    return false;
                }
            }
            return true;
        }
    }


    /** HmacSHA256 来实现两个64bit的hash函数 */
    public static class HMACSHA256_MITZ_64 extends HASH_FUNCTION_WITHKEY{
        public HMACSHA256_MITZ_64(byte[] key){
            this.key = key;
        }

        public <T> boolean put(T object, Funnel<? super T> funnel, int numHashFunctions, AtomicLongArray bits) {
            long bitSize = bits.length() * Long.SIZE;
            byte[] bytes = Hashing.hmacSha256(this.key).hashObject(object, funnel).asBytes();
            long hash1 = lowerEight(bytes);
            long hash2 = upperEight(bytes);

            boolean bitsChanged = false;
            long combinedHash = hash1;
            for (int i = 0; i < numHashFunctions; i++) {
                // Make the combined hash positive and indexable
                bitsChanged |= set(bits, (combinedHash & Long.MAX_VALUE) % bitSize);
                combinedHash += hash2;
            }
            return bitsChanged;
        }

        public <T> boolean mightContain(T object, Funnel<? super T> funnel, int numHashFunctions, AtomicLongArray bits) {
            long bitSize = bits.length() * Long.SIZE;
            byte[] bytes = Hashing.hmacSha256(this.key).hashObject(object, funnel).asBytes();
            long hash1 = lowerEight(bytes);
            long hash2 = upperEight(bytes);

            long combinedHash = hash1;
            for (int i = 0; i < numHashFunctions; i++) {
                // Make the combined hash positive and indexable
                if (!get(bits, (combinedHash & Long.MAX_VALUE) % bitSize)) {
                    return false;
                }
                combinedHash += hash2;
            }
            return true;
        }
    }

    /** MURMUR128 来实现两个32bit的hash函数 */
    public static class MURMUR128_MITZ_32 extends HASH_FUNCTION{

        public <T> boolean put(T object, Funnel<? super T> funnel, int numHashFunctions, AtomicLongArray bits) {
                long bitSize = bits.length() * Long.SIZE;
                long hash64 = Hashing.murmur3_128().hashObject(object, funnel).asLong();
                int hash1 = (int) hash64;
                int hash2 = (int) (hash64 >>> 32);

                boolean bitsChanged = false;
                for (int i = 1; i <= numHashFunctions; i++) {
                    int combinedHash = hash1 + (i * hash2);
                    // Flip all the bits if it's negative (guaranteed positive number)
                    if (combinedHash < 0) {
                        combinedHash = ~combinedHash;
                    }
                    bitsChanged |= set(bits, combinedHash % bitSize);
                }
                return bitsChanged;
        }

        public <T> boolean mightContain(T object, Funnel<? super T> funnel, int numHashFunctions, AtomicLongArray bits) {
            long bitSize = bits.length() * Long.SIZE;
            long hash64 = Hashing.murmur3_128().hashObject(object, funnel).asLong();
            int hash1 = (int) hash64;
            int hash2 = (int) (hash64 >>> 32);

            for (int i = 1; i <= numHashFunctions; i++) {
                int combinedHash = hash1 + (i * hash2);
                // Flip all the bits if it's negative (guaranteed positive number)
                if (combinedHash < 0) {
                    combinedHash = ~combinedHash;
                }
                if (!get(bits, combinedHash % bitSize)) {
                    return false;
                }
            }
            return true;
        }
    }

    /** MURMUR128 来实现两个64bit的hash函数 */
    public static class MURMUR128_MITZ_64 extends HASH_FUNCTION{
        public <T> boolean put(T object, Funnel<? super T> funnel, int numHashFunctions, AtomicLongArray bits) {
            long bitSize = bits.length() * Long.SIZE;
            byte[] bytes = Hashing.murmur3_128().hashObject(object, funnel).asBytes();
            long hash1 = lowerEight(bytes);
            long hash2 = upperEight(bytes);

            boolean bitsChanged = false;
            long combinedHash = hash1;
            for (int i = 0; i < numHashFunctions; i++) {
                // Make the combined hash positive and indexable
                bitsChanged |= set(bits, (combinedHash & Long.MAX_VALUE) % bitSize);
                combinedHash += hash2;
            }
            return bitsChanged;
        }

        public <T> boolean mightContain(T object, Funnel<? super T> funnel, int numHashFunctions, AtomicLongArray bits) {
            long bitSize = bits.length() * Long.SIZE;
            byte[] bytes = Hashing.murmur3_128().hashObject(object, funnel).asBytes();
            long hash1 = lowerEight(bytes);
            long hash2 = upperEight(bytes);

            long combinedHash = hash1;
            for (int i = 0; i < numHashFunctions; i++) {
                // Make the combined hash positive and indexable
                if (!get(bits, (combinedHash & Long.MAX_VALUE) % bitSize)) {
                    return false;
                }
                combinedHash += hash2;
            }
            return true;
        }
    }


    /** MURMUR128 with key 来实现两个32bit的hash函数 */
    public static class MURMURWITHKEY128_MITZ_32 extends HASH_FUNCTION_WITHKEY{
        int intkey;

        public MURMURWITHKEY128_MITZ_32(byte[] key) {
            this.key = key;
            intkey = Ints.fromByteArray(key);
        }

        public <T> boolean put(T object, Funnel<? super T> funnel, int numHashFunctions, AtomicLongArray bits) {
            long bitSize = bits.length() * Long.SIZE;
            long hash64 = Hashing.murmur3_128(intkey).hashObject(object, funnel).asLong();
            int hash1 = (int) hash64;
            int hash2 = (int) (hash64 >>> 32);

            boolean bitsChanged = false;
            for (int i = 1; i <= numHashFunctions; i++) {
                int combinedHash = hash1 + (i * hash2);
                // Flip all the bits if it's negative (guaranteed positive number)
                if (combinedHash < 0) {
                    combinedHash = ~combinedHash;
                }
                bitsChanged |= set(bits, combinedHash % bitSize);
            }
            return bitsChanged;
        }

        public <T> boolean mightContain(T object, Funnel<? super T> funnel, int numHashFunctions, AtomicLongArray bits) {
            long bitSize = bits.length() * Long.SIZE;
            long hash64 = Hashing.murmur3_128(intkey).hashObject(object, funnel).asLong();
            int hash1 = (int) hash64;
            int hash2 = (int) (hash64 >>> 32);

            for (int i = 1; i <= numHashFunctions; i++) {
                int combinedHash = hash1 + (i * hash2);
                // Flip all the bits if it's negative (guaranteed positive number)
                if (combinedHash < 0) {
                    combinedHash = ~combinedHash;
                }
                if (!get(bits, combinedHash % bitSize)) {
                    return false;
                }
            }
            return true;
        }
    }

    /** MURMUR128 来实现两个64bit的hash函数 */
    public static class MURMURWITHKEY128_MITZ_64 extends HASH_FUNCTION_WITHKEY{
        int intkey;

        public MURMURWITHKEY128_MITZ_64(byte[] key) {
            this.key = key;
            intkey = Ints.fromByteArray(key);
        }

        public <T> boolean put(T object, Funnel<? super T> funnel, int numHashFunctions, AtomicLongArray bits) {
            long bitSize = bits.length() * Long.SIZE;
            byte[] bytes = Hashing.murmur3_128(intkey).hashObject(object, funnel).asBytes();
            long hash1 = lowerEight(bytes);
            long hash2 = upperEight(bytes);

            boolean bitsChanged = false;
            long combinedHash = hash1;
            for (int i = 0; i < numHashFunctions; i++) {
                // Make the combined hash positive and indexable
                bitsChanged |= set(bits, (combinedHash & Long.MAX_VALUE) % bitSize);
                combinedHash += hash2;
            }
            return bitsChanged;
        }

        public <T> boolean mightContain(T object, Funnel<? super T> funnel, int numHashFunctions, AtomicLongArray bits) {
            long bitSize = bits.length() * Long.SIZE;
            byte[] bytes = Hashing.murmur3_128(intkey).hashObject(object, funnel).asBytes();
            long hash1 = lowerEight(bytes);
            long hash2 = upperEight(bytes);

            long combinedHash = hash1;
            for (int i = 0; i < numHashFunctions; i++) {
                // Make the combined hash positive and indexable
                if (!get(bits, (combinedHash & Long.MAX_VALUE) % bitSize)) {
                    return false;
                }
                combinedHash += hash2;
            }
            return true;
        }
    }

    /** SHA1 来实现两个32bit的hash函数 */
    public static class SHA1_MITZ_32 extends HASH_FUNCTION{
        public <T> boolean put(T object, Funnel<? super T> funnel, int numHashFunctions, AtomicLongArray bits) {
            long bitSize = bits.length() * Long.SIZE;
            long hash64 = Hashing.sha1().hashObject(object, funnel).asLong();
            int hash1 = (int) hash64;
            int hash2 = (int) (hash64 >>> 32);

            boolean bitsChanged = false;
            for (int i = 1; i <= numHashFunctions; i++) {
                int combinedHash = hash1 + (i * hash2);
                // Flip all the bits if it's negative (guaranteed positive number)
                if (combinedHash < 0) {
                    combinedHash = ~combinedHash;
                }
                bitsChanged |= set(bits, combinedHash % bitSize);
            }
            return bitsChanged;
        }

        public <T> boolean mightContain(T object, Funnel<? super T> funnel, int numHashFunctions, AtomicLongArray bits) {
            long bitSize = bits.length() * Long.SIZE;
            long hash64 = Hashing.sha1().hashObject(object, funnel).asLong();
            int hash1 = (int) hash64;
            int hash2 = (int) (hash64 >>> 32);

            for (int i = 1; i <= numHashFunctions; i++) {
                int combinedHash = hash1 + (i * hash2);
                // Flip all the bits if it's negative (guaranteed positive number)
                if (combinedHash < 0) {
                    combinedHash = ~combinedHash;
                }
                if (!get(bits, combinedHash % bitSize)) {
                    return false;
                }
            }
            return true;
        }
    }

    /** SHA1 来实现两个64bit的hash函数 */
    public static class SHA1_MITZ_64 extends HASH_FUNCTION{
        public <T> boolean put(T object, Funnel<? super T> funnel, int numHashFunctions, AtomicLongArray bits) {
            long bitSize = bits.length() * Long.SIZE;
            byte[] bytes = Hashing.sha1().hashObject(object, funnel).asBytes();
            long hash1 = lowerEight(bytes);
            long hash2 = upperEight(bytes);

            boolean bitsChanged = false;
            long combinedHash = hash1;
            for (int i = 0; i < numHashFunctions; i++) {
                // Make the combined hash positive and indexable
                bitsChanged |= set(bits, (combinedHash & Long.MAX_VALUE) % bitSize);
                combinedHash += hash2;
            }
            return bitsChanged;
        }

        public <T> boolean mightContain(T object, Funnel<? super T> funnel, int numHashFunctions, AtomicLongArray bits) {
            long bitSize = bits.length() * Long.SIZE;
            byte[] bytes = Hashing.sha1().hashObject(object, funnel).asBytes();
            long hash1 = lowerEight(bytes);
            long hash2 = upperEight(bytes);

            long combinedHash = hash1;
            for (int i = 0; i < numHashFunctions; i++) {
                // Make the combined hash positive and indexable
                if (!get(bits, (combinedHash & Long.MAX_VALUE) % bitSize)) {
                    return false;
                }
                combinedHash += hash2;
            }
            return true;
        }
    }


    /** SHA256 来实现两个32bit的hash函数 */
    public static class SHA256_MITZ_32 extends HASH_FUNCTION{

        public <T> boolean put(T object, Funnel<? super T> funnel, int numHashFunctions, AtomicLongArray bits) {
            long bitSize = bits.length() * Long.SIZE;
            long hash64 = Hashing.sha256().hashObject(object, funnel).asLong();
            int hash1 = (int) hash64;
            int hash2 = (int) (hash64 >>> 32);

            boolean bitsChanged = false;
            for (int i = 1; i <= numHashFunctions; i++) {
                int combinedHash = hash1 + (i * hash2);
                // Flip all the bits if it's negative (guaranteed positive number)
                if (combinedHash < 0) {
                    combinedHash = ~combinedHash;
                }
                bitsChanged |= set(bits, combinedHash % bitSize);
            }
            return bitsChanged;
        }

        public <T> boolean mightContain(T object, Funnel<? super T> funnel, int numHashFunctions, AtomicLongArray bits) {
            long bitSize = bits.length() * Long.SIZE;
            long hash64 = Hashing.sha256().hashObject(object, funnel).asLong();
            int hash1 = (int) hash64;
            int hash2 = (int) (hash64 >>> 32);

            for (int i = 1; i <= numHashFunctions; i++) {
                int combinedHash = hash1 + (i * hash2);
                // Flip all the bits if it's negative (guaranteed positive number)
                if (combinedHash < 0) {
                    combinedHash = ~combinedHash;
                }
                if (!get(bits, combinedHash % bitSize)) {
                    return false;
                }
            }
            return true;
        }
    }


    /** SHA256 来实现两个64bit的hash函数 */
    public static class SHA256_MITZ_64 extends HASH_FUNCTION{
        public <T> boolean put(T object, Funnel<? super T> funnel, int numHashFunctions, AtomicLongArray bits) {
            long bitSize = bits.length() * Long.SIZE;
            byte[] bytes = Hashing.sha256().hashObject(object, funnel).asBytes();
            long hash1 = lowerEight(bytes);
            long hash2 = upperEight(bytes);

            boolean bitsChanged = false;
            long combinedHash = hash1;
            for (int i = 0; i < numHashFunctions; i++) {
                // Make the combined hash positive and indexable
                bitsChanged |= set(bits, (combinedHash & Long.MAX_VALUE) % bitSize);
                combinedHash += hash2;
            }
            return bitsChanged;
        }

        public <T> boolean mightContain(T object, Funnel<? super T> funnel, int numHashFunctions, AtomicLongArray bits) {
            long bitSize = bits.length() * Long.SIZE;
            byte[] bytes = Hashing.sha256().hashObject(object, funnel).asBytes();
            long hash1 = lowerEight(bytes);
            long hash2 = upperEight(bytes);

            long combinedHash = hash1;
            for (int i = 0; i < numHashFunctions; i++) {
                // Make the combined hash positive and indexable
                if (!get(bits, (combinedHash & Long.MAX_VALUE) % bitSize)) {
                    return false;
                }
                combinedHash += hash2;
            }
            return true;
        }
    }




    static boolean set(AtomicLongArray bits, long bitIndex){
        if (get(bits, bitIndex)) {
            return false;
        }

        int longIndex = (int) (bitIndex >>> LONG_ADDRESSABLE_BITS);
        long mask = 1L << bitIndex; // only cares about low 6 bits of bitIndex

        long oldValue;
        long newValue;
        do {
            oldValue = bits.get(longIndex);
            newValue = oldValue | mask;
            if (oldValue == newValue) {
                return false;
            }
        } while (!bits.compareAndSet(longIndex, oldValue, newValue));

        // We turned the bit on, so increment bitCount.
        //bitCount.increment();
        return true;
    }

    static boolean get(AtomicLongArray bits, long bitIndex) {
        return (bits.get((int) (bitIndex >>> LONG_ADDRESSABLE_BITS)) & (1L << bitIndex)) != 0;
    }

    static long lowerEight(byte[] bytes) {
        return Longs.fromBytes(
                bytes[7], bytes[6], bytes[5], bytes[4], bytes[3], bytes[2], bytes[1], bytes[0]);
    }

    static long upperEight(byte[] bytes) {
        return Longs.fromBytes(
                bytes[15], bytes[14], bytes[13], bytes[12], bytes[11], bytes[10], bytes[9], bytes[8]);
    }
}
