package com.tiezh.hash;

import com.google.common.hash.BloomFilter;
import com.google.common.hash.Funnel;

import java.lang.reflect.Field;
import java.util.Collection;
import java.util.Iterator;
import java.util.concurrent.atomic.AtomicLongArray;

import static com.google.common.base.Preconditions.checkNotNull;

public class BloomFilterUtil <T> implements Hasher {

    /** Bloom Filter from Guava */
    private BloomFilter<T> bf = null;

    /** The bit set of the BloomFilter (not necessarily power of 2!) */
    private AtomicLongArray bits = null;

    /** Number of hashes per element */
    private int numHashFunctions = 0;

    /** The funnel to translate Ts to bytes */
    private Funnel<? super T> funnel = null;

    /** The strategy we employ to map an element T to {@code numHashFunctions} bit indexes. */
    private Strategy strategy = null;



    /** reflect member variable */
    private static Class classBloomFilter;
    private static Class classBloomFilterStrategies;
    private static Class classLockFreeBitArray;
    private static Class classAtomicLongArray;

    static{
        try {
            classBloomFilter = Class.forName("com.google.common.hash.BloomFilter");
            classBloomFilterStrategies = Class.forName("com.google.common.hash.BloomFilterStrategies");
            classLockFreeBitArray = Class.forName("com.google.common.hash.BloomFilterStrategies$LockFreeBitArray");
            classAtomicLongArray = Class.forName("java.util.concurrent.atomic.AtomicLongArray");
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
    }


    public interface Strategy extends HashStrategy {

        /**
         * Sets {@code numHashFunctions} bits of the given bit array, by hashing a user element.
         *
         * <p>Returns whether any bits changed as a result of this operation.
         */
        <T> boolean put(
                T object, Funnel<? super T> funnel, int numHashFunctions, AtomicLongArray bits);

        /**
         * Queries {@code numHashFunctions} bits of the given bit array, by hashing a user element;
         * returns {@code true} if and only if all selected bits are set.
         */
        <T> boolean mightContain(
                T object, Funnel<? super T> funnel, int numHashFunctions, AtomicLongArray bits);

    }


    private BloomFilterUtil(BloomFilter<T> bf, Funnel funnel, int numHashFunctions, Strategy strategy) {
        this.funnel = funnel;
        this.numHashFunctions = numHashFunctions;
        this.strategy = checkNotNull(strategy);
        this.bf = bf;

        Field fBits = null;
        try {
            fBits = classBloomFilter.getDeclaredField("bits");
            fBits.setAccessible(true);
            Object bits = fBits.get(bf);

            Field fAtomicLongArray = classLockFreeBitArray.getDeclaredField("data");
            fAtomicLongArray.setAccessible(true);
            this.bits = (AtomicLongArray)fAtomicLongArray.get(bits);
        } catch (Exception e) {
            bits = null;
        }

    }


    public static <T> BloomFilterUtil<T> create(Funnel<? super T> funnel, long expectedInsertions, double fpp, Strategy strategy) {
        BloomFilter<T> bf = BloomFilter.create(funnel, expectedInsertions, fpp);
        checkNotNull(strategy);

        long numBits = optimalNumOfBits(expectedInsertions, fpp);
        int numHashFunctions = optimalNumOfHashFunctions(expectedInsertions, numBits);
        try {

            return new BloomFilterUtil<T>(bf, funnel, numHashFunctions, strategy);
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException("Could not create BloomFilter of " + numBits + " bits", e);
        }
    }

    public static <T> BloomFilterUtil <T> create(Funnel<? super T> funnel, int expectedInsertions, double fpp, Strategy strategy) {
        return create(funnel, (long) expectedInsertions, fpp, strategy);
    }


    public AtomicLongArray getBits() {
        return bits;
    }

    public long[] getBitsArray() throws Exception {
        Field fArray = classAtomicLongArray.getDeclaredField("array");
        fArray.setAccessible(true);
        long[] array = (long[]) fArray.get(bits);
        return array;
    }


    public int getNumHashFunctions() {
        return numHashFunctions;
    }

    public Strategy getStrategy() {
        return strategy;
    }

    public boolean put(T object){
        return strategy.put(object, funnel, numHashFunctions, bits);
    }

    public boolean mightContain(T object){
        return strategy.mightContain(object, funnel, numHashFunctions, bits);
    }


    /** predict optimal number of bits of bloom filter */
    public static long optimalNumOfBits(long n, double p) {
        if (p == 0) {
            p = Double.MIN_VALUE;
        }
        return (long) (-n * Math.log(p) / (Math.log(2) * Math.log(2)));
    }

    /** predict optimal number of hash function of bloom filter */
    public static int optimalNumOfHashFunctions(long n, long m) {
        // (m / n) * log(2), but avoid truncation due to division!
        return Math.max(1, (int) Math.round((double) m / n * Math.log(2)));
    }


    /** merge another hash into this.hash */
    /** notice that MultiSetHash.hash = 0 xor hash(a1) xor hash(a2) xor ...,
     *  thus, merge another MultiSetHash actually get:
     *  0 xor (0 xor hash(a1) xor hash(a2) xor ... )
     *  = hash(a1) xor hash(a2) xor ...
     *  but actually we need 0 xor hash(a1) xor hash(a2) xor ....
     *  We need to xor one more 0 if we merge odd number of MultiSetHash
     *
     *  Be sure that all merged multiSetHash with the same funnel and strategy,
     *  because this function does not check it!
     * */
    public void merge(Collection<BloomFilterUtil> bfs) throws Exception {

        Iterator<BloomFilterUtil> iterator = bfs.iterator();
        BloomFilterUtil bf1 = iterator.next();

        Field fArray = classAtomicLongArray.getDeclaredField("array");
        long[] bfLongArray = getBitsArray();

        for(int i = 0; i < bfLongArray.length; i++){
            for(BloomFilterUtil bf : bfs){
                long[] longArray = bf.getBitsArray();
                bfLongArray[i] = longArray[i] | bfLongArray[i];
            }
        }
    }

    public static BloomFilterUtil merge(BloomFilterUtil bf1, BloomFilterUtil bf2) throws Exception {
        if(bf1.getNumHashFunctions() != bf2.getNumHashFunctions()) {
            throw new Exception("the number of hash functions of two Bloom Filter is different");
        }
        if(!bf1.getStrategy().equals(bf2.getStrategy())){
            throw new Exception("two Bloom Filter with different Strategy");
        }
        if(bf1.getBits().length() != bf2.getBits().length()){
            throw new Exception("two Bloom Filter with different length");
        }

        BloomFilterUtil bf3 = new BloomFilterUtil(bf1.bf.copy(), bf1.funnel, bf1.getNumHashFunctions(), bf1.getStrategy());

        AtomicLongArray bits2 = bf2.getBits();
        AtomicLongArray bits3 = bf3.getBits();

        Field fArray = classAtomicLongArray.getDeclaredField("array");
        fArray.setAccessible(true);
        long[] array2 = (long[]) fArray.get(bits2);
        long[] array3 = (long[]) fArray.get(bits3);

        for(int i = 0; i < array3.length; i++){
            array3[i] = array3[i] | array2[i];
        }

        return bf3;
    }
}
