package com.tiezh.gbf;

import com.google.common.hash.Funnel;
import com.tiezh.hash.HashStrategy;

import java.util.*;

public class GarbledBloomFilter<K, V> {

    /** 128-bit byte array, byte[16] */
    private List<byte[]> gbf;


    /** store <Base64(HMAC(key)), H(valSet) xor random1 xor ...> */
    private Map<String, byte[]> grabledValues;

    /** size of list gbf */
    private int size;

    /** hash strategy HMAC256 */
    private Strategy strategy;

    private Funnel<? super K> kFunnel;
    private Funnel<? super V> vFunnel;

    /** id hash murmur128, byte[16] */
    private static final int bytesLen = 16;

    /** String  funnel */
//    private static final Funnel<CharSequence> strfunnel = Funnels.stringFunnel(Charsets.UTF_8);

    /** Number of hashes per element */
    private static final int numHashFunctions = 12;


    /** size = multiple * elementNum */
    private static final int multipleOfElement  = 31;


    public interface Strategy extends HashStrategy {

        /**
         * Sets {@code numHashFunctions} bits of the given bit array, by hashing a user element.
         *
         * <p>Returns whether any bits changed as a result of this operation.
         */
        <K, V> void put(K key, Funnel<? super K> kFunnel, Set<V> values, Funnel<? super V> vFunnel,
                int numHashFunctions, Map<String, byte[]> grabledValues, List<byte[]> gbf, int bytesLen);

        /**
         * Queries {@code numHashFunctions} bits of the given bit array, by hashing a user element;
         * returns {@code true} if and only if all selected bits are set.
         */
        <K, V> boolean mightContain(K key, Funnel<? super K> kFunnel, Set<V> values, Funnel<? super V> vFunnel,
                int numHashFunctions, Map<String, byte[]> grabledValues, List<byte[]> gbf, int bytesLen);

    }


    public void put(K key, Set<V> valSet) {
        strategy.put(key, kFunnel, valSet, vFunnel, numHashFunctions,
                this.grabledValues, this.gbf, this.bytesLen);
    }


    public boolean contain(K key, Set<V> valSet){
        return strategy.mightContain(key, kFunnel, valSet, vFunnel, numHashFunctions,
                this.grabledValues, this.gbf, this.bytesLen);
    }


    public int getSize() {
        return size;
    }

    public Strategy getStrategy() {
        return strategy;
    }

    public Funnel<? super K> getkFunnel() {
        return kFunnel;
    }

    public Funnel<? super V> getvFunnel() {
        return vFunnel;
    }

    public static int getBytesLen() {
        return bytesLen;
    }

    public static int getNumHashFunctions() {
        return numHashFunctions;
    }

    public static int getMultipleOfElement() {
        return multipleOfElement;
    }

    private GarbledBloomFilter(int elementNum, Funnel<? super K> kFunnel, Funnel<? super V> vFunnel, Strategy strategy){
        this.size = 31 * elementNum;
        this.kFunnel = kFunnel;
        this.vFunnel = vFunnel;
        this.strategy = strategy;

        this.gbf = new ArrayList<>(this.size);
        //create gbf list
        for(int i = 0; i < this.size; i++){
            // inital and fill with null
            gbf.add(null);
        }

        this.grabledValues = new HashMap<>();
    }

    public static <K, V> GarbledBloomFilter<K, V> create(
            int elementNum, Funnel<? super K> kFunnel, Funnel<? super V> vFunnel, Strategy strategy){
        return new GarbledBloomFilter<> (elementNum, kFunnel, vFunnel, strategy);
    }

}
