package com.tiezh.hash;

import com.google.common.hash.Funnel;

import java.util.*;

public class BloomFilterVHT<K, V> extends VerifiableHashTable<K,V> {
    protected double fpp;
    protected int expectedInsertions;

    static class Node<V> extends VerifiableHashTable.Node<V>{
        Node(V value, Hasher hash) {
            this(new HashSet<V>(), hash);
            this.getValues().add(value);
        }

        Node(Set<V> values, Hasher hash) {
            super(values, hash);
        }
    }


    public BloomFilterVHT(Funnel< ? super V > funnel, int expectedInsertions, double fpp, HashStrategy hashStrategy){
        this.funnel = funnel;
        this.hashStrategy = hashStrategy;
        this.expectedInsertions = expectedInsertions;
        this.fpp = fpp;
        this.table = new HashMap<>();
    }

    /** add a value into key */
    public void add(K key, V value) {
        if(this.table.containsKey(key)){
            BloomFilterUtil bf = (BloomFilterUtil)table.get(key).getHash();
            bf.put(value);
            Node<V> node = (Node<V>) table.get(key);
            node.getValues().add(value);
        }else {
            BloomFilterUtil bf = BloomFilterUtil.create(funnel,
                    expectedInsertions,
                    fpp,
                    (BloomFilterUtil.Strategy) hashStrategy);
            bf.put(value);
            Node<V> node = new Node<V>(value, bf);
            table.put(key, node);
        }
    }


    /** get merged values (multi set, implement as LinkedList) */
    public List<V> getMergedValues(Set<K> keys){
        List<V> mergedValues = new LinkedList<>();
        for(Object k : keys){
            mergedValues.addAll(getValues(k));
        }
        return mergedValues;
    }


    /** get merged hash */
    public Hasher getMergedHash(Set<K> keys) throws Exception {
        BloomFilterUtil<V> mergeHash =
                BloomFilterUtil.create(funnel, this.expectedInsertions, this.fpp, (BloomFilterUtil.Strategy) hashStrategy);
        List<BloomFilterUtil> hashes = new LinkedList<>();
        for(Object k : keys){
            hashes.add((BloomFilterUtil) getHash(k));
        }
        mergeHash.merge(hashes);
        return mergeHash;
    }

    public boolean verify(K key) {
        if(!table.containsKey(key))
            return false;
        Node<V> node = (Node<V>) table.get(key);
        LinkedList<V> values = (LinkedList<V>) node.getValues();
        try {
            long[] bitsArray = ((BloomFilterUtil)node.getHash()).getBitsArray();

            BloomFilterUtil curbf = BloomFilterUtil.create(funnel,
                    expectedInsertions,
                    fpp,
                    (BloomFilterUtil.Strategy) hashStrategy);
            for(V value : values){
                curbf.put(value);
            }
            long[] curBitsArray = curbf.getBitsArray();
            for(int i = 0; i < bitsArray.length; i++){
                if(bitsArray[i] != curBitsArray[i])
                    return false;
            }
        } catch (Exception e) {
            e.printStackTrace();
            return false;
        }
        return true;
    }
}
