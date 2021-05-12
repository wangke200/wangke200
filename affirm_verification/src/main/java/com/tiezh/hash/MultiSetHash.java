package com.tiezh.hash;

import com.google.common.hash.Funnel;

import java.util.Arrays;
import java.util.List;

public class  MultiSetHash <T> implements Hasher {
    private byte[] hash;
    private Strategy strategy;
    private Funnel<? super T> funnel;

    public interface Strategy extends HashStrategy{
        public <T> byte[] hash(T object, Funnel<? super T> funnel);
        public int getSize();
    }

    private MultiSetHash(byte[] hash, Funnel<? super T> funnel, Strategy strategy){
        if(hash.length != strategy.getSize())
            throw new IllegalArgumentException("length of hash byte array and hash strategy is different");
        this.hash = hash;
        this.strategy = strategy;
        this.funnel = funnel;
    }

    /** add a object into multi set hash */
    public void add(T object){
        byte[] addhash = strategy.hash(object, this.funnel);
        add(this.hash, addhash);
    }


    /** merge another hash into this.hash */
    public void merge(List<MultiSetHash> multiSetHashes){
        MultiSetHash newMultiSetHash = MultiSetHash.create(this.hash, this.funnel, this.strategy);
        for(MultiSetHash multiSetHash : multiSetHashes){
            byte[] hash = multiSetHash.getBytes();
            add(this.hash, hash);
        }
    }


    public byte[] getBytes(){return hash;}

    /** add, xor two hash, and write it into the first hash array */
    private static void add(byte[] hash1, byte[] hash2){
        if(hash1.length != hash2.length)
            throw new IllegalArgumentException("length of two hash (" + hash1.length
                    + "," + hash2.length + ") are different");

    }


    @Override
    public String toString() {
        return "MultiSetHash{" +
                "hash=" + Arrays.toString(hash) +
                '}';
    }


    public static <T> MultiSetHash <T> create(byte[] hash, Funnel<? super T> funnel, Strategy strategy){
        return new MultiSetHash(hash, funnel, strategy);
    }

    public static <T> MultiSetHash <T> create(Funnel<? super T> funnel, Strategy strategy){
        byte[] hash = new byte[strategy.getSize()];
        return create(hash, funnel, strategy);
    }

}
