package com.tiezh.test;

public class Test {

    /** add, xor two hash, and write it into the first hash array */
    private static void add(byte[] hash1, byte[] hash2){
        if(hash1.length != hash2.length)
            throw new IllegalArgumentException("length of two hash (" + hash1.length
                    + "," + hash2.length + ") are different");
        for(int i = 0; i < hash1.length; i++){
            hash1[i] = (byte) (hash2[i] ^ hash1[i]);
        }
    }

    public static void main(String[] args) {

//        String str = "hello world";
//        byte[] bytes1 = new byte[16];
//        byte[] bytes2 = Hashing.murmur3_128().hashObject(str, new Funnel<String>() {
//            @Override
//            public void funnel(String from, PrimitiveSink into) {
//                into.putString(from, Charsets.UTF_8);
//            }
//        }).asBytes();
//
//        for(int i = 0; i < 16; i ++){
//            System.out.print(" " + bytes1[i]);
//        }System.out.println();
//
//        for(int i = 0; i < 16; i ++){
//            System.out.print(" " + bytes2[i]);
//        }System.out.println();
//
//        add(bytes1, bytes2);
//        for(int i = 0; i < 16; i ++){
//            System.out.print(" " + bytes1[i]);
//        }System.out.println();

//        //funnel
//        Funnel<String> funnel = new Funnel<String>() {
//            public void funnel(String from, PrimitiveSink into) {
//                into.putString(from, Charsets.UTF_8);
//            }
//        };
//
//        //Strategy
//        HashStrategy strategy = new MultiSetHashStrategies.MURMUR128();
//
//        //MultiSetHash
//        MultiSetHash mshash = MultiSetHash.create(funnel, (MultiSetHash.Strategy) strategy);
//
//        String input = "hello world";
//        mshash.add(input);
//
//        byte[] hash = mshash.getBytes();
//
//        for(byte b : hash){
//            System.out.print(" " + b);
//        }

        boolean pad = true;
        boolean flag1 = true, flag2 = false;

        for(int i = 0; i < 10; i++){
            System.out.println("[" + i + "] flag1 = " + flag1);
            System.out.println("[" + i + "] flag2 = " + flag2);
            flag1 ^= pad;
            flag2 ^= pad;
        }

    }

}
