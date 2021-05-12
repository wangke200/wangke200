package com.tiezh.hash;

import com.google.common.hash.Funnel;

import java.util.*;

public class MultiSetHashVHT <K,V> extends VerifiableHashTable <K,V> {

    static class Node<V> extends VerifiableHashTable.Node<V>{
        Node(V value, Hasher hash) {
            this(new HashSet<V>(), hash);
            this.getValues().add(value);
        }

        Node(Set<V> values, Hasher hash) {
            super(values, hash);
        }
    }

    public MultiSetHashVHT(Funnel < ? super V > funnel, HashStrategy hashStrategy){
        this.funnel = funnel;
        this.hashStrategy = hashStrategy;
        this.table = new HashMap<>();
    }


    public void add(K key, V value) {
//        if(!(hashStrategy instanceof VerifiableHashTable.Strategy))
//            throw new IllegalArgumentException("need a VerifiableHashTable.Strategy strategy");
        if(this.table.containsKey(key)){
            MultiSetHash multiSetHash = (MultiSetHash) table.get(key).getHash();
            multiSetHash.add(value);
            Node<V> node = (Node<V>) table.get(key);
            node.getValues().add(value);
        }else {
            MultiSetHash multiSetHash = MultiSetHash.create(funnel, (MultiSetHash.Strategy) hashStrategy);
            multiSetHash.add(value);
            Node<V> node = new Node<V>(value, multiSetHash);
            table.put(key, node);
        }
    }

    /** get values of multi key */
    public List<V> getValues(Object ... key){
        LinkedList allVals = new LinkedList();
        for(Object k : key){
            allVals.addAll(getValues(key));
        }
        return allVals;
    }

    /** get merged values (multi set, implement as LinkedList) */
    public Set<V> getMergedValues(Set<K> keys){
        Set<V> mergedValues = new HashSet<V>();
        for(Object k : keys){
            mergedValues.addAll(getValues(k));
        }
        return mergedValues;

    }

    /** get merged hash,
     * and assuming that one value with only one key */
    public Hasher getMergedHash(Set<K> keys){
        MultiSetHash mergeHash = MultiSetHash.create(funnel, (MultiSetHash.Strategy) hashStrategy);
        List<MultiSetHash> hashes = new LinkedList<>();
        for(Object k : keys){
            hashes.add((MultiSetHash) getHash(k));
        }
        mergeHash.merge(hashes);
        return mergeHash;
    }

    /** get merged hash,
     * and assuming one value with multi key, like document with some keywords.
     * we need to repair */
    public Hasher getMergedRepairHash(Set<K> keys){

        //to collect repeated values
        HashSet<V> repeatedValSet = new HashSet<>();
        ArrayList<K> keyArray = new ArrayList<>(keys);
        for(int i = 0; i < keys.size() - 1; i++) {
            K key1 = keyArray.get(i);
            Set<V> valsOfkey1 = table.get(key1).getValues();
            for (V val1 : valsOfkey1) {
                if (!repeatedValSet.contains(val1)) {
                    // first we assume that val1 is not repeated yet,
                    // and next we check if it is really not repeated
                    boolean isRepeated = false;
                    for (int j = i + 1; j < keys.size(); j++) {
                        K key2 = keyArray.get(j);
                        Set<V> valsOfkey2 = table.get(key2).getValues();
                        if (valsOfkey2.contains(val1)) {
                            // even appearance equal to not repeated in multSetHash with xor;
                            // odd appearance is actually repeated in multiSetHash with xor;
                            isRepeated ^= true;
                        }
                    }
                    if (isRepeated)
                        repeatedValSet.add(val1);
                }
            }
        }
        // deal with merged hash
        MultiSetHash mergeHash = MultiSetHash.create(funnel, (MultiSetHash.Strategy) hashStrategy);
        List<MultiSetHash> hashes = new LinkedList<>();
        for(Object k : keys){
            hashes.add((MultiSetHash) getHash(k));
        }
        mergeHash.merge(hashes);

        for(V val : repeatedValSet){
                mergeHash.add(val);
        }


        return mergeHash;
    }


    @Deprecated
    public Hasher getMergedRepairHashNotOptimized(Set<K> keys){
        // deal with merged hash
        MultiSetHash mergeHash = MultiSetHash.create(funnel, (MultiSetHash.Strategy) hashStrategy);
        List<MultiSetHash> hashes = new LinkedList<>();
        HashMap<V, Integer> valCounter = new HashMap<> ();
        for(Object k : keys){
            hashes.add((MultiSetHash) getHash(k));
            // record the number of repeated values
            Set<V> values = table.get(k).getValues();
            for(V val : values){
                int count = (valCounter.containsKey(val))? valCounter.get(val) : 0;
                valCounter.put(val, count + 1);
            }
        }
        mergeHash.merge(hashes);
        for(V val : valCounter.keySet()){
            for(int count = valCounter.get(val) - 1; count > 0; count--){
                mergeHash.add(val);
            }
        }

        return mergeHash;
    }


    /** override keySet method because we define a new vht */
    @Override
    public Set<K> keySet() {
        return this.table.keySet();
    }

    public boolean verify(K key) {
        if(!table.containsKey(key))
            return false;
        Node<V> node = (Node<V>) table.get(key);
        LinkedList<V> values = (LinkedList<V>) node.getValues();
        byte[] hashBytes = ((MultiSetHash)node.getHash()).getBytes();
        MultiSetHash curHash = MultiSetHash.create(this.funnel, (MultiSetHash.Strategy) hashStrategy);
        for(V value : values){
            curHash.add(value);
        }
        byte[] curHashByte = curHash.getBytes();
        for(int i = 0; i < hashBytes.length; i++){
            if(hashBytes[i] != curHashByte[i])
                return false;
        }
        return true;
    }
}
