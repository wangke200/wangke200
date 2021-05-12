package com.tiezh.hash;

import com.google.common.hash.Funnel;

import java.util.Collection;
import java.util.HashMap;
import java.util.Set;

public abstract class VerifiableHashTable <K,V> {

    protected HashMap<K,Node<V>> table;
    protected Funnel <? super V> funnel;
    protected HashStrategy hashStrategy;

    /** Node store value and message authentication code) */
    static class Node<V>{
        private Set<V> values;
        private Hasher hash;

        public Set<V> getValues() { return values; }
        public Hasher getHash(){ return hash; }

        protected Node(Set<V> values, Hasher hash){
            this.values = values;
            this.hash = hash;
        }
    }

    /** get values according to key */
    public Collection<V> getValues(Object key){ return table.get(key).values; }

    /** get a hash according to key */
    public Hasher getHash(Object key){
        return table.get(key).hash;
    }

    /** get key set */
    public Set<K> keySet(){
        return table.keySet();
    }

//    /** put key-values-hash pair */
//    public abstract void put(K key, Collection<V> values);

    /** add a value into set of key pair */
    public abstract void add(K key, V value);

    /** verify */
    public abstract boolean verify(K key);

}
