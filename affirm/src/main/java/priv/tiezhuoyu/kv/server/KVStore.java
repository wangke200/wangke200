package priv.tiezhuoyu.kv.server;

import java.util.List;

//an interface for KV store, enabling to select store source (like java.util.HashMap or Redis)
public interface KVStore {
	public String set(String key, String val);		//set function
	public void batchSet(List<String> keys, List<String> vals);	//batch set function
	public String get(String key);					//get function
	public List<String> batchGet(List<String> keys);//batch get function
	public String del(String key);					//del function
	public void flushAll();						//flush all
	public int nodeId();
	
	public static final String NULL = "(nil)";
}
