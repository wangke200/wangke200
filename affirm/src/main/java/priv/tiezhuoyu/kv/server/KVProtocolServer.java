package priv.tiezhuoyu.kv.server;

import java.util.List;

public interface KVProtocolServer {
	public String set(String key, String val);
	public void batchSet(List<String> keys, List<String> vals);
	public String get(String key);
	public List<String> batchGet(List<String> keys);
	public String buildIndex(String key, String val);
	public List<String> query(List<String> token);
	public String del(String key);
	public void flushall();
}
