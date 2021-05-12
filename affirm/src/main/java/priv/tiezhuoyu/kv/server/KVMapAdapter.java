package priv.tiezhuoyu.kv.server;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class KVMapAdapter implements KVStore{
	//data source
	Map<String, String> source;
	int nodeId;
	
	public KVMapAdapter(Map<String, String> source, int nodeId) {
		this.source = source;
		this.nodeId = nodeId;
	}
	
	
	@Override
	public String set(String key, String val) {
		return source.put(key, val);
	}


	@Override
	public void batchSet(List<String> keys, List<String> vals) {
		for(int i = 0; i < keys.size(); i++) {
			source.put(keys.get(i), vals.get(i));
		}
	}

	
	@Override
	public String get(String key) {
		return source.get(key);
	}


	@Override
	public String del(String key) {
		return source.get(key);
	}


	@Override
	public int nodeId() {
		return this.nodeId;
	}


	@Override
	public List<String> batchGet(List<String> keys) {
		List<String> vals = new ArrayList<>();
		for(String key : keys) {
			vals.add(source.get(key));
		}
		return vals;
	}


	@Override
	public void flushAll() {
		source = new HashMap<String, String>();
	}
}
