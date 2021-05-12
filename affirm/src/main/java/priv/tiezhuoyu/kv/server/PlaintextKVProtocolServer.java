package priv.tiezhuoyu.kv.server;

import java.util.ArrayList;
import java.util.List;

public class PlaintextKVProtocolServer implements KVProtocolServer{
	//choose a adapter, local hashmap or redis
	KVStore kvAdapter;
	
	public PlaintextKVProtocolServer(KVStore kvAdapter) {
		this.kvAdapter = kvAdapter;
	}
	
	@Override
	public String set(String key, String val) {
		String pval = kvAdapter.set(key, val);
		if(pval == null)
			return KVStore.NULL;
		return pval;
	}
	
	@Override
	public void batchSet(List<String> keys, List<String> vals) {
		kvAdapter.batchSet(keys, vals);
	}
	

	@Override
	public String get(String key) {
		String val = kvAdapter.get(key);
		if(val == null)
			return KVStore.NULL;
		return val;
	}


	@Override
	public List<String> batchGet(List<String> keys) {
		List<String> vals = kvAdapter.batchGet(keys);
		for(int i = 0; i < vals.size(); i++) {
			if(vals.get(i) == null)
				vals.set(i, KVStore.NULL);
		}
		return vals;
	}
	
	
	@Override
	public String buildIndex(String key, String val) {
		// Temporarily useless for plaintext kv
		return null;
	}

	@Override
	public List<String> query(List<String> token) {
		int cnt = 0;
		List<String> Rs = new ArrayList<>();
		while(true) {
			String indexKey = token.get(0) + ":" + cnt;
			String R = kvAdapter.get(indexKey);
			if(R == null)
				break;
			Rs.add(R);
			cnt++;
		}
		//if no result
		if(Rs.size() == 0)
			Rs.add(KVStore.NULL);
		
		return Rs;
	}

	@Override
	public String del(String key) {
		String pval = kvAdapter.del(key);
		if(pval == null)
			return KVStore.NULL;
		return pval;
	}
	
	@Override
	public void flushall() {
		kvAdapter.flushAll();
	}
}
