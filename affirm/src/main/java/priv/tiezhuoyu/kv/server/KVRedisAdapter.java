package priv.tiezhuoyu.kv.server;

import java.util.ArrayList;
import java.util.List;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.Pipeline;
import redis.clients.jedis.Response;

public class KVRedisAdapter implements KVStore{
	//data source
	Jedis jedis;
	int nodeId;
	
	public KVRedisAdapter(Jedis jedis, int nodeId) {
		this.jedis = jedis;
		this.nodeId = nodeId;
	}
	
	@Override
	public String set(String key, String val) {
		return jedis.set(key, val);
	}

	@Override
	public void batchSet(List<String> keys, List<String> vals) {
		Pipeline pipeline = jedis.pipelined();
		for(int i = 0; i < keys.size(); i++)
			pipeline.set(keys.get(0), vals.get(i));
		pipeline.sync();
	}
	
	
	@Override
	public String get(String key) {
		return jedis.get(key);
	}
	

	@Override
	public List<String> batchGet(List<String> keys) {
		Pipeline pipeline = jedis.pipelined();
		List<Response<String>> responses = new ArrayList<>();
		for(String key : keys)
			responses.add(pipeline.get(key));
		pipeline.sync();
		List<String> vals = new ArrayList<>();
		for(Response<String> r : responses) {
			vals.add(r.get());
		}
		return vals;
	}

	@Override
	public String del(String key) {
		return Long.toString(jedis.del(key));
	}

	@Override
	public int nodeId() {
		return this.nodeId;
	}
	

	@Override
	public void flushAll() {
		jedis.flushAll();
	}

}
