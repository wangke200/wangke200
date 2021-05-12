package priv.tiezhuoyu.kv.client;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.security.InvalidAlgorithmParameterException;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.security.NoSuchProviderException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import javax.crypto.NoSuchPaddingException;

import priv.tiezhuoyu.kv.Protocol;
import priv.tiezhuoyu.kv.Route;
import priv.tiezhuoyu.kv.server.KVService;
import priv.tiezhuoyu.kv.server.KVService.Client;
import priv.tiezhuoyu.kv.server.KVStore;
import redis.clients.jedis.JedisPool;

import org.apache.thrift.TException;


public class PlaintextProtocal implements KVProtocol{
	
	String protocolName;
	
	List<KVService.Client> cliGroup;
	
	
	//counter of each value for different attribute and store node
	HashMap<String, Integer> counterMap;
	
	//thread pool
	ExecutorService executorService;
	
	// thread to run batch get
	// (useless for now)
	class BatchGetRunnable implements Runnable{
		Client client;
		List<String> keys;
		List<String> results;
		
		public BatchGetRunnable(Client client, List<String> keys, List<String> results) {
			this.client = client;
			this.keys = keys;
			this.results = results;
		}
		
		@Override
		public void run() {
			try {
				List<String> vals = client.batchGetValue(keys);
				for(int i = 0; i < results.size(); i++) {
					if(!vals.get(i).equals(KVStore.NULL) && results.get(i).equals(KVStore.NULL))
						results.set(i, vals.get(i));
				}
			} catch (TException e) {
				e.printStackTrace();
			}
		}}
	
	// thread task to get
	class GetCallable implements Callable<String>{
		Client client;
		String key;
		
		public GetCallable(Client client, String key) {
			this.client = client;
			this.key = key;
		}

		@Override
		public String call() throws Exception {
			try {
				return client.getValue(key);
			} catch (TException e) {
				e.printStackTrace();
			}
			return KVStore.NULL;
		}
	}
	
	class QueryCallable implements Callable<Set<String>>{
		Client client;
		List<String> token;
		CopyOnWriteArraySet<String> rowSet;
		
		public QueryCallable(Client client, List<String> token, CopyOnWriteArraySet<String> rowSet) {
			this.client = client;
			this.token = token;
			this.rowSet = rowSet;
		}
		
		@Override
		public Set<String> call() throws Exception {
			int cnt = 0;
			List<String> Rs = client.query(token);
			for(String R : Rs)
				if(!R.equals(KVStore.NULL))
					rowSet.add(R);
			return rowSet;
		}
	}
	
	public PlaintextProtocal(List<KVService.Client> cliGroup) {
		// protocol name
		this.protocolName = Protocol.Plaintext.name();
		
		counterMap = new HashMap<>();
		this.cliGroup = cliGroup;
		
		// init thread pool
		this.executorService = new ThreadPoolExecutor(cliGroup.size(), 10 * cliGroup.size(), 
				10, TimeUnit.SECONDS, 
				new ArrayBlockingQueue<>(512), 
				new ThreadPoolExecutor.CallerRunsPolicy());
	}
	
	
	@Override
	public String set(String R, String C, String v) throws TException {
		int routeId = routeId(R);
		Client client = cliGroup.get(routeId);
		return client.setPair(C + ":" + R, v);
	}
	

	@Override
	public void batchSet(List<String> Rs, String C, List<String> vs)
			throws TException, UnsupportedEncodingException, InvalidKeyException, InvalidAlgorithmParameterException,
			NoSuchAlgorithmException, NoSuchProviderException, NoSuchPaddingException, IOException {
		// divide list Rs, Cs, vs into cliGroup.size() parts, according to routeId
		List<List<String>> keysList = new ArrayList<>();
		List<List<String>> valsList = new ArrayList<>();
		
		for(int i = 0; i < Rs.size(); i++) {
			String R = Rs.get(i);
			String v = vs.get(i);
			
			int routeId = routeId(R);
			if(keysList.get(routeId) == null) {
				keysList.set(routeId, new ArrayList<>());
				valsList.set(routeId, new ArrayList<>());
			}
			keysList.get(routeId).add(C + ":" + R);
			valsList.get(routeId).add(v);
		}
		
		for(int routeId = 0; routeId < cliGroup.size(); routeId++) {
			if(keysList.get(routeId) == null || keysList.get(routeId).size() == 0)
				continue;
			Client client = cliGroup.get(routeId);
			client.batchSetPair(keysList.get(routeId), valsList.get(routeId));
		}
	}
	

	@Override
	public String get(String R, String C) throws TException, InterruptedException, ExecutionException {
		//single thread
		int routeId = routeId(R);
		Client client = cliGroup.get(routeId);
		String val = client.getValue(C + ":" + R);
		return val;
	}


	/**
	 * called in query()
	 * be sured that all R in Rs is routed to same node
	 * (single thread)
	 * */
	@Override
	public List<String> batchGet(List<String> Rs, String C)
			throws TException, UnsupportedEncodingException, InvalidKeyException, InvalidAlgorithmParameterException,
			NoSuchAlgorithmException, NoSuchProviderException, NoSuchPaddingException, IOException {
		
		// get routeId
		int routeId = routeId(Rs.get(0));
		
		List<String> keys = new ArrayList<>();
		
		// keys list
		for(int i = 0; i < Rs.size(); i++)
			keys.add(C + ":" + Rs.get(i));
		
		//init result list
		List<String> results = new ArrayList<>(Rs.size());
		for(int i = 0; i < Rs.size(); i++)
			results.add(KVStore.NULL);
		
		//batch get
		Client client = cliGroup.get(routeId);
		List<String> vals = client.batchGetValue(keys);
		for(int i = 0; i < vals.size(); i++) {
			if(!vals.get(i).equals(KVStore.NULL))
				results.set(i, vals.get(i));
		}
		
		return results;
	}

	
	@Override
	public String buildIndex(String R, String C, String v) throws TException {
		String cmKey = C + ":" + v;
		int routeId = routeId(R);
		cmKey = cmKey + ":" + routeId;
		
		//get current counter of C||v
		int cnt = 0;
		if(counterMap.containsKey(cmKey))
			cnt = counterMap.get(cmKey) + 1;
		//write into counter map
		counterMap.put(cmKey, cnt);
		String indexKey = cmKey + ":"  + cnt;
		
		Client client = cliGroup.get(routeId);
		
		return client.setPair(indexKey, R);
	}
	
	
	@Override
	public void batchBuildIndex(List<String> Rs, List<String> Cs, List<String> vs)
			throws TException, UnsupportedEncodingException, InvalidKeyException, InvalidAlgorithmParameterException,
			NoSuchAlgorithmException, NoSuchProviderException, NoSuchPaddingException, IOException {
		// TO DO HERE
	}
	

	@Override
	public List query(String Cv, String v, String Cr) throws TException, InvalidKeyException, UnsupportedEncodingException, 
	InvalidAlgorithmParameterException, NoSuchAlgorithmException, NoSuchProviderException, NoSuchPaddingException, IOException, InterruptedException, ExecutionException {
		
		// thread counter for main thread wait for sub thread
		CountDownLatch queryLatch = new CountDownLatch(cliGroup.size());
		
		// store rowList queried from each node
		List<List<String>> rowLists = new CopyOnWriteArrayList<>();
		
		// fill with null, easy to insertion
		for(int i = 0; i < cliGroup.size(); i++)
			rowLists.add(null);
		
		// get Rs from each node
		for(int id = 0; id < cliGroup.size(); id++) {
			Client client = cliGroup.get(id);
			String indexKey = Cv + ":" + v + ":" + id;
			List<String> token = new ArrayList<>();
			token.add(indexKey);
			
			// packing id into an Integer object, used in Runnable
			Integer idInteger = new Integer(id);
			this.executorService.submit(new Runnable() {
				
				@Override
				public void run() {
					List<String> Rs;
					try {
						Rs = client.query(token);
						rowLists.set(idInteger, Rs);
					} catch (Exception e) {
						e.printStackTrace();
					}

					// thread counter down
					queryLatch.countDown();
				}
			});
		}
		
		// wait for the sub thread
		queryLatch.await();
		
		// remove '(nil)' from rowLists
		for(List<String> rList : rowLists) {
			for(int i = 0; i < rList.size(); i++) {
				String R = rList.get(i);
				if(R.equals(KVStore.NULL))
					rList.remove(i);
			}
		}
		
		// thread counter for main thread wait for sub thread
		CountDownLatch getLatch = new CountDownLatch(cliGroup.size());
		
		// result list
		List<String> resultList = new CopyOnWriteArrayList<>();
		
		
		// get vals according to each Rs
		for(int id = 0; id < cliGroup.size(); id++) {
			Client client = cliGroup.get(id);
			Integer idInteger = new Integer(id);
			
			this.executorService.submit(new Runnable() {

				@Override
				public void run() {
					List<String> Rs = rowLists.get(idInteger);
					List<String> valList;
					
					if(Rs != null && Rs.size() != 0)
						try {
							valList = batchGet(Rs, Cr);

							for (int i = 0; i < Rs.size(); i++) {
								String R = Rs.get(i);
								String val = valList.get(i);

								if (!R.equals(KVStore.NULL))
									resultList.add("(R=" + R + "," + Cr + "=" + val + ")");
							}

						} catch (Exception e) {
							e.printStackTrace();
						}
					
					getLatch.countDown();
				}
			});
		}
		
		// wait for the sub thread
		getLatch.await();
		return resultList;
	}

	@Override
	public List<String> keyExchange() throws TException {
		List<String> keyAndParam = new ArrayList<>();
		keyAndParam.add(protocolName);
		for(Client client : cliGroup) {
			// tell server run Plaintext protocol
			client.keyExchange(keyAndParam);
			
		}
		return null;
	}
	
	@Override
	public void flushall() throws TException {
		for(Client client : cliGroup) {
			client.flushall();
		}
	}

	int routeId(String R) {
		return Route.routeHash(R) % cliGroup.size();
	}

	@Override
	public void close() {
		executorService.shutdown();
	}
}
