package priv.tiezhuoyu.kv.client;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.security.InvalidAlgorithmParameterException;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.security.NoSuchProviderException;
import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import javax.crypto.NoSuchPaddingException;

import org.apache.thrift.TException;
import org.bouncycastle.crypto.Digest;
import org.bouncycastle.crypto.digests.MD5Digest;
import org.bouncycastle.crypto.digests.SHA256Digest;

import priv.tiezhuoyu.crypto.ApacheBase64Util;
import priv.tiezhuoyu.crypto.CryptoPrimitives;
import priv.tiezhuoyu.kv.Protocol;
import priv.tiezhuoyu.kv.Route;
import priv.tiezhuoyu.kv.client.PlaintextProtocal.GetCallable;
import priv.tiezhuoyu.kv.client.PlaintextProtocal.QueryCallable;
import priv.tiezhuoyu.kv.server.KVService;
import priv.tiezhuoyu.kv.server.KVService.Client;
import priv.tiezhuoyu.kv.server.KVStore;

public class SEKVProtocol implements KVProtocol{
	String protocolName;
	
	//thread pool
	ExecutorService executorService;
	
	SecureRandom secureRandom;
	
	private static final int IVBYTES_LENGTH = 16;
	
	List<KVService.Client> cliGroup;
	//counter of each value for different attribute and store node
	HashMap<String, Integer> counterMap;
	
	//secret key
	byte[] skl, skv, skr;
	byte[] skG1, skG2, skH1, skH2;

	
	public SEKVProtocol(List<KVService.Client> cliGroup) {
		// protocol name
		protocolName = Protocol.SEKV.name();
		
		counterMap = new HashMap<>();
		this.cliGroup = cliGroup;
		secureRandom = new SecureRandom();
		
		// init thread pool
		this.executorService = new ThreadPoolExecutor(cliGroup.size(), 10 * cliGroup.size(), 
				10, TimeUnit.SECONDS, 
				new ArrayBlockingQueue<>(512), 
				new ThreadPoolExecutor.CallerRunsPolicy());
	}
	
	
	public void init(String key) {
		Digest md5 = new MD5Digest();
		Digest sha256 = new SHA256Digest();
		
		//skl
		byte[] tmpkey = (key + "skl").getBytes(); 
		sha256.update(tmpkey, 0, tmpkey.length);
		skl = new byte[sha256.getDigestSize()];
		sha256.doFinal(skl, 0);
		
		//skv
		tmpkey = (key + "skv").getBytes(); 
		md5.update(tmpkey, 0, tmpkey.length);
		skv = new byte[md5.getDigestSize()];
		md5.doFinal(skv, 0);
		
		//skr
		tmpkey = (key + "skr").getBytes(); 
		md5.update(tmpkey, 0, tmpkey.length);
		skr = new byte[md5.getDigestSize()];
		md5.doFinal(skr, 0);
		
		//skG1
		tmpkey = (key + "skG1").getBytes(); 
		sha256.update(tmpkey, 0, tmpkey.length);
		skG1 = new byte[sha256.getDigestSize()];
		sha256.doFinal(skG1, 0);
		
		//skG2
		tmpkey = (key + "skG2").getBytes(); 
		sha256.update(tmpkey, 0, tmpkey.length);
		skG2 = new byte[sha256.getDigestSize()];
		sha256.doFinal(skG2, 0);
		
		//skH1
		tmpkey = ("skH1").getBytes(); 
		sha256.update(tmpkey, 0, tmpkey.length);
		skH1 = new byte[sha256.getDigestSize()];
		sha256.doFinal(skH1, 0);
		
		//skH2
		tmpkey = ("skH2").getBytes(); 
		sha256.update(tmpkey, 0, tmpkey.length);
		skH2 = new byte[sha256.getDigestSize()];
		sha256.doFinal(skH2, 0);
	}
	
	
	@Override
	public String set(String R, String C, String v) throws TException, InvalidKeyException, InvalidAlgorithmParameterException, 
	NoSuchAlgorithmException, NoSuchProviderException, NoSuchPaddingException, IOException {
		// P(skl, C||R), P is HMAC function
		byte[] CR = CryptoPrimitives.concat(C.getBytes(), R.getBytes());
		byte[] P = CryptoPrimitives.generateHmac(skl, CR);
		
		// E(skv, v), E is AES
		byte[] ivBytes = new byte[IVBYTES_LENGTH];
		secureRandom.nextBytes(ivBytes);
		byte[] E = CryptoPrimitives.encryptAES_CBC(skv, ivBytes, v.getBytes("UTF-8"));
		int routeId = routeId(R);
		Client client = cliGroup.get(routeId);
		
		//base64 encode and set operation
		return client.setPair(ApacheBase64Util.encode2String(P), ApacheBase64Util.encode2String(E));
	}

	@Override
	public String get(String R, String C) throws TException, InvalidKeyException, InvalidAlgorithmParameterException, 
	NoSuchAlgorithmException, NoSuchProviderException, NoSuchPaddingException, IOException, InterruptedException, ExecutionException {
		
		int routeId = routeId(R);
		Client client = cliGroup.get(routeId);
		// P(skl, C||R), P is HMAC function
		byte[] CR = CryptoPrimitives.concat(C.getBytes(), R.getBytes());
		byte[] P = CryptoPrimitives.generateHmac(skl, CR);
		
		String r = client.getValue(ApacheBase64Util.encode2String(P));
		byte[] E = ApacheBase64Util.decode(r);
		String result = new String(CryptoPrimitives.decryptAES_CBC(E, skv), "UTF-8"); 
		
		return result;
	}

	@Override
	public String buildIndex(String R, String C, String v) throws TException, InvalidKeyException, InvalidAlgorithmParameterException, 
	NoSuchAlgorithmException, NoSuchProviderException, NoSuchPaddingException, IOException {
		//get route id
		int routeId = routeId(R);
		
		//t1
		byte[] t1 = CryptoPrimitives.generateHmac(skG1, C + ":" + v + ":" + routeId);
		
		//t2
		byte[] t2 = CryptoPrimitives.generateHmac(skG2, C + ":" + v + ":" + routeId);
		
		//get counter
		String cmKey = C + ":" + v;
		cmKey = cmKey + ":" + routeId;
		
		//get current counter of C||v
		int cnt = 0;
		if(counterMap.containsKey(cmKey))
			cnt = counterMap.get(cmKey) + 1;
		//write into counter map
		counterMap.put(cmKey, cnt);
		
		//alpha = H1(t1, cnt)
		byte[] t1Cnt = CryptoPrimitives.concat(t1, Integer.toString(cnt).getBytes());
		byte[] alpha = CryptoPrimitives.generateHmac(skH1, t1Cnt);
		
		//beta = E(ke, R)
		byte[] ivBytes = new byte[16];
		secureRandom.nextBytes(ivBytes);
		byte[] beta = CryptoPrimitives.encryptAES_CBC(skr, ivBytes, R.getBytes("UTF-8"));
		
		//beta = E(ke, R) xor H2(t2, cnt)
		byte[] t2Cnt = CryptoPrimitives.concat(t2, Integer.toString(cnt).getBytes());
		byte[] betaMask = CryptoPrimitives.generateHmac(skH2, t2Cnt);
		for(int i = 0; i < beta.length; i++) {
			beta[i] = (byte)(beta[i] ^ betaMask[i % betaMask.length]);
		}
		
		Client client = cliGroup.get(routeId);
		
		return client.setPair(ApacheBase64Util.encode2String(alpha), ApacheBase64Util.encode2String(beta));
	}

	@Override
	public List<String> query(String Cv, String v, String Cr) throws TException, InvalidKeyException, InvalidAlgorithmParameterException, 
	NoSuchAlgorithmException, NoSuchProviderException, NoSuchPaddingException, IOException, InterruptedException, ExecutionException{

		// thread counter for main thread wait for sub thread
		CountDownLatch latch = new CountDownLatch(cliGroup.size());
		
		// store rowList queried from each node
		List<List<String>> rowLists = new CopyOnWriteArrayList<>();
		
		// fill with null, easy to insertion
		for(int i = 0; i < cliGroup.size(); i++)
			rowLists.add(null);
		
		for(int routeId = 0; routeId < cliGroup.size(); routeId++) {
			Client client = cliGroup.get(routeId);
			//t1
			byte[] t1 = CryptoPrimitives.generateHmac(skG1, Cv + ":" + v + ":" + routeId);
			
			//t2
			byte[] t2 = CryptoPrimitives.generateHmac(skG2, Cv + ":" + v + ":" + routeId);

			List<String> token = new ArrayList<>();
			token.add(ApacheBase64Util.encode2String(t1));
			token.add(ApacheBase64Util.encode2String(t2));
			
			// packing id into an Integer object, used in Runnable
			Integer idInteger = new Integer(routeId);
			
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
					
					latch.countDown();
				}
			});
		}
		
		// wait for the sub thread
		latch.await();

		// remove '(nil)' from rowLists, and decrypt each element
		for(List<String> rList : rowLists) {
			for(int i = 0; i < rList.size(); i++) {
				String E = rList.get(i);
				// remove '(nil)'
				if(E.equals(KVStore.NULL)) {
					rList.remove(i);
					continue;
				}
				
				// decytpe element
				byte[] byteE = ApacheBase64Util.decode(E);
				byte[] plaintext = CryptoPrimitives.decryptAES_CBC(byteE, skr);
				rList.set(i, new String(plaintext, "UTF-8"));
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
		List<String> keyAndParams = new ArrayList<>();
		keyAndParams.add(protocolName);
		for(Client client : cliGroup) {
			client.keyExchange(keyAndParams);
		}
		return null;
	}
	

	protected int routeId(String R) {
		return Route.routeHash(R) % cliGroup.size();
	}


	@Override
	public void batchSet(List<String> Rs, String C, List<String> vs)
			throws TException, UnsupportedEncodingException, InvalidKeyException, InvalidAlgorithmParameterException,
			NoSuchAlgorithmException, NoSuchProviderException, NoSuchPaddingException, IOException {
		// TODO Auto-generated method stub
		
	}

	/**
	 * called in query()
	 * be sured that all R in Rs is routed to same node
	 * */
	@Override
	public List<String> batchGet(List<String> Rs, String C)
			throws TException, UnsupportedEncodingException, InvalidKeyException, InvalidAlgorithmParameterException,
			NoSuchAlgorithmException, NoSuchProviderException, NoSuchPaddingException, IOException {
		List<String> keys = new ArrayList<>();
		// keys list
		for(int i = 0; i < Rs.size(); i++) {
			String R = Rs.get(i);
			// P(skl, C||R), P is HMAC function
			byte[] CR = CryptoPrimitives.concat(C.getBytes(), R.getBytes());
			byte[] P = CryptoPrimitives.generateHmac(skl, CR);
			keys.add(ApacheBase64Util.encode2String(P));
		}
		
		//init result list
		List<String> result = new ArrayList<>(Rs.size());
		for(int i = 0; i < Rs.size(); i++)
			result.add(KVStore.NULL);
		
		//batch get
		int routeId = routeId(Rs.get(0));
		
		Client client = cliGroup.get(routeId);
		List<String> vals = client.batchGetValue(keys);
		for(int i = 0; i < vals.size(); i++) {
			if(!vals.get(i).equals(KVStore.NULL)) {
				byte[] E = ApacheBase64Util.decode(vals.get(i));
				String v = new String(CryptoPrimitives.decryptAES_CBC(E, skv), "UTF-8"); 
				result.set(i, v);
			}
		}
		return result;
	}


	@Override
	public void batchBuildIndex(List<String> Rs, List<String> Cs, List<String> vs)
			throws TException, UnsupportedEncodingException, InvalidKeyException, InvalidAlgorithmParameterException,
			NoSuchAlgorithmException, NoSuchProviderException, NoSuchPaddingException, IOException {
		// TODO Auto-generated method stub
		
	}
	
	@Override
	public void flushall() throws TException {
		for(Client client : cliGroup) {
			client.flushall();
		}
	}


	@Override
	public void close() {
		this.executorService.shutdown();
	}

}

