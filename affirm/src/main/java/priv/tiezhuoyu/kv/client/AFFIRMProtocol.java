package priv.tiezhuoyu.kv.client;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.math.BigInteger;
import java.security.InvalidAlgorithmParameterException;
import java.security.InvalidKeyException;
import java.security.KeyPair;
import java.security.NoSuchAlgorithmException;
import java.security.NoSuchProviderException;
import java.security.cert.CertificateFactory;
import java.security.cert.X509Certificate;
import java.security.interfaces.RSAPrivateKey;
import java.security.interfaces.RSAPublicKey;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import javax.crypto.NoSuchPaddingException;

import org.apache.thrift.TException;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;

import priv.tiezhuoyu.crypto.ApacheBase64Util;
import priv.tiezhuoyu.crypto.CryptoPrimitives;
import priv.tiezhuoyu.crypto.TrapdoorPermutation;
import priv.tiezhuoyu.kv.server.KVService.Client;
import priv.tiezhuoyu.kv.Protocol;
import priv.tiezhuoyu.kv.server.KVStore;

public class AFFIRMProtocol extends SEKVProtocol{
	static final int RSA_KEY_SIZE = 1024;
	static final int COUNTER_SIZE = 128;
	static final String CERT_FILE_PATH = "./affrim.cert"; 
	
	// different from EncKVProtocol.counterMap<String, Integer>
	public Map<String, BigInteger> counterMap;
	RSAPublicKey pk;
	RSAPrivateKey sk;
	
	
	// json string to counter map
	public void loadCounterMap(String jsonString) {
		if(jsonString == null)
			throw new NullPointerException();
		this.counterMap = (Map)JSONObject.parse(jsonString);
	}
	
	// counter map to json string
	public String counterMap2JSONString() {
		return JSON.toJSONString(this.counterMap);
	}
	
	
	@Override
	public List<String> keyExchange() throws TException {
		List<String> keyAndParams = new ArrayList<>();
		keyAndParams.add(protocolName);
		
		for(Client client : cliGroup) {
			keyAndParams.add(ApacheBase64Util.encode2String(pk.getEncoded()));
			client.keyExchange(keyAndParams);
		}
		return null;
	}
	
	public AFFIRMProtocol(List<Client> cliGroup) {
		super(cliGroup);
		this.counterMap = new HashMap<>();
		protocolName = Protocol.AFFIRM.name();
	}
	
	@Override
	public void init(String key) {
		super.init(key);
		KeyPair keyPair;
		try {
			File keyPairFile = new File(CERT_FILE_PATH);
			if(keyPairFile.exists()) {		// read keypair from file if exsist
				byte[] keyPairBytes = file2Bytes(keyPairFile);
				
			    ByteArrayInputStream bi = new ByteArrayInputStream(keyPairBytes);
			    ObjectInputStream oi = new ObjectInputStream(bi);
			    keyPair = (KeyPair) oi.readObject();
			    
				pk = (RSAPublicKey) keyPair.getPublic();
				sk = (RSAPrivateKey) keyPair.getPrivate();
				
			}else {		// generate keypair and write it into file
				keyPair = TrapdoorPermutation.genKeyPair(RSA_KEY_SIZE);
				pk = (RSAPublicKey) keyPair.getPublic();
				sk = (RSAPrivateKey) keyPair.getPrivate();
				
				// write keypair to file
			    ByteArrayOutputStream b = new ByteArrayOutputStream();
			    ObjectOutputStream o =  new ObjectOutputStream(b);
			    o.writeObject(keyPair);
			    byte[] keyPairBytes = b.toByteArray();
				
			    o.close();
			    b.close(); 
			    
			    keyPairFile.createNewFile();
			    
			    bytes2File(keyPairBytes, keyPairFile);
			}
		} catch (NoSuchAlgorithmException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		}
	}
	

	@Override
	public String buildIndex(String R, String C, String v)
			throws TException, InvalidKeyException, InvalidAlgorithmParameterException, NoSuchAlgorithmException,
			NoSuchProviderException, NoSuchPaddingException, IOException {
		//get route id
		int routeId = routeId(R);
		
		//t1
		byte[] t1 = CryptoPrimitives.generateHmac(skG1, C + ":" + v + ":" + routeId);
		
		//t2
		byte[] t2 = CryptoPrimitives.generateHmac(skG2, C + ":" + v + ":" + routeId);
		
		//get counter
		String cmKey = C + ":" + v + ":" + routeId;
		
		//get current counter of C||v||routeId
		BigInteger cnt = null;
		if(counterMap.containsKey(cmKey)) {
			cnt = counterMap.get(cmKey);
			cnt = TrapdoorPermutation.tP(sk, cnt);
		}else
			//debug
			//cnt = BigInteger.ONE;
			cnt = BigInteger.probablePrime(COUNTER_SIZE, secureRandom);

			
		//write into counter map
		counterMap.put(cmKey, cnt);
		
		//alpha = H1(t1, cnt)
		byte[] t1Cnt = CryptoPrimitives.concat(t1,cnt.toByteArray());
		byte[] alpha = CryptoPrimitives.generateHmac(skH1, t1Cnt);
		
		//beta = E(ke, R)
		byte[] ivBytes = new byte[16];
		secureRandom.nextBytes(ivBytes);
		byte[] beta = CryptoPrimitives.encryptAES_CBC(skr, ivBytes, R.getBytes("UTF-8"));
		
		//beta = E(ke, R) xor H2(t2, cnt)
		byte[] t2Cnt = CryptoPrimitives.concat(t2, cnt.toByteArray());
		byte[] betaMask = CryptoPrimitives.generateHmac(skH2, t2Cnt);
		for(int i = 0; i < beta.length; i++) {
			beta[i] = (byte)(beta[i] ^ betaMask[i % betaMask.length]);
		}
		
		Client client = cliGroup.get(routeId);
		
		return client.setPair(ApacheBase64Util.encode2String(alpha), ApacheBase64Util.encode2String(beta));
	}
	
	
	@Override
	public List<String> query(String Cv, String v, String Cr)
			throws TException, InvalidKeyException, InvalidAlgorithmParameterException, NoSuchAlgorithmException,
			NoSuchProviderException, NoSuchPaddingException, IOException, InterruptedException, ExecutionException {

		// store rowList queried from each node
		List<List<String>> rowLists = new CopyOnWriteArrayList<>();
		
		// fill with null, easy to insertion
		for(int i = 0; i < cliGroup.size(); i++)
			rowLists.add(null);
		
		// thread counter for main thread wait for sub thread
		CountDownLatch latch = new CountDownLatch(cliGroup.size());
		
		for(int routeId = 0; routeId < cliGroup.size(); routeId++) {
			Client client = cliGroup.get(routeId);
			//t1
			byte[] t1 = CryptoPrimitives.generateHmac(skG1, Cv + ":" + v + ":" + routeId);
			
			//t2
			byte[] t2 = CryptoPrimitives.generateHmac(skG2, Cv + ":" + v + ":" + routeId);

			//cnt
			String cmKey = Cv + ":" + v + ":" + routeId;
			
			BigInteger cnt = counterMap.get(cmKey);
			if(cnt == null) {
				// it means that there is no record which value is v
				// is store at node with routeId
				cnt = BigInteger.ONE;
			}
			List<String> token = new ArrayList<>();
			token.add(ApacheBase64Util.encode2String(t1));
			token.add(ApacheBase64Util.encode2String(t2));
			token.add(ApacheBase64Util.encode2String(cnt.toByteArray()));
			
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
				String R = new String(plaintext, "UTF-8");
				rList.set(i, R);
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
									resultList.add("(R=" + R + "->" + routeId(R) + "," + Cr + "=" + val + ")");
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
	
	private static byte[] file2Bytes(File file) throws IOException {
		FileInputStream fis = new FileInputStream(file);
		ByteArrayOutputStream bOut = new ByteArrayOutputStream();
		int ch;
		while ((ch = fis.read()) >= 0) {
			bOut.write(ch);
		}
		return bOut.toByteArray();
	}
	
	private static void bytes2File(byte[] input, File file) throws IOException {
		if(!file.exists())
			file.createNewFile();
		FileOutputStream fos = new FileOutputStream(file);
		fos.write(input);
		fos.close();
	}
}
