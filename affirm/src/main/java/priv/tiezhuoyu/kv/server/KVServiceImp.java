package priv.tiezhuoyu.kv.server;

import java.security.KeyFactory;
import java.security.NoSuchAlgorithmException;
import java.security.interfaces.RSAPublicKey;
import java.security.spec.InvalidKeySpecException;
import java.security.spec.X509EncodedKeySpec;
import java.util.List;

import org.apache.thrift.TException;

import priv.tiezhuoyu.crypto.ApacheBase64Util;
import priv.tiezhuoyu.kv.Protocol;


public class KVServiceImp implements KVService.Iface{
	
	//singleton pattern
	private static volatile KVServiceImp instance = null;
	
	KVProtocolServer kvProtocolServer = null;
	KVStore kvAdapter;
	
	public KVServiceImp(KVStore kvAdapter) {
		this.kvAdapter = kvAdapter;
	}
	
	
	@Override
	public String setPair(String key, String val) throws TException {
		return kvProtocolServer.set(key, val);
	}
	


	@Override
	public String batchSetPair(List<String> keys, List<String> vals) throws TException {
		
		return null;
	}


	@Override
	public String getValue(String key) throws TException {
		return kvProtocolServer.get(key);
	}


	@Override
	public List<String> batchGetValue(List<String> keys) throws TException {
		return kvProtocolServer.batchGet(keys);
	}

	
	@Override
	public String buildIndex(String key, String val) throws TException {
		//do nothing because this function will not be called
		return null;
	}

	@Override
	public List<String> query(List<String> token) throws TException {
		return kvProtocolServer.query(token);
	}

	@Override
	public String delPair(String key) throws TException {
		return kvProtocolServer.del(key);
	}

	@Override
	public List<String> keyExchange(List<String> keyAndParam)
			throws TException, NoSuchAlgorithmException, InvalidKeySpecException {
		// get protocol name 
		String protocol = keyAndParam.get(0);
		
		if(protocol.equals(Protocol.Plaintext.name())) {
			this.kvProtocolServer = new PlaintextKVProtocolServer(kvAdapter);
		}else if(protocol.equals(Protocol.AFFIRM.name())) {
			// get rsa public key
			String base64Pk = keyAndParam.get(1);
			byte[] keyBytes = ApacheBase64Util.decode(base64Pk);
			X509EncodedKeySpec keySpec = new X509EncodedKeySpec(keyBytes);
			KeyFactory keyFactory = KeyFactory.getInstance("RSA");
			RSAPublicKey pk = (RSAPublicKey) keyFactory.generatePublic(keySpec);
			this.kvProtocolServer = new AFFIRMProtocolServer(kvAdapter, pk);
		}
		
		//return nothing new
		return keyAndParam;
	}


	@Override
	public String flushall() throws TException {
		try {
			kvProtocolServer.flushall();
		}catch(NullPointerException e) {
			System.out.println("flushall: " + e.getClass().getName());
		}
		return "DONE";
	}


}
