package priv.tiezhuoyu.kv.client;

import java.io.Closeable;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.security.InvalidAlgorithmParameterException;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.security.NoSuchProviderException;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutionException;

import javax.crypto.NoSuchPaddingException;

import org.apache.thrift.TException;

// Protocol for KV store 
// This interface call KVStore to excute detail store operation
public interface KVProtocol {
	public String set(String R, String C, String v) throws Exception;
	public void batchSet(List<String> Rs, String C, List<String> vs) throws Exception;
	public String get(String R, String C) throws Exception;
	public List<String> batchGet(List<String> Rs, String C) throws Exception;
	public String buildIndex(String R, String C, String v) throws Exception;
	public void batchBuildIndex(List<String> Rs, List<String> Cs, List<String> vs) throws Exception;
	public List query(String Cv, String v, String Cr) throws Exception;
	public List<String> keyExchange() throws Exception;
	public void flushall() throws Exception;
	//close the protocol (mainly refer to thread pool)
	public void close();
}
