package priv.tiezhuoyu.test;

import java.io.File;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.math.BigInteger;
import java.security.InvalidAlgorithmParameterException;
import java.security.InvalidKeyException;
import java.security.InvalidParameterException;
import java.security.NoSuchAlgorithmException;
import java.security.NoSuchProviderException;
import java.security.KeyStore.Entry;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;

import javax.crypto.NoSuchPaddingException;

import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;

import com.alibaba.fastjson.JSONArray;

import priv.tiezhuoyu.kv.Protocol;
import priv.tiezhuoyu.kv.client.AFFIRMProtocol;
import priv.tiezhuoyu.kv.client.KVProtocol;
import priv.tiezhuoyu.kv.client.PlaintextProtocal;
import priv.tiezhuoyu.kv.server.KVService;

public class TestQuery {

	private final static int DEFAULT_TIMEOUT = 10000000;
	static final String counterMapPath = "./counter-map.json";
	
	public static void main(String[] args) throws IOException {
		Protocol kvProtocol = Protocol.Plaintext;
		
		int nodeNum = 1;
		int dataSize = 10000;
		int matchedNum = 1;
		int blockSize = 4;
		
		/*
		 * java -jar Client.jar [Protocol] [nodeNum] [dataSize] [matchedNum]
		 */
		
		
		// check args: configure
		if (args.length == 0) {
			System.out.println("You need to specify a configuration file like './cli-configure.json'");
			return;
		}
		
		// check args: protocol
		if(args.length > 1) {
			try {
				kvProtocol = Protocol.valueOf(args[1]);
			}catch (IllegalArgumentException e) {
				System.out.println("You need to specify a protocol from 'Plaintext' or 'AFFIRM'");
				return;
			}
		}
		
		// check args: nodeNum
		if(args.length > 2) {
			try {
				nodeNum = Integer.valueOf(args[2]);
			}catch(NumberFormatException e) {
				System.out.println("args[2]: '" + args[2] + "' should be an integer");
			}
		}
		
		// check args: dataSize
		if(args.length > 3) {
			try {
				dataSize = Integer.valueOf(args[3]);
			}catch(NumberFormatException e) {
				System.out.println("args[3]: '" + args[3] + "' should be an integer");
			}
		}
		
		// check args: matchedNum
		if(args.length > 4) {
			try {
				matchedNum = Integer.valueOf(args[4]);
			}catch(NumberFormatException e) {
				System.out.println("args[4]: '" + args[4] + "' should be an integer");
			}
		}
		
		// check args: block size
		if(args.length > 5) {
			try {
				blockSize = Integer.valueOf(args[5]);
				if(blockSize != 2 && blockSize != 4 && blockSize != 8)
					throw new InvalidParameterException("block size should be 2, 4, or 8 bits");
				
			}catch(NumberFormatException e) {
				System.out.println("args[5]: '" + args[5] + "' should be an integer");
			}
		}
		
		// configure for client to find server
		File configureFile = new File(args[0]);
		String configure = Configure.readJSON(configureFile);
		List<ServerInfo> serverInfos = JSONArray.parseArray(configure, ServerInfo.class);

		// client group
		List<KVService.Client> clients = new ArrayList<>();
		for(int i = 0; i < nodeNum; i++) {
			ServerInfo sInfo = serverInfos.get(i);
			String address = sInfo.getAddress();
			int thriftPort = sInfo.getThriftPort();
			TTransport tTransport = getTTransport(address, thriftPort, DEFAULT_TIMEOUT);
			TProtocol protocol = new TBinaryProtocol(tTransport, true, true);

			KVService.Client client = new KVService.Client(protocol);
			clients.add(client);
		}
		
		try {
			if(kvProtocol.equals(Protocol.Plaintext)) {					//plaintextkv
				testKVProtocol(new PlaintextProtocal(clients), nodeNum, dataSize, matchedNum, blockSize);
			}else if(kvProtocol.equals(Protocol.AFFIRM)) {					//affirm
				AFFIRMProtocol affirmProtocol = new AFFIRMProtocol(clients);
				affirmProtocol.init("mykey");
				affirmProtocol.keyExchange();
				testKVProtocol(affirmProtocol, nodeNum, dataSize, matchedNum, blockSize);
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		
	}

	private static TTransport getTTransport(String address, int thriftPort, int timeOut) {
		try {
			TTransport tTransport = new TFramedTransport(new TSocket(address, thriftPort, timeOut));
			if (!tTransport.isOpen()) {
				tTransport.open();
			}
			return tTransport;
		} catch (Exception e) {
			e.printStackTrace();
		}
		return null;
	}
	
	private static void testKVProtocol(KVProtocol kvProtocol, int nodeNum, int dataSize, int matchedNum, int blockSize) throws Exception {
		// if protocol is AFFIRM, load counter map from file
		if(kvProtocol instanceof AFFIRMProtocol) {
			AFFIRMProtocol affirm = (AFFIRMProtocol)kvProtocol;
			File cMapfile = new File(counterMapPath);
			affirm.loadCounterMap(Configure.readJSON(cMapfile));
		}
		
		//keyExchange, tell the server which protocol and key is 
		kvProtocol.keyExchange();
		
		// detailed determines if output detail info or not
		// none yet
		String Cr = "Score";
		long stime, etime;
		long queryCost;
		System.out.println("protocol = " + kvProtocol);
		System.out.println("node num = " + nodeNum);
		System.out.println("data size = " + dataSize);
		System.out.println("matched num = " + matchedNum);
		
		// test query()
		System.out.println("query operation...");
		stime = System.currentTimeMillis();
		for (int v = 0; v < (dataSize / matchedNum); v++) {
			List<String> result = kvProtocol.query(Cr, Integer.toString(v), Cr);
			/*
			System.out.println("query:");
			System.out.println("  Cv: " + Cr);
			System.out.println("  v: " + Integer.toString(v));
			System.out.println("  result: " + result);
			*/
		}
		etime = System.currentTimeMillis();
		queryCost = etime - stime;
		
		System.out.println("quryCost = " + queryCost + "ms");
		System.out.println("quryCost = " + (queryCost * 1.0) / (dataSize / matchedNum) + "ms");
		System.out.println("qury throughput = " +  (dataSize / matchedNum) *1000.0  / queryCost + " queries/s");
		System.out.println("qury throughput = " +  (dataSize) *1000  / queryCost + " entries/s");
		//System.out.println("batch buildCost = " + batchBuildCost + "ms");
		

		//close protocol (mainly close thread pool)
		kvProtocol.close();
	}

}
