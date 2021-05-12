package priv.tiezhuoyu.test;

import java.io.File;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.security.InvalidAlgorithmParameterException;
import java.security.InvalidKeyException;
import java.security.InvalidParameterException;
import java.security.NoSuchAlgorithmException;
import java.security.NoSuchProviderException;
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

public class TestBuild {

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
		//flushall
		kvProtocol.flushall();
		
		//keyExchange, tell the server which protocol and key is 
		kvProtocol.keyExchange();
		
		// detailed determines if output detail info or not
		// none yet
		String Cr = "Score";
		long stime, etime;
		long setCost, getCost, buildCost, queryCost, batchBuildCost, batchQueryCost;
		System.out.println("protocol = " + kvProtocol);
		System.out.println("node num = " + nodeNum);
		System.out.println("data size = " + dataSize);
		System.out.println("matched num = " + matchedNum);
		
		// test set()
		System.out.println("set operation...");
		stime = System.currentTimeMillis();
		for (int id = 0; id < dataSize; id++) {
			int v = id % (dataSize / matchedNum);
			/*
			System.out.println("set: \n  R: " + id);
			System.out.println("  C: " + Cr);
			System.out.println("  v: " + Integer.toString(id % 20 + 80));
			*/
			kvProtocol.set(Integer.toString(id), Cr, Integer.toString(v));
		}
		etime = System.currentTimeMillis();
		setCost = etime - stime;
		
		
		// test buildIndex()
		System.out.println("build operation...");
		stime = System.currentTimeMillis();
		for (int id = 0; id < dataSize; id++) {
			int v = id % (dataSize / matchedNum);
			/*
			System.out.println("buildIndex:");
			System.out.println("  C: " + Cr);
			System.out.println("  v: " + Integer.toString(v));
			System.out.println("  R: " + id);
			*/
			kvProtocol.buildIndex(Integer.toString(id), Cr, Integer.toString(v));
		}
		etime = System.currentTimeMillis();
		buildCost = etime - stime;
		
		System.out.println("setCost = " + setCost + "ms");
		System.out.println("buildCost = " + buildCost + "ms");
		
		//close protocol (mainly close thread pool)
		kvProtocol.close();
		
		
		// if AFFIRM, write counter map into file
		if(kvProtocol instanceof AFFIRMProtocol) {
			AFFIRMProtocol affirm = (AFFIRMProtocol)kvProtocol;
			File cMapfile = new File(counterMapPath);
			Configure.writeJSON(cMapfile, affirm.counterMap2JSONString());
		}
	}

}
