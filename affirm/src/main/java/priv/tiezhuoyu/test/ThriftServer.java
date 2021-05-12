package priv.tiezhuoyu.test;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.UnsupportedEncodingException;

import org.apache.thrift.TProcessor;
import org.apache.thrift.TProcessorFactory;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.server.THsHaServer;
import org.apache.thrift.server.THsHaServer.Args;
import org.apache.thrift.server.TServer;
import org.apache.thrift.server.TServer.AbstractServerArgs;
import org.apache.thrift.server.TSimpleServer;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TNonblockingServerSocket;
import org.apache.thrift.transport.TServerSocket;
import org.apache.thrift.transport.TTransportException;

import com.alibaba.fastjson.JSON;

import priv.tiezhuoyu.kv.server.KVRedisAdapter;
import priv.tiezhuoyu.kv.server.KVService;
import priv.tiezhuoyu.kv.server.KVServiceImp;
import redis.clients.jedis.Jedis;
 
public class ThriftServer {
 
    private final static int DEFAULT_THRIFT_PORT = 9090; // thrift�˿�
    private final static int DEFAULT_REDIS_PORT = 6379; // redis�˿�
    
    private static TServer server = null;
 
    @SuppressWarnings({ "rawtypes", "unchecked" })
    public static void main(String[] args) throws TTransportException, UnsupportedEncodingException, IOException {
		String address = "localhost";
		int thriftPort = DEFAULT_THRIFT_PORT;
		int redisPort = DEFAULT_REDIS_PORT;
		String protocol = "Plaintext";
    	
    	if (args.length == 0) {
			System.out.println("You need to specify a configuration file like './server-configure.json'");
		} else {
			File configureFile = new File(args[0]);
			String configure = Configure.readJSON(configureFile);
			ServerInfo sInfo = JSON.parseObject(configure, ServerInfo.class);
			address = sInfo.getAddress();
			thriftPort = sInfo.getThriftPort();
			redisPort = sInfo.redisPort;
			protocol = sInfo.getProtocol();
		}
    	

    	Jedis jedis = new Jedis(address, redisPort);
    	TProcessor processor = null;processor = new priv.tiezhuoyu.kv.server.KVService.Processor<KVService.Iface>(
        			new KVServiceImp(new KVRedisAdapter(jedis, 0)));
    	
    	// multi thread server
		TNonblockingServerSocket socket = new TNonblockingServerSocket(thriftPort);
		Args args1 = new Args(socket);
        args1.processorFactory(new TProcessorFactory(processor));
        args1.protocolFactory(new TBinaryProtocol.Factory());
        args1.transportFactory(new TFramedTransport.Factory());
        //
        args1.maxReadBufferBytes = 10 * 1024 *1024L;
        
        System.out.println("server start!");
        server = new THsHaServer(args1);
        server.serve();
		
    	//single thread server
        /*
    	TServerSocket socket = new TServerSocket(thriftPort);
        AbstractServerArgs args1 = new TServer.Args(socket);
        args1.processorFactory(new TProcessorFactory(processor));
        args1.protocolFactory(new TBinaryProtocol.Factory());
        args1.transportFactory(new TFramedTransport.Factory());
        
        System.out.println("server start!");
        server = new TSimpleServer(args1);
        server.serve();
        */
    }
}
