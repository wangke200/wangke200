package priv.tiezhuoyu.test;

import org.apache.commons.pool2.PooledObject;
import org.apache.commons.pool2.PooledObjectFactory;
import org.apache.commons.pool2.impl.DefaultPooledObject;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;

import priv.tiezhuoyu.kv.server.KVService;
import priv.tiezhuoyu.kv.server.KVService.Client;

public class ThriftTransportFactory implements PooledObjectFactory<TTransport>{
	private final static int DEFAULT_TIMEOUT = 5000;
	
	String hostAddress;
	int thriftPort;
	
	@Override
	public PooledObject<TTransport> makeObject() throws Exception {
		// TODO Auto-generated method stub
		return null;
	}
	@Override
	public void destroyObject(PooledObject<TTransport> p) throws Exception {
		// TODO Auto-generated method stub
		
	}
	@Override
	public boolean validateObject(PooledObject<TTransport> p) {
		// TODO Auto-generated method stub
		return false;
	}
	@Override
	public void activateObject(PooledObject<TTransport> p) throws Exception {
		// TODO Auto-generated method stub
		
	}
	@Override
	public void passivateObject(PooledObject<TTransport> p) throws Exception {
		// TODO Auto-generated method stub
		
	}
	

}
