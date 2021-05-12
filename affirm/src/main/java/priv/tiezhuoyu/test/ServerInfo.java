package priv.tiezhuoyu.test;

import com.alibaba.fastjson.annotation.JSONField;

public class ServerInfo {
	public static final String PROTOCOL_PLAINTEXT = "Plaintext";
	public static final String PROTOCOL_AFFIRM = "Affirm";
	
	@JSONField(name = "ADDRESS")
	String address;
	
	@JSONField(name = "THRIFT PORT")
	int thriftPort;
	
	@JSONField(name = "REDIS PORT")
	int redisPort;
	
	@JSONField(name = "PROTOCOL")
	String protocol;
	
	public String getAddress() {
		return address;
	}
	public ServerInfo setAddress(String address) {
		this.address = address;
		return this;
	}
	public int getThriftPort() {
		return thriftPort;
	}
	public ServerInfo setThriftPort(int thriftPort) {
		this.thriftPort = thriftPort;
		return this;
	}
	public int getRedisPort() {
		return redisPort;
	}
	public ServerInfo setRedisPort(int redisPort) {
		this.redisPort = redisPort;
		return this;
	}
	
	public ServerInfo setProtocol(String protocol) throws Exception {
		if(!protocol.equals(PROTOCOL_PLAINTEXT) && !protocol.equals(PROTOCOL_AFFIRM)) {
			System.out.println("Unsupported Protocol " + protocol);
			throw new Exception();
		}
		this.protocol = protocol;
		return this;
	}
	
	public String getProtocol() {
		return this.protocol;
	}
	
	@Override
	public String toString() {
		return "(" + this.address + "," +  this.thriftPort + "," + this.redisPort + ")";
	}
}
