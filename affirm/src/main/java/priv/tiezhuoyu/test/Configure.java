package priv.tiezhuoyu.test;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.List;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;

public class Configure {
	
	public static String readJSON(File file) throws IOException {
		FileInputStream fis = new FileInputStream(file);
		InputStreamReader isReader = new InputStreamReader(fis, "UTF-8");
		BufferedReader bReader = new BufferedReader(isReader);
		StringBuilder sBuilder = new StringBuilder();
		String s = "";
		while((s = bReader.readLine()) != null)
			sBuilder.append(s);
		bReader.close();
		isReader.close();
		fis.close();
		return sBuilder.toString();
	}
	
	public static void writeJSON(File file, String JSONString) throws IOException {
		FileOutputStream fos = new FileOutputStream(file);
		OutputStreamWriter osWriter = new OutputStreamWriter(fos, "UTF-8");
		BufferedWriter bWriter = new BufferedWriter(osWriter);
		bWriter.write(JSONString);
		bWriter.close();
		osWriter.close();
		fos.close();
	}
	
	public static void main(String[] args) throws Exception {
		JSONObject object = new JSONObject();
		//cli_configure
		List<ServerInfo> serverInfos = new ArrayList<>();
		serverInfos.add(new ServerInfo().setAddress("localhost").setThriftPort(9090).setRedisPort(6379));
		//serverInfos.add(new ServerInfo().setAddress("127.0.0.1").setThriftPort(9090).setRedisPort(6379));
		String configure = JSON.toJSON(serverInfos).toString();
		System.out.println(configure);
		File configureFile = new File("./cli-configure.json");
		writeJSON(configureFile, configure);
		
		
		//server_configure
		ServerInfo serverInfo = new ServerInfo().setAddress("localhost")
				.setThriftPort(9090).setRedisPort(6379).setProtocol(ServerInfo.PROTOCOL_PLAINTEXT);
		configure = JSON.toJSON(serverInfo).toString();
		System.out.println(configure);
		configureFile = new File("./server-configure.json");
		writeJSON(configureFile, configure);
	}

}
