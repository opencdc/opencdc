package deltad.core;

import java.io.FileOutputStream;
import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Properties;
import java.util.Set;

import javax.management.MBeanServerConnection;

public class AgentInfoCache {	
	private final LinkedHashMap<String, MBeanServerConnection> agentInfo = new LinkedHashMap<String, MBeanServerConnection>();
	private static AgentInfoCache instance = new AgentInfoCache();
	
	private AgentInfoCache() {		
	}
	
	public void put(String name_url, MBeanServerConnection connection) {
		agentInfo.put(name_url, connection);
	}
	
	public MBeanServerConnection get(String key) {
		return agentInfo.get(key);
	}
	
	public Set<String> keySet() {
		return agentInfo.keySet();
	}
	
	protected void store(FileOutputStream out) throws IOException {
		Properties Properties = new Properties();
		Properties.store(out, null);
	}
	
	public static AgentInfoCache getInstance() {
		return instance;
	}

	public static void main(String[] args) {

	}

}
