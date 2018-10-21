package deltad.core;


/**
 * @author haozhu
 */
public interface AgentMBean {
	public void shutdown() throws Exception;
	public void start() throws Exception;
	public String getServerName();
	public String getAgentProperty(String key);
	
}
