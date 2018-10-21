package deltad.core;

import java.util.Properties;
import java.util.Set;

import org.pentaho.di.core.exception.KettleException;

public interface ServiceManagerMBean {
	public void deploy(String xml) throws Exception;
	public void deployForName(String fileName) throws Exception;
	
	public void undeploy(String name) throws Exception;
	
	public void startModule(String moduleName, boolean clean) throws Exception;
	
	public String[] getModuleNames() throws Exception;
	public String displayModule(String key);
	public void shutdown() throws Exception;
	public Service getService(String moduleName);
	public boolean started(String moduleName);
	public String getProperty(String moduleName, String key);
	
	public Set<String[]> getMatchedTables(String xml) throws Exception;
	public Set<String[]> getMatchedTablesByName(String amoduleName) throws Exception;
	
	public void purgeQueue(String destName, String queueName) throws Exception;
	
	public void schedulePlan(String name, String scripts) throws Exception;
	public void schedulePlanForName(String fileName) throws Exception;
	public boolean cancelPlan(String name);	
	public String[] getPlanNames();
	public Properties getPlanContext(String planName);
	public String[] getPlanResult(String planName) throws Exception;
	public String[] getPlanScripts(String planName);
	public void deployTableMapping(String name, Properties mapping) throws Exception;
	public void unDeployTableMapping(String name);
	public String[] getTableMappingNames();
	public Properties getTableMapping(String key);
	
	public void loginRepository(String user, String pass) throws KettleException;
}
