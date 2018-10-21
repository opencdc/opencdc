package deltad.core;

import java.math.BigInteger;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import javax.management.MBeanServerConnection;
import javax.management.MBeanServerInvocationHandler;
import javax.management.ObjectName;
import javax.management.remote.JMXConnector;
import javax.management.remote.JMXConnectorFactory;
import javax.management.remote.JMXServiceURL;

import org.apache.activemq.broker.jmx.QueueViewMBean;

import deltad.core.api.Module;
import deltad.core.api.TXExecutorModule;
import deltad.core.api.TableMatchEntry;
import deltad.core.api.TableMatchEntry.TableMatchRule;
import deltad.core.dsm.DataSourceManagerMBean;
import deltad.core.mcm.MailConfigureManagerMBean;


public class Utils {
	public static String TESTUSER = "DIBADMIN";
	public static String TESTPASS = "DIBADMIN";
	public static String DIB_TARGET_LOG = "v_dib_target_log";
	
	public static String AGENT_DOMAIN_NAME = "DIBServer:";
	public static String AGENT_OBJECT_NAME = AGENT_DOMAIN_NAME + "AgentName=singleton";
	public static final String DATASOUCE_MANAGER = AGENT_DOMAIN_NAME + "type=DataSouceManager,dsm=singleton";
	public static final String SERVICE_MANAGER = AGENT_DOMAIN_NAME + "type=ServiceManager,name=singleton";
	public static final String MAIL_MANAGER = AGENT_DOMAIN_NAME + "type=MailConfigureManager,dsm=singleton";
	public static final String SERVICE_OBJECT_NAME = AGENT_DOMAIN_NAME + "type=TransactionService,serviceName=%s";
	public static final String ACTIVEMQ_QUEUE_OBJECT_NAME = "org.apache.activemq:BrokerName=localhost,Type=Queue,Destination=%s";
	
	public static boolean pickBit(BigInteger big, int i) {
		BigInteger tmp = big.and(BigInteger.ONE.shiftLeft(i));		
		return tmp.shiftRight(i).longValue() > 0;
	}
	
	public static MBeanServerConnection getMBeanServerConnection(String url, Map<String, Object> env) throws Exception {
		String jmxUrl = String.format("service:jmx:rmi:///jndi/rmi://%s/jmxrmi", url);
		JMXServiceURL jmxServiceURL = new JMXServiceURL(jmxUrl);
		JMXConnector jmxConnector = JMXConnectorFactory.connect(jmxServiceURL, null);
		MBeanServerConnection mbeanServConn = jmxConnector.getMBeanServerConnection();
		return mbeanServConn;
	}
	
	public static QueueViewMBean getQueueView(String url, String queue) throws Exception {
		MBeanServerConnection connection = AgentInfoCache.getInstance().get(url);
		if (null == connection) {
			connection = getMBeanServerConnection(url, null);
			AgentInfoCache.getInstance().put(url, connection);
		}
		ObjectName queueon = new ObjectName(String.format(ACTIVEMQ_QUEUE_OBJECT_NAME, queue));
		return MBeanServerInvocationHandler.newProxyInstance(connection, queueon, QueueViewMBean.class, false);
	}
	
	public static long getQueueSize(String url, String queue) throws Exception {
		return getQueueView(url, queue).getQueueSize();
	}
	
	public static Properties getServiceLoggerConfiguration(String name, String level, String file, String size, int index) {
		Properties properties = new Properties();
		properties.setProperty(String.format("log4j.logger.%s", name), String.format("%s,%s", level.toUpperCase(), name));
		properties.setProperty(String.format("log4j.appender.%s", name), "org.apache.log4j.RollingFileAppender");
		properties.setProperty(String.format("log4j.appender.%s.file", name), file);
		properties.setProperty(String.format("log4j.appender.%s.maxFileSize", name), size);
		properties.setProperty(String.format("log4j.appender.%s.maxBackupIndex", name), String.valueOf(index));
		properties.setProperty(String.format("log4j.appender.%s.append", name), "true");
		properties.setProperty(String.format("log4j.appender.%s.layout", name), "org.apache.log4j.PatternLayout");
		properties.setProperty(String.format("log4j.appender.%s.layout.ConversionPattern", name), "%d %-5p [%c] (%t) %m%n");
		return properties;	
	}
	
	public static Properties getDefaultServiceLoggerConfiguration(String name) {
		String level = System.getProperty("DIBSERVICELOGLEVEL", "DEBUG");
		return getServiceLoggerConfiguration(name, level, String.format("${%s}/log/%s/dib.log", Bootstrap.SERVER_HOME_KEY, name),"16MB", 1024 * 64);
	}

	public static String[] getDataSourceNames(String agentUrl) throws Exception {		
		return getDataSourceManager(agentUrl).getDatasourceNames();
	}

	public static String[] getModuleNames(String agentUrl) throws Exception {
		return getServiceManager(agentUrl).getModuleNames();
	}
	
	public static String[] getTableMappingNames(String agentUrl) throws Exception {
		return getServiceManager(agentUrl).getTableMappingNames();
	}
	
	public static DataSourceManagerMBean getDataSourceManager(String agentUrl) throws Exception {
		MBeanServerConnection connection = AgentInfoCache.getInstance().get(agentUrl);
		ObjectName dsmon = new ObjectName(DATASOUCE_MANAGER);		
		return MBeanServerInvocationHandler.newProxyInstance(connection, dsmon, DataSourceManagerMBean.class, false);
	}
	
	public static ServiceManagerMBean getServiceManager(String agentUrl) throws Exception {
		MBeanServerConnection connection = AgentInfoCache.getInstance().get(agentUrl);
		ObjectName sm = new ObjectName(SERVICE_MANAGER);		
		return MBeanServerInvocationHandler.newProxyInstance(connection, sm, ServiceManagerMBean.class, false);
	}
	
	public static MailConfigureManagerMBean getMailConfigureManager(String agentUrl) throws Exception {
		MBeanServerConnection connection = AgentInfoCache.getInstance().get(agentUrl);
		ObjectName sm = new ObjectName(MAIL_MANAGER);		
		return MBeanServerInvocationHandler.newProxyInstance(connection, sm, MailConfigureManagerMBean.class, false);
	}
	
	public static Service getService(String agentUrl, String serviceName)  throws Exception {
		MBeanServerConnection connection = AgentInfoCache.getInstance().get(agentUrl);
		ObjectName sm = new ObjectName(String.format(SERVICE_OBJECT_NAME, serviceName));		
		return MBeanServerInvocationHandler.newProxyInstance(connection, sm, Service.class, false);
	}
	
	public static void startService(String url, String moduleName, boolean clean) throws Exception {
		ServiceManagerMBean sm = Utils.getServiceManager(url);
		sm.startModule(moduleName, clean);
	}

	public static void stopService(String url, String serviceName) throws Exception {
		Service service = Utils.getService(url, serviceName);
		service.stop();
	}

	public static void suspendService(String url, String serviceName) throws Exception {
		Service service = Utils.getService(url, serviceName);
		service.suspend();
	}

	public static  void resumeService(String url, String serviceName, boolean skip) throws Exception {
		Service service = Utils.getService(url, serviceName);
		if (skip) {
			service.resume();
		} else {
			service.recover();
		}

	}
	
	public static void deployConsumerModule(String name_url, TXExecutorModule module) throws Exception {
		String datasourceName = module.getStringProperty(Module.DATASOURCE_NAME);
		if (null == datasourceName || datasourceName.length() == 0) {
			throw new IllegalArgumentException("datasource Name should not be empty");
		}
		String moduleName = module.getName();
		if (null == moduleName || moduleName.length() == 0) {
			throw new IllegalArgumentException("Module Name should not be empty");
		}
		String xml = module.toXML();
		ServiceManagerMBean sm = Utils.getServiceManager(name_url);
		sm.deploy(xml);
	}
	
	public static Map<TableMatchEntry, TableMatchEntry> fromProperties(Properties cxt, java.util.Set<String> ignorekeys) {
		Map<TableMatchEntry, TableMatchEntry> result = new HashMap<TableMatchEntry, TableMatchEntry>();
		for (String key : cxt.stringPropertyNames()) {
			if (null != ignorekeys && ignorekeys.contains(key)) {
				continue;
			}
			String value = cxt.getProperty(key);
			int si = key.indexOf('.');
			int ti = value.indexOf('.');
			if (si < 0 || ti < 0) {
				continue;
			}
			String su = key.substring(0, si);
			String st = key.substring(si + 1);
			String tu = value.substring(0, ti);
			String tt = value.substring(ti + 1);
			if (null == su || su.length() == 0 || null == tu || tu.length() == 0)
				throw new IllegalArgumentException("user/schema mapping should not be empty");

			if (null == st || st.length() == 0 || st.equals("*") || st.equals("%")) {
				result.put(new TableMatchEntry(su, "%", TableMatchRule.LIKE), new TableMatchEntry(tu, "%", TableMatchRule.LIKE));
			} else if (null == tt || tt.length() == 0 || tt.equals("*") || tt.equals("%")) {
				throw new IllegalArgumentException("named mapping, target table should not be empty.");
			} else {
				result.put(new TableMatchEntry(su, st, TableMatchRule.EQUAL), new TableMatchEntry(tu, tt, TableMatchRule.EQUAL));
			}
		}
		return result;
	}
	
	public static Properties toProperties(Map<TableMatchEntry, TableMatchEntry> mappingItems) {
		Properties result = new Properties();
		for (TableMatchEntry key : mappingItems.keySet()) {
			TableMatchEntry value = mappingItems.get(key);
			result.setProperty(String.format("%s.%s", key.getSchema(), key.getTable()), String.format("%s.%s", value.getSchema(), value.getTable()));	
		}
		return result;
	}
	
	public static Map<TableMatchEntry, TableMatchEntry> mergeTableMappings(DataSourceManagerMBean srcDataSourceManager,String srcDataSource, Map<TableMatchEntry, TableMatchEntry> mappingItems) throws Exception {
		Map<TableMatchEntry, TableMatchEntry> equalItems = new java.util.HashMap<TableMatchEntry, TableMatchEntry>();
		Map<TableMatchEntry, TableMatchEntry> likeItems = new java.util.LinkedHashMap<TableMatchEntry, TableMatchEntry>();
		Map<TableMatchEntry, TableMatchEntry> result = new java.util.HashMap<TableMatchEntry, TableMatchEntry>();

		for (TableMatchEntry item : mappingItems.keySet()) {
			if (TableMatchRule.EQUAL.equals(item.getMatchRule())) {
				equalItems.put(item, mappingItems.get(item));
			} else if (TableMatchRule.LIKE.equals(item.getMatchRule())) {
				likeItems.put(item, mappingItems.get(item));
			}
		}

		for (TableMatchEntry item : likeItems.keySet()) {
			String src_schema = item.getSchema();
			TableMatchEntry targetItem = likeItems.get(item);
			String target_schema = targetItem.getSchema();
			String[] tables = srcDataSourceManager.getTables(srcDataSource, src_schema, null);
			for (String table : tables) {
				result.put(new TableMatchEntry(src_schema, table, TableMatchRule.EQUAL), new TableMatchEntry(target_schema, table, TableMatchRule.EQUAL));
			}
		}
		// equals over write wild-card
		result.putAll(equalItems);
		return result;
	}

	public static void main(String[] args) {
		System.out.println(pickBit(new BigInteger("6"), 2));
		
	}

}
