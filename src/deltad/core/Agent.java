package deltad.core;

import java.io.File;
import java.io.FileInputStream;
import java.util.Properties;

import javax.jms.JMSException;

import org.apache.log4j.Logger;

import deltad.core.dsm.DataSourceManager;


/**
 * Agent also as a service manager
 * @author haozhu
 */
public class Agent extends AbstractMBeanSupport implements AgentMBean {
	private static final Logger logger = Logger.getLogger(Agent.class.getName());

	//private final ConnectionFactory connectionFactory;
	//protected final Connection connection;
	//private final Session session;
	//private final MessageConsumer adminConsumer;
	//private final Destination cmdQueue;
	protected ServiceManager serviceManager;
	
//	public static String AGENT_DOMAIN_NAME = "DIBServer:";
//	public static String AGENT_OBJECT_NAME = AGENT_DOMAIN_NAME + "AgentName=singleton";
	public static String SERVER_HOME = System.getProperty(Bootstrap.SERVER_HOME_KEY);
	
	public static String DIB_CONF_DIR = SERVER_HOME + File.separator + "conf/";
	public static String DIB_DATASOURCE_DIR = Bootstrap.KETTLE_MODE ? Bootstrap.getRepositoryHome() : DIB_CONF_DIR + "datasource/";
	public static String TABLE_MAPPING_DIR = Bootstrap.KETTLE_MODE ? Bootstrap.getRepositoryHome() : DIB_CONF_DIR + "tableMapping/";
	public static String MAIL_CONF_DIR = Bootstrap.KETTLE_MODE ? Bootstrap.getRepositoryHome() : DIB_CONF_DIR + "mail/";
	public static String SERVICE_DEPLOY_DIR = Bootstrap.KETTLE_MODE ? Bootstrap.getRepositoryHome() : SERVER_HOME + File.separator + "deploy/";
	public static String DIB_AGENT_CONF = DIB_CONF_DIR + "agent.properties";
	
	private static final Properties agentInfo = new Properties();
	
	public static Agent singleton = null;
	
	public static synchronized Agent getInstance() throws Exception {
		if (singleton == null)
			singleton = new Agent();
		return singleton;
	}
	
	static {
		try {
			agentInfo.load(new FileInputStream(DIB_AGENT_CONF));
		} catch (Exception e) {
			logger.error("load agent configuration failed...", e);
		} 
	}
	
	public Agent() throws Exception {
		//fixed agent name
		super(Utils.AGENT_OBJECT_NAME);
		
		//agentInfo.load(new FileInputStream(DIB_AGENT_CONF));
		//String cmdQueueName = getAgentProperty("cmd.queue.name");
		
		CacheManagerBuilder.buildTXCacheManager(this, Utils.AGENT_DOMAIN_NAME + "type=CacheManager,cm=singleton");
		
		//this.connection = connectionFactory.createConnection();
		//this.session = connection.createSession(true, Session.CLIENT_ACKNOWLEDGE);
		//this.cmdQueue = session.createQueue(cmdQueueName);
		//this.adminConsumer = session.createConsumer(cmdQueue);
		DataSourceManager.getInstance();
		initServiceManager();
		addShutdownHook();
		//adminConsumer.setMessageListener(new AdminCommandListener());	
		registerMBean();
	}
	
	public synchronized void initServiceManager() throws Exception {
		if (null == getServiceManager())
			serviceManager = new ServiceManager();
	}
	
	private void addShutdownHook() {
		Runnable t = new Runnable() {
			public void run() {
				try {
					shutdown();
				} catch (Exception e) {
					logger.info("shut down with exception ", e);
				}
			}
		};
		Runtime.getRuntime().addShutdownHook(new Thread(t));
	}
	
	@Override
	public String getAgentProperty(String key) {
		return getProperty(key);
	}
	
	public static String getProperty(String key) {
		return agentInfo.getProperty(key);
	}
	
	@Override
	public void shutdown() throws Exception {
		logger.info("shutting down agent: " + getServerName());
		this.serviceManager.shutdown();
		//this.connection.close();
		AbstractMBeanSupport.getConnectorServer().stop();
	}

	@Override
	public void start() throws JMSException {
		logger.info("starting agent: " + getServerName());
		//this.connection.start();
	}
	
	@Override
	public String getServerName() {
		return getAgentProperty("agent.name");
	}
	
	public ServiceManager getServiceManager() {
		return this.serviceManager;
	}

}
