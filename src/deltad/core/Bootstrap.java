package deltad.core;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.lang.reflect.Method;
import java.util.Properties;

import javax.management.MBeanServerConnection;
import javax.management.MBeanServerInvocationHandler;
import javax.management.ObjectName;
import javax.management.remote.JMXConnector;
import javax.management.remote.JMXConnectorFactory;
import javax.management.remote.JMXServiceURL;

import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;
import org.pentaho.di.core.Const;
import org.pentaho.di.core.plugins.PluginRegistry;
import org.pentaho.di.core.plugins.RepositoryPluginType;
import org.pentaho.di.repository.RepositoriesMeta;
import org.pentaho.di.repository.Repository;
import org.pentaho.di.repository.RepositoryMeta;
import org.pentaho.di.repository.filerep.KettleFileRepository;

public class Bootstrap {

	public static final String JMX_PORT_KEY = "com.sun.management.jmxremote.port";
	public static final String JMX_ADDRESS_KEY = "com.sun.management.jmxremote.address";

	public static final String SERVER_HOME_KEY = "dib.server.home";
	public static final String DIB_HOME = System.getProperty("DIB_HOME");
	public static final String SERVER_HOME = DIB_HOME + File.separator + "server.home";
	
	public static final boolean KETTLE_MODE = true;	
	
	public static String getRepositoryHome() {
		return KETTLE_MODE ? Bootstrap.getDefaultRepository().getRepositoryMeta().getBaseDirectory() + File.separator : null;
	}	

	public static void printUsage() {
		StringBuffer sb = new StringBuffer("Usage:  dib <cmd> <server>\n\n");
		sb.append("cmd: start|stop|console|failover\n\n");

		sb.append("Start DIB Server: ");
		sb.append("    dib start\n");

		sb.append("Stop DIB Agent Server: ");
		sb.append("     dib stop\n");

		sb.append("Start DIB console: ");
		sb.append("         dib console\n");

		sb.append("Failover DIB Agent Server:");
		sb.append("  dib failover\n");

		sb.append("Config System Properties: ");
		sb.append("  dib config <agent_name>\n");

		sb.append("initialize Source Database: ");
		sb.append("  dib initSourceDB\n");

		sb.append("initialize Target Database: ");
		sb.append("  dib initTargetDB\n");

		System.out.println(sb.toString());
	}

	public static void main(String[] args) throws Exception {
		String cmd = null;
		String option = null;

		if (args.length > 0) {
			cmd = args[0];
			if (args.length > 1)
				option = args[1];
		} else {
			printUsage();
			return;
		}

		if ("start".equalsIgnoreCase(cmd)) {
			startServer();
		} else if ("stop".equalsIgnoreCase(cmd)) {
			stopServer();
		} else if ("console".equalsIgnoreCase(cmd)) {
			startConsole();
		} else if ("failover".equalsIgnoreCase(cmd)) {
			startFailover(option);
		} else if ("config".equalsIgnoreCase(cmd)) {
			config(option);
		} else if ("initSourceDB".equalsIgnoreCase(cmd)) {
			initSourceDB();
		} else if ("initTargetDB".equalsIgnoreCase(cmd)) {
			initTargetDB();
		} else {
			printUsage();
			return;
		}
	}

	private static void initSourceDB() throws Exception {
		System.out.println("Begin to initialize source database, please check the DB log configuration by the following directions: ");
		System.out.println("Open sqlplus and log as sysdba: sqlplus sys/sys as sysdba");
		System.out.println("Check if archive log is enabled:");
		System.out.println("SQL>select log_mode from v$database;");
		System.out.println("SQL>archive log list;");
		System.out.println("If not, enable it:");
		System.out.println("SQL> shutdown immediate");
		System.out.println("SQL> startup mount");
		System.out.println("alter database archivelog;");
		System.out.println("alter database force logging;");
		System.out.println("Check it again:");
		System.out.println("SQL>select log_mode from v$database;");
		System.out.println("Open database:");
		System.out.println("SQL>alter database open;");
		System.out.println("Make sure the above steps is done? (Y/N)");
		byte buffer[] = new byte[512];
		int count = System.in.read(buffer);
		if (!(count > 0 && (buffer[0] == 'y' || buffer[0] == 'Y'))) {
			System.out.println("Initialization not finished.");
			return;
		}
		System.out.println("please input password for sys");
		count = System.in.read(buffer);

	}

	private static void initTargetDB() throws Exception {

	}

	private static void config(String agentName) throws Exception {
		if (null == agentName || agentName.length() == 0) {
			System.out.println("dib config <agent_name>");
			return;
		}
		String localAddress = java.net.InetAddress.getLocalHost().getHostAddress();
		Properties config = new Properties();
		config.load(new FileInputStream(Bootstrap.SERVER_HOME + "/conf/agent.properties"));
		config.setProperty("com.sun.management.jmxremote.address", localAddress);
		config.setProperty("agent.name", agentName);
		config.store(new FileOutputStream(Bootstrap.SERVER_HOME + "/conf/agent.properties"), null);
	}

	private static void initLog(String server_home) {
		System.setProperty(SERVER_HOME_KEY, server_home);
		System.out.println("DIB_HOME" + "=" + DIB_HOME);
		System.out.println(SERVER_HOME_KEY + "=" + server_home);
		PropertyConfigurator.configure(server_home + "/conf/log4j.properties");
	}

	private static void startFailover(String server) {
		initLog(SERVER_HOME);
		new FailoverMonitor(server).start();
	}

	private static void startConsole() throws Exception {
		Class<?> claz = Class.forName("deltad.console.Console");
		Method method = claz.getDeclaredMethod("launch");
		method.invoke(claz);
	}

	private static void startServer() throws Exception {
		String server_class = "deltad.core.Agent";
		initLog(SERVER_HOME);
		int jmxport = Integer.parseInt(Agent.getProperty(JMX_PORT_KEY));
		Class<?> claz = Class.forName(server_class);
		Method method = claz.getDeclaredMethod("getInstance");
		AgentMBean agent = (AgentMBean) method.invoke(claz);
		Logger logger = Logger.getLogger(Bootstrap.class.getName());
		if (null != agent) {
			agent.start();
			logger.info(String.format("Server %s is started on %d.", agent.getServerName(), jmxport));
		}
	}

	private static void stopServer() throws Exception {
		initLog(SERVER_HOME);
		String ip = "127.0.0.1";
		int jmxport = Integer.parseInt(Agent.getProperty(JMX_PORT_KEY));
		String jmxUrl = String.format("service:jmx:rmi:///jndi/rmi://%s:%s/jmxrmi", ip, jmxport);

		JMXServiceURL jmxServiceURL = new JMXServiceURL(jmxUrl);
		JMXConnector jmxConnector = JMXConnectorFactory.connect(jmxServiceURL, null);
		MBeanServerConnection mbeanServConn = jmxConnector.getMBeanServerConnection();
		ObjectName mbeanName = new ObjectName(Utils.AGENT_DOMAIN_NAME + "AgentName=singleton");

		AgentMBean proxy = MBeanServerInvocationHandler.newProxyInstance(mbeanServConn, mbeanName, AgentMBean.class, false);
		try {
			proxy.shutdown();
		} catch (Exception e) {
			Logger logger = Logger.getLogger(Bootstrap.class.getName());
			logger.error(String.format("Shut down server with error."), e);
		}
	}

	private static KettleFileRepository defaultRepository;

	public static synchronized KettleFileRepository getDefaultRepository() {
		return null == defaultRepository ? (KettleFileRepository)openRepository("main", "admin", null) : defaultRepository;		
	}

	public static Repository openRepository(String repositoryName, String user, String pass) {
		if (Const.isEmpty(repositoryName))
			return null;
		
		RepositoriesMeta repositoriesMeta = new RepositoriesMeta();
		try {			
			repositoriesMeta.readData();
			RepositoryMeta repositoryMeta = repositoriesMeta.findRepository(repositoryName);
			PluginRegistry registry = PluginRegistry.getInstance();
			Repository repository = registry.loadClass(RepositoryPluginType.class, repositoryMeta, Repository.class);
			repository.init(repositoryMeta);
			repository.connect(user, pass);
			return repository;
		} catch (Exception e) {
			System.out.println("can not open the default workspace main, please check...");
			e.printStackTrace();
		}
		
		return null;
	}

}
