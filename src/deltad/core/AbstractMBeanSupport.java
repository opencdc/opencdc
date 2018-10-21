package deltad.core;

import java.lang.management.ManagementFactory;
import java.rmi.registry.LocateRegistry;
import java.util.HashMap;

import javax.management.MBeanServer;
import javax.management.ObjectName;
import javax.management.remote.JMXConnectorServer;
import javax.management.remote.JMXConnectorServerFactory;
import javax.management.remote.JMXServiceURL;

/**
 * AbstractMBeanSupport
 * @author haozhu
 */
public class AbstractMBeanSupport implements MBean {
	
	private final ObjectName objectName;
	

	@Override
	public ObjectName getObjectName() {
		return objectName;
	}
	
	public AbstractMBeanSupport(String name) throws Exception {
		this.objectName = new ObjectName(name);
	}

	@Override
	public void registerMBean() throws Exception {
		getMBeanServer().registerMBean(this, getObjectName());

	}

	@Override
	public void unregisterMBean() throws Exception {
		getMBeanServer().unregisterMBean(getObjectName());

	}
	
	private static MBeanServer mbeanServer;
	private static JMXConnectorServer cs;
	
	public static synchronized JMXConnectorServer getConnectorServer() throws Exception {
		if (null == cs) {
			int jmxport = Integer.parseInt(Agent.getProperty(Bootstrap.JMX_PORT_KEY));
			String jmx_address = Agent.getProperty(Bootstrap.JMX_ADDRESS_KEY);
			System.setProperty("java.rmi.server.randomIDs", "true");
			LocateRegistry.createRegistry(jmxport);
			JMXServiceURL url = new JMXServiceURL(String.format("service:jmx:rmi:///jndi/rmi://%s:%d/jmxrmi",jmx_address, jmxport));
			MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();
			HashMap<String, Object> env = new HashMap<String, Object>();
			cs = JMXConnectorServerFactory.newJMXConnectorServer(url, env, mbs);
			
			cs.start();
		}
		
		return cs;	
	}
	
	public static synchronized MBeanServer getMBeanServer() throws Exception {
		if (null == mbeanServer) {
			mbeanServer = getConnectorServer().getMBeanServer();
		}
		return mbeanServer;
	}

}
