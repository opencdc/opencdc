package deltad.core;
import javax.management.ObjectName;


public interface MBean {
	public void registerMBean() throws Exception;
	public void unregisterMBean() throws Exception;
	public ObjectName getObjectName();

}
