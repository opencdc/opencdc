package deltad.core;

import java.lang.management.ManagementFactory;

import javax.management.ObjectName;

import org.apache.log4j.Logger;

import net.sf.ehcache.CacheManager;
import net.sf.ehcache.config.Configuration;
import net.sf.ehcache.config.DiskStoreConfiguration;

public class CacheManagerBuilder {
	private static final Logger logger = Logger.getLogger(CacheManagerBuilder.class.getName());
	
	public static final String DISK_STORE_PATH = "cachemanager.disk.store.path";
	private static CacheManager instance;
	
	public static synchronized CacheManager buildTXCacheManager(Agent agent, String oname) throws Exception {
		if (null != instance)
			return instance;
		
		String diskStorePath = agent.getAgentProperty(DISK_STORE_PATH) == null ? Agent.SERVER_HOME : agent.getAgentProperty(DISK_STORE_PATH);		
		DiskStoreConfiguration dsc = new DiskStoreConfiguration().path(diskStorePath);
		Configuration config = new Configuration().diskStore(dsc);
		instance = CacheManager.create(config);
		net.sf.ehcache.management.CacheManager cacheManagerBean = new net.sf.ehcache.management.CacheManager(instance);
		ManagementFactory.getPlatformMBeanServer().registerMBean(cacheManagerBean, new ObjectName(oname));
		
		logger.info("CacheManager is created!");
		return instance;
	}
	
	public static synchronized CacheManager getInstance() {
		if (null == instance)
			throw new IllegalStateException("CacheManager is not initialized yet.");
		
		return instance;
	}

}
