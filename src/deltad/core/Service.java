package deltad.core;

import java.util.Properties;

import deltad.core.api.Module;
import deltad.core.api.TableMatchEntry;


public interface Service {
	
	public static final String STATE = "state";
	public static final String DISTANCE_BEHIND = "distance_behind";
	public static final String TX_COUNT = "tx_count";
	public static final String OPERATION_COUNT = "operation_count";
	public static final String INSERT_COUNT = "insert_count";
	public static final String UPDATE_COUNT = "update_count";
	public static final String DELETE_COUNT = "delete_count";
	public static final String PENDING_TX_CACHE_SIZE = "pending_tx_cache_size";
	
	public static final String QUEUE_NAME_DELIMITER = "_";
	
	public void start(boolean clean) throws Exception;
	public void stop() throws Exception;
	public void suspend() throws Exception;
	public String resume() throws Exception;
	public void recover() throws Exception;

	public String getBlockingTXAsString();
	public TXRecoverInfo getBlockingTX();
	public Exception getBlockingCause();
	public ServiceState getState();
	public String getstate();
	public long getStartTime();

	public boolean isAutoRecover();
	public boolean isSuspendOnException();

	public String getServiceName();
	public String getMailConfigurationName();
	public DatabaseType getDatabaseType() throws Exception;
	public String getProperty(String key);
	public String getServiceRuntime(String key);
	public Object[] getServiceSummary();
	public Module getModule();

	public void executeDMLUpdate(String sql) throws Exception;
	public void executeDDL(String sql) throws Exception;
	public Properties getPendingCacheSnapshot();
	public Properties getTableOperationSummary();
	public long getQueueSize() throws Exception;
	public int sourceBlockingQueueSize();
	public void addFilterItem(String key, TableMatchEntry item);
	public void removeFilter(String filterName);

}