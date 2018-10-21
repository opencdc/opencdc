package deltad.core;

import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.log4j.Logger;

import deltad.core.api.Module;



public class ServiceMonitor implements Runnable {
	private static final Logger logger = Logger.getLogger(ServiceMonitor.class.getName());

	private final Map<String, Service> mserviceMap;
	private final Map<String, Module> modules;
	private final ServiceManager serviceManager;
	
	private Queue<ServiceEvent> events= new LinkedList<ServiceEvent>();
	private List<ServiceEventListener> listeners = new LinkedList<ServiceEventListener>();

	private AtomicBoolean running = new AtomicBoolean(true);
	
	private static ServiceMonitor instance;

	public ServiceMonitor(Map<String, Service> mserviceMap, Map<String, Module> modules, ServiceManager manager) {
		this.mserviceMap = mserviceMap;
		this.modules = modules;
		this.serviceManager = manager;
	}

	private void restartService(Service service) throws Exception {
		logger.warn("restartService() restarting Service " + service.getServiceName());
		String serviceName = service.getServiceName();
		service.stop();
		serviceManager.startModule(serviceName, false);
	}

	public void stop() {
		running.set(false);
	}
	
	public void sendEvent(ServiceEvent event) {
		this.events.offer(event);
	}
	
	public void registerServiceStateListener(ServiceEventListener listener) {
		listeners.add(listener);
	}
	
	public void handleEvent(ServiceEvent event) {	
		if (listeners.size() > 0) {
			for (ServiceEventListener listener : listeners) {
				listener.handleEvent(event);
			}
		}
	}
	
	private void processEvent() {
		if (!events.isEmpty()) {
			ServiceEvent event = events.poll();
			//logger.info("processEvent() :" + event);
			if (null != event)
				handleEvent(event);
		}
	}
	
	private boolean isAutoRecover(Module module) {
		if (module.getBooleanProperty(Module.SUSPEND_ON_EXCEPTION))
			return false;
		
		return (module.getBooleanProperty(Module.AUTO_RECOVER));
	}
	
	private void processRecover() {
		for (Module module : modules.values()) {
			String moduleName = module.getName().endsWith(ServiceManager.SERVICE_MODULE_FILE_EXT) ? module.getName() : module.getName() + ServiceManager.SERVICE_MODULE_FILE_EXT;
			Service service = mserviceMap.get(moduleName);
			if (null == service) {
				if (isAutoRecover(module)) {
					try {
						serviceManager.startModule(moduleName, false);
					} catch (Exception e) {
						logger.warn("run() startModule failed " + moduleName, e);
					}
				}
				continue;
			}

			ServiceState state = service.getState();
			if (service.isAutoRecover() && !service.isSuspendOnException()) {
				try {
					switch (state) {
					case SUSPENDED:
						TXRecoverInfo recoverInfo = service.getBlockingTX();
						if (null == recoverInfo) {
							//service.recover();//do nothing
							break;
						}

						if (null == recoverInfo.getBlockingXID() || service.getBlockingCause() instanceof java.sql.SQLRecoverableException 
								|| service.getBlockingCause() instanceof org.hibernate.exception.JDBCConnectionException) {
							
							restartService(service);
							break;
						}
						break;
					case STOPPED:
						restartService(service);
						break;
					}
				} catch (Exception e) {
					logger.warn("run() recover/restart failed " + service.getServiceName(), e);

				}

			}

		}

	}

	@Override
	public void run() {
		if (running.get()) {
			//logger.warn("---------------------ServiceMonitor running...");
			processRecover();
			processEvent();

		}
	}
	
	public static synchronized ServiceMonitor getInstance(Map<String, Service> mserviceMap, Map<String, Module> modules, ServiceManager manager) {
		if (null == instance) {
			instance = new ServiceMonitor(mserviceMap, modules, manager);
		}
		return instance;
	}
	
	public static synchronized ServiceMonitor getInstance() throws Exception {
		if (null == instance)
			throw new IllegalStateException("ServiceMonitor is not initialized yet.");
		return instance;
	}

}
