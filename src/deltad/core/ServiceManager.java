package deltad.core;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.Writer;
import java.lang.reflect.Constructor;
import java.sql.PreparedStatement;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import javax.jms.ConnectionFactory;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.log4j.Logger;
import org.hibernate.SQLQuery;
import org.hibernate.StatelessSession;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.repository.filerep.KettleFileRepository;

import deltad.core.api.Module;
import deltad.core.api.TXExecutorModule;
import deltad.core.api.TXServiceModule;
import deltad.core.api.TableMatchEntry;
import deltad.core.api.TableMatchEntry.TableMatchRule;
import deltad.core.dsm.DataSourceManager;
import deltad.core.mcm.MailConfigureManager;
import deltad.core.scheduling.DeployingPlanExecutor;



public class ServiceManager extends AbstractMBeanSupport implements ServiceManagerMBean {
	private static final Logger logger = Logger.getLogger(ServiceManager.class.getName());
	public static final String TRANSACTION_ENGINE_FILE_EXT = ".dte";
	public static final String SERVICE_MODULE_FILE_EXT = Bootstrap.KETTLE_MODE ? TRANSACTION_ENGINE_FILE_EXT : ".xml";
	
	//public static final String TRANSACTION_CONSUMER_FILE_EXT = ".dtc";
	
	public static final String PROPERTIES_FILE_EXT = ".properties";
	public static final String PLAN_SCRIPT_FILE_EXT = ".rps";
	public static final String LOGQUEUESUFFIX = "_latest_scn";

	public static String SERVICE_MANAGER_OBJECT_NAME = Utils.AGENT_DOMAIN_NAME + "type=ServiceManager,name=singleton";
	
	protected final Map<String, Module> modules = new HashMap<String, Module>();
	protected final Map<String, Service> mserviceMap = new HashMap<String, Service>();

//	protected final Map<String, TXProducerEngineModule> producerModules = new HashMap<String, TXProducerEngineModule>();
//	protected final Map<String, ProducerServiceMBean> producerServiceMap = new HashMap<String, ProducerServiceMBean>();
//	
//	protected final Map<String, TXConsumerModule> consumerModules = new HashMap<String, TXConsumerModule>();
//	protected final Map<String, ConsumerServiceMBean> consumerServiceMap = new HashMap<String, ConsumerServiceMBean>();

	protected final Map<String, DeployingPlanExecutor> planMap = new HashMap<String, DeployingPlanExecutor>();
	protected final Map<String, Properties> tableMappings = new HashMap<String, Properties>();

	protected ServiceMonitor monitor = null;
	protected final ScheduledExecutorService scheduler = new ScheduledThreadPoolExecutor(1);

	protected final ScheduledExecutorService planScheduler = new ScheduledThreadPoolExecutor(8);

	public ServiceManager() throws Exception {
		super(SERVICE_MANAGER_OBJECT_NAME);
		loadModules();
		loadMappings();
		registerMBean();
		MailConfigureManager.getInstance();
		// -----------------------------------------------------------------
		monitor = ServiceMonitor.getInstance(mserviceMap, modules, this);
		monitor.registerServiceStateListener(new DefaultServiceEventListener(MailConfigureManager.getInstance()));
		scheduler.scheduleWithFixedDelay(monitor, -1, 10, TimeUnit.SECONDS);
	}

	@Override
	public void schedulePlan(String planname, String scripts) throws Exception {
		logger.info("deploying plan: " + planname);
		DeployingPlanExecutor plan = new DeployingPlanExecutor(this, planname, null, scripts);
		schedulePlan(planname, plan);

		// do NOT write as file anymore
		/*
		String fileName = planname.endsWith(SERVICE_MODULE_FILE_EXT) ? planname : planname + PLAN_SCRIPT_FILE_EXT;
		Writer writer = new FileWriter(Agent.SERVICE_DEPLOY_DIR + fileName);
		writer.write(scripts);
		writer.close();
		*/
	}

	@Override
	public void schedulePlanForName(String planname) throws Exception {
		logger.info("deploying plan: " + planname);
		String fileName = planname.endsWith(SERVICE_MODULE_FILE_EXT) ? planname : planname + PLAN_SCRIPT_FILE_EXT;
		DeployingPlanExecutor plan = new DeployingPlanExecutor(this, planname, null, new File(Agent.SERVICE_DEPLOY_DIR + fileName));
		schedulePlan(planname, plan);
	}

	private void schedulePlan(String name, DeployingPlanExecutor plan) {
		if (planMap.containsKey(name))
			throw new RuntimeException(String.format("plan with name %s already scheduled", name));
		
		plan.schedule(planScheduler);
		this.planMap.put(plan.getName(), plan);
	}
	
	@Override
	public boolean cancelPlan(String name) {
		DeployingPlanExecutor plan = planMap.remove(name);
		if (null == plan)
			return true;
		
		plan.stop();
		
		ScheduledFuture<String> scheduledFuture = plan.getFuture();
		if (null == scheduledFuture)
			return true;
		
		return scheduledFuture.cancel(true);
	}
	
	@Override
	public String[] getPlanNames() {
		return this.planMap.keySet().toArray(new String[planMap.size()]);
	}
	
	@Override
	public Properties getPlanContext(String planName) {
		DeployingPlanExecutor exe = planMap.get(planName);
		return exe.getContext();
	}
	
	@Override
	public String[] getPlanResult(String planName) throws Exception {
		DeployingPlanExecutor exe = planMap.get(planName);
		return exe == null ? null : exe.getResult();
	}
	
	@Override
	public String[] getPlanScripts(String planName) {
		DeployingPlanExecutor exe = planMap.get(planName);
		return exe == null ? null : exe.getScripts();
	}
	
	@Override
	public void deploy(String xml) throws Exception {		
		Module module = upload(xml);
		if (Bootstrap.KETTLE_MODE) {
			saveDefaultEngine((TXServiceModule)module);
		} else {
			deploy(module);
		}
		
	}
	
	public Module upload(String xml) throws Exception {
		Module module =  Module.fromXML(xml);
		String name = module.getName();
		logger.info("uploading module: " + name);
		String moduleName = Bootstrap.KETTLE_MODE ? name.endsWith(TRANSACTION_ENGINE_FILE_EXT) ? name : name + TRANSACTION_ENGINE_FILE_EXT : name.endsWith(SERVICE_MODULE_FILE_EXT) ? name : name + SERVICE_MODULE_FILE_EXT;
		File moduleFile = new File(Agent.SERVICE_DEPLOY_DIR + moduleName);
		Writer writer = new FileWriter(moduleFile);
		try {
			writer.write(module.toXML());
		} catch (Exception e) {
			throw e;
		} finally {
			writer.close();
		}
		return module;
	}
	
	private void deploy(Module module) throws Exception {
		String name = module.getName();
		logger.info("deploying module: " + name);
		String moduleName = Bootstrap.KETTLE_MODE ? name.endsWith(TRANSACTION_ENGINE_FILE_EXT) ? name : name + TRANSACTION_ENGINE_FILE_EXT : name.endsWith(SERVICE_MODULE_FILE_EXT) ? name : name + SERVICE_MODULE_FILE_EXT;
		//String moduleName = name.endsWith(SERVICE_MODULE_FILE_EXT) ? name : name + SERVICE_MODULE_FILE_EXT;
		if (!Bootstrap.KETTLE_MODE) {
			if (modules.containsKey(moduleName)) {
				throw new RuntimeException("a module with the same name already deployed");
			}
		}
		if (module instanceof TXServiceModule) {
			//source additions
			TXServiceModule producerModule = (TXServiceModule)module;
			String srcds = producerModule.getStringProperty(Module.DATASOURCE_NAME);
			if (null == srcds || srcds.length() == 0)
				throw new IllegalArgumentException("datasource should not be empty");
			if (DatabaseType.SQLSERVER.equals(DataSourceManager.getInstance().getDatabaseType(srcds))) {
				setupCDC(producerModule);
			}

			String agentID = Agent.getInstance().getAgentProperty("agent.name");
			remove_md_filters(agentID, moduleName, producerModule.getStringProperty(Module.DATASOURCE_NAME));
			seed_md_filters(agentID, moduleName, producerModule);
			logger.info("seed_md_filters OK " + moduleName);	
		}
		
		this.modules.put(moduleName, module);		
		logger.info("deployed module OK " + module.getName());
	}
	
	private void saveDefaultEngine(TXServiceModule default_module) throws Exception {
		//undeploy(default_module.getName());
		deploy(default_module);
	}
	
	@Override
	public void deployTableMapping(String name, Properties mapping) throws Exception {
		if (mapping.size() == 0)
			return;
		
		FileOutputStream out = new FileOutputStream(Agent.TABLE_MAPPING_DIR + name + PROPERTIES_FILE_EXT);
		try {
			mapping.store(out, "table mapping file");
			tableMappings.put(name, mapping);
		} catch (Exception e) {
			throw e;
		} finally {
			if(null != out)
				out.close();
		}
	}
	
	@Override
	public void unDeployTableMapping(String name) {
		logger.info("undeploying tableMapping: " + name);
		tableMappings.remove(name);
		File file = new File(Agent.TABLE_MAPPING_DIR + name);
		if (file.exists()) {
			file.delete();
		}
	}
	
	@Override
	public String[] getTableMappingNames() {
		return tableMappings.keySet().toArray(new String[tableMappings.size()]);
	}
	
	@Override
	public Properties getTableMapping(String key) {
		return tableMappings.get(key);
	}
	
	@Override
	public void deployForName(String name) throws Exception {
		String fileName = name.endsWith(SERVICE_MODULE_FILE_EXT) ? name : name + SERVICE_MODULE_FILE_EXT;
		StringBuffer buffer = new StringBuffer();
		java.io.BufferedReader reader = new java.io.BufferedReader(new java.io.FileReader(Agent.SERVICE_DEPLOY_DIR + fileName));
		String line = reader.readLine();
		while (null != line) {
			buffer.append(line).append("\n");
			line = reader.readLine();
		}
		Module module =  Module.fromXML(buffer.toString());
		deploy(module);
	}

	@Override	
	public void undeploy(String name) throws Exception {
		logger.info("undeploying module: " + name);
		//String moduleName = name.endsWith(SERVICE_MODULE_FILE_EXT) ? name : name + SERVICE_MODULE_FILE_EXT;
		String moduleName = Bootstrap.KETTLE_MODE ? name.endsWith(TRANSACTION_ENGINE_FILE_EXT) ? name : name + TRANSACTION_ENGINE_FILE_EXT : name.endsWith(SERVICE_MODULE_FILE_EXT) ? name : name + SERVICE_MODULE_FILE_EXT;
			
		Service service = mserviceMap.get(moduleName);
		if (null != service && service.getState() != ServiceState.STOPPED) {
			throw new IllegalStateException(String.format("service %s is still active, shut it down first.", service.getServiceName()));
		}
		
		Module mm = modules.get(moduleName);
		if (null == mm)
			return;
		
		if (mm instanceof TXExecutorModule) {
			removeServiceLog(mm);
			
		} else if (mm instanceof TXServiceModule) {
			TXServiceModule producerModule = (TXServiceModule)mm;
			String datasource = producerModule.getStringProperty(Module.DATASOURCE_NAME);
			String agentID = Agent.getInstance().getAgentProperty("agent.name");
			remove_md_filters(agentID, moduleName, datasource);
			logger.info("remove system log table OK " + moduleName);
			
			if (DatabaseType.SQLSERVER.equals(DataSourceManager.getInstance().getDatabaseType(datasource))) {
				disableCDC(producerModule);
			}
			//purge from source side, do not purger from target
			for (String item : producerModule.getEntryNames()) {
				if (!TXServiceModule.DEFAULT_FILTER_ENGINE.equals(item)) {
					purgeQueue(mm.getName(), item);
				}
			}			
			purgeQueue(mm.getName(), mm.getName() + LOGQUEUESUFFIX);			
			logger.info("purgeQueue OK " + mm.getName());
		}
		
		File module = new File(Agent.SERVICE_DEPLOY_DIR + moduleName);
		if (module.exists()) {
			module.delete();
		}
		
		this.mserviceMap.remove(moduleName);
		this.modules.remove(moduleName);
		logger.info("undeployed module " + mm.getName());
	}

	public static final String PRODUCER_SERVICE_CLASS = "deltad.engine.TransactionService";
	public static final String CONSUMER_SERVICE_CLASS = "rdi.target.ConsumerService";

	public Service submitMirroringService(Module module) throws Exception {
		
		// reflect
		Service service;
		Class<?> claz;
		if (module instanceof TXServiceModule) {
			((TXServiceModule)module).consolidate();
			claz = Thread.currentThread().getContextClassLoader().loadClass(PRODUCER_SERVICE_CLASS);
		} else if (module instanceof TXExecutorModule) {
			claz = Thread.currentThread().getContextClassLoader().loadClass(CONSUMER_SERVICE_CLASS);
		} else {
			throw new IllegalArgumentException("invalid service type " + module.getClass().getName());
		}
		
		String url = module.getStringProperty(Module.ASSOCIATED_MQ_URL);
		String user = module.getStringProperty(Module.ASSOCIATED_MQ_USERNAME);
		String password = module.getStringProperty(Module.ASSOCIATED_MQ_PASSWORD);
		ConnectionFactory connectionFactory = new ActiveMQConnectionFactory(user, password, url);

		Constructor<?> constructor = claz.getDeclaredConstructor(new Class[] { javax.jms.Connection.class, String.class, Module.class });
		service = (Service) constructor.newInstance(new Object[] { connectionFactory.createConnection(), String.format(Utils.SERVICE_OBJECT_NAME, module.getName()), module });
		logger.info("submit a tx service module : " + module.getName());
		//logger.debug("submitMirroringService: \n" + module.toXML());

		//service.start(clean);
		String name = service.getServiceName();
		//String moduleName = name.endsWith(SERVICE_MODULE_FILE_EXT) ? name : name + SERVICE_MODULE_FILE_EXT;
		String moduleName = Bootstrap.KETTLE_MODE ? name.endsWith(TRANSACTION_ENGINE_FILE_EXT) ? name : name + TRANSACTION_ENGINE_FILE_EXT : name.endsWith(SERVICE_MODULE_FILE_EXT) ? name : name + SERVICE_MODULE_FILE_EXT;
		mserviceMap.put(moduleName, service);
		return service;
	}

	@Override
	public void startModule(String name, boolean clean) throws Exception {
		String moduleName = Bootstrap.KETTLE_MODE ? name.endsWith(TRANSACTION_ENGINE_FILE_EXT) ? name : name + TRANSACTION_ENGINE_FILE_EXT : name.endsWith(SERVICE_MODULE_FILE_EXT) ? name : name + SERVICE_MODULE_FILE_EXT;
		//String moduleName = name.endsWith(SERVICE_MODULE_FILE_EXT) ? name : name + SERVICE_MODULE_FILE_EXT;
		Module module = this.modules.get(moduleName);
		if (null == module)
			return;
		
		Service service = submitMirroringService(module);
		if (null != service)
			service.start(clean);	
	}
	
	@Override
	public Service getService(String moduleName) {
		return this.mserviceMap.get(moduleName);
	}

	public void removeService(String name) {
		String moduleName = name.endsWith(SERVICE_MODULE_FILE_EXT) ? name : name + SERVICE_MODULE_FILE_EXT;
		this.mserviceMap.remove(moduleName);
	}

	@Override
	public boolean started(String name) {
		String moduleName = name.endsWith(SERVICE_MODULE_FILE_EXT) ? name : name + SERVICE_MODULE_FILE_EXT;
		return getService(moduleName) != null;
	}

	@Override
	public String displayModule(String name) {
		String moduleName = Bootstrap.KETTLE_MODE ? name.endsWith(TRANSACTION_ENGINE_FILE_EXT) ? name : name + TRANSACTION_ENGINE_FILE_EXT : name.endsWith(SERVICE_MODULE_FILE_EXT) ? name : name + SERVICE_MODULE_FILE_EXT;
		
//		if (!key.endsWith(SERVICE_MODULE_FILE_EXT))
//			key = key + SERVICE_MODULE_FILE_EXT;
		Module module = modules.get(moduleName);
		return module == null ? null : module.toXML();
	}

	@Override
	public String[] getModuleNames() throws Exception {
		return this.modules.keySet().toArray(new String[modules.size()]);
	}
	
	protected void removeServiceLog(Module module) throws Exception {
		String sql_insert = String.format("delete from %s where service_name = '%s'", Utils.DIB_TARGET_LOG, module.getName());
		String datasource = module.getStringProperty(Module.DATASOURCE_NAME);
		StatelessSession hibSession = DataSourceManager.getInstance().openStatelessSession(datasource);
		try {
			org.hibernate.Transaction transaction = hibSession.beginTransaction();
			SQLQuery query = hibSession.createSQLQuery(sql_insert);
			query.executeUpdate();
			transaction.commit();
		} catch (Exception e) {
			throw e;
		} finally {
			hibSession.close();
		}
	}

	protected void loadModules() {
		File dir = new File(Agent.SERVICE_DEPLOY_DIR);
		File[] files = dir.listFiles(new FilenameFilter() {
			public boolean accept(File dir, String name) {
				return name.endsWith(Bootstrap.KETTLE_MODE ? TRANSACTION_ENGINE_FILE_EXT : SERVICE_MODULE_FILE_EXT);
			}
		});

		if (null == files || files.length == 0)
			return;

		for (File f : files) {
			try {
				this.modules.put(f.getName(), loadModule(f));
			} catch (IOException e) {
				// do nothing
				logger.info("fail to load module " + f.getName(), e);
			}
		}
	}
	
	protected void loadMappings() {
		File dir = new File(Agent.TABLE_MAPPING_DIR);
		File[] files = dir.listFiles(new FilenameFilter() {
			public boolean accept(File dir, String name) {
				return name.endsWith(PROPERTIES_FILE_EXT);
			}
		});

		if (null == files || files.length == 0)
			return;

		for (File f : files) {
			FileInputStream in = null;
			try {
				Properties tableMapping = new Properties();
				in = new FileInputStream(f);
				tableMapping.load(in);
				tableMappings.put(f.getName(), tableMapping);
			} catch (IOException e) {
				// do nothing
				logger.info("fail to load module " + f.getName(), e);
			} finally {
				try {
					in.close();
				} catch (IOException e) {
				}
			}
		}
	}

	public static Module loadModule(File moduleFile) throws IOException {
		return Module.fromXML(loadModuleAsString(moduleFile));
	}

	public String getProperty(String amoduleName, String key) {
		String moduleName = amoduleName.endsWith(SERVICE_MODULE_FILE_EXT) ? amoduleName : amoduleName + SERVICE_MODULE_FILE_EXT;
		Module module = this.modules.get(moduleName);
		return module == null ? null : module.getStringProperty(key);

	}

	public static String loadModuleAsString(File moduleFile) throws IOException {
		BufferedReader reader = new BufferedReader(new FileReader(moduleFile));
		StringBuffer sb = new StringBuffer();
		for (String line = reader.readLine(); line != null; line = reader.readLine()) {
			sb.append(line);
			sb.append("\n");
		}
		reader.close();
		return sb.toString();

	}
	
	private Service[] getServicesByType(String serviceClass) {
		java.util.Vector<Service>  serviceList = new java.util.Vector<Service>();
		for (Service s : this.mserviceMap.values()) {
			if (s.getClass().getName().equals(serviceClass)) {
				serviceList.add(s);
			}
		}
		return serviceList.toArray(new Service[serviceList.size()]);
	}

	public void shutdown() throws Exception {
		if (null != monitor)
			monitor.stop();

		this.scheduler.shutdown();
		this.scheduler.awaitTermination(-1, TimeUnit.MILLISECONDS);
		
		Service[] targetServiceList = getServicesByType("deltad.engine.TransactionService");
		for (Service s : targetServiceList) {
			s.stop();
		}
		
		Service[] srcServiceList = getServicesByType("deltad.engine.TargetService");
		for (Service s : srcServiceList) {
			s.stop();
		}
		
		mserviceMap.clear();

		this.planScheduler.shutdown();
		this.planMap.clear();
	}

	@Override
	public Set<String[]> getMatchedTablesByName(String amoduleName) throws Exception {
		String moduleName = amoduleName.endsWith(SERVICE_MODULE_FILE_EXT) ? amoduleName : amoduleName + SERVICE_MODULE_FILE_EXT;
		TXServiceModule module = (TXServiceModule)this.modules.get(moduleName);
		return getMatchedTables(module);
	}

	@Override
	public Set<String[]> getMatchedTables(String xml) throws Exception {
		return getMatchedTables((TXServiceModule)Module.fromXML(xml));
	}

	private Set<String[]> getMatchedTables(TXServiceModule module) throws Exception {
		Set<String[]> tableSet = new HashSet<String[]>();
		TableMatchEntry[] xwcDess = module.getEngineEntries(TableMatchRule.EQUAL);
		for (TableMatchEntry des : xwcDess)
			tableSet.add(new String[] { des.getSchema(), des.getTable() });

		TableMatchEntry[] wcDess = module.getEngineEntries(TableMatchRule.LIKE);
		for (TableMatchEntry des : wcDess) {
			String srcUser = des.getSchema();
			String srcTable = des.getTable();
			String[] tables = DataSourceManager.getInstance().getTables(module.getStringProperty(Module.DATASOURCE_NAME), srcUser, null);
			for (String table : tables) {
				if (RegexUtil.sqlLikeMatch(table, srcTable))
					tableSet.add(new String[] { srcUser, table });
			}
		}
		// for (String[] s : tableSet) {
		// System.out.println(s[0] + ":" + s[1]);
		// }
		return tableSet;
	}

	// for MS SQLSERVER
	// private static final String ENABLE_CDC =
	// "EXEC sys.sp_cdc_enable_table @source_schema = 'dbo', @source_name = '%s',@role_name = 'change_table_user1', @index_name = NULL, @capture_instance = 'ST_Instance', @supports_net_changes = 1, @captured_column_list = 'column_key, column_1, column_2', @filegroup_name = 'PRIMARY' ";
	private static final String ENABLE_CDC = "EXEC sys.sp_cdc_enable_table @source_schema = '%s', @source_name = '%s',@role_name = '%s', @index_name = %s, @capture_instance = '%s', @supports_net_changes = %s, @captured_column_list = '%s', @filegroup_name = '%s' ";
	private static final String DISABLE_CDC = "sys.sp_cdc_disable_table @source_schema = '%s', @source_name = '%s', @capture_instance = '%s'";

	private static String formatENABLE_CDC(String source_schema, String source_name, String role_name, String index_name, String capture_instance, String supports_net_changes, String captured_column_list, String filegroup_name) {
		return String.format(ENABLE_CDC, source_schema, source_name, role_name, index_name, capture_instance, supports_net_changes, captured_column_list, filegroup_name);
	}

	public static String captureName(String module, String table) {
		return String.format("rdicapture_%s_%s", module, table);
	}

	private void disableCDC(TXServiceModule module) throws Exception {
		String datasource = module.getStringProperty(Module.DATASOURCE_NAME);
		org.hibernate.StatelessSession session = DataSourceManager.getInstance().openStatelessSession(datasource);
		PreparedStatement ps = null;
		try {
			for (TableMatchEntry des : module.getEngineEntries()) {
				String user = des.getSchema();
				String table = des.getTable();
				String cap_instance = captureName(module.getName(), table);
				String ddl = String.format(DISABLE_CDC, user, table, cap_instance);
				ps = session.connection().prepareStatement(ddl);
				ps.executeUpdate();
				session.connection().commit();
			}
		} catch (Exception e) {
			throw e;
		} finally {
			if (null != ps)
				ps.close();

			session.close();
		}

	}

	private void setupCDC(TXServiceModule module) throws Exception {
		String datasource = module.getStringProperty(Module.DATASOURCE_NAME);
		org.hibernate.StatelessSession session = DataSourceManager.getInstance().openStatelessSession(datasource);
		PreparedStatement ps = null;
		try {
			ps = session.connection().prepareStatement("EXECUTE sys.sp_cdc_enable_db");
			ps.executeUpdate();
			ps.close();
			// query.executeUpdate();
			// to named MirroringDescriptions
			for (TableMatchEntry des : module.getEngineEntries()) {
				String user = des.getSchema();
				String table = des.getTable();
				String[] columns = DataSourceManager.getInstance().getColumnNames(datasource, table, null);
				StringBuffer columnList = new StringBuffer();
				for (String col : columns) {
					columnList.append(col).append(",");
				}
				columnList.setCharAt(columnList.length() - 1, ' ');
				String enableCDC = formatENABLE_CDC(user, table, module.getName() + "_role", "NULL", captureName(module.getName(), table), "1", columnList.toString(), "PRIMARY");
				ps = session.connection().prepareStatement(enableCDC);
				ps.executeUpdate();
				session.connection().commit();
			}
		} catch (Exception e) {
			throw e;
		} finally {
			if (null != ps)
				ps.close();

			session.close();
		}
	}

	public static void seed_md_filters(String agentID, String moduleName, TXServiceModule mirroringModule) throws Exception {// or mapping
		com.pentaho.commons.dsc.a.b cxt = new com.pentaho.commons.dsc.a.b();
		String sql = "insert into " + cxt.VL + "(AGENTID, MODULENAME, SEG_OWNER, SEG_NAME) values(?,?,?,?)";
		String datasource = mirroringModule.getStringProperty(Module.DATASOURCE_NAME);
		org.hibernate.StatelessSession session = DataSourceManager.getInstance().openStatelessSession(datasource);
		org.hibernate.Transaction transaction = session.beginTransaction();
		try {
			for (TableMatchEntry md : mirroringModule.getEngineEntries()) {
				SQLQuery query = session.createSQLQuery(sql);
				query.setString(0, agentID);
				query.setString(1, moduleName);
				query.setString(2, md.getSchema());
				query.setString(3, md.getTable());
				query.executeUpdate();
			}
			transaction.commit();
		} catch (Exception e) {
			logger.error("fail to seed system log table for " + moduleName, e);
			throw e;
		} finally {
			if (null != session)
				session.close();
		}
	}

	public static void remove_md_filters(String agentID, String moduleName, String srcDataSourceName) throws Exception {
		com.pentaho.commons.dsc.a.b cxt = new com.pentaho.commons.dsc.a.b();
		String rawsql = "delete from " + cxt.VL + " where AGENTID = '%s' and MODULENAME = '%s'";
		org.hibernate.StatelessSession session = DataSourceManager.getInstance().openStatelessSession(srcDataSourceName);
		org.hibernate.Transaction transaction = session.beginTransaction();
		try {
			String sql = String.format(rawsql, agentID, moduleName);
			SQLQuery query = session.createSQLQuery(sql);
			query.executeUpdate();
			transaction.commit();
		} catch (Exception e) {
			logger.error("fail to remove system log table for " + moduleName, e);
			throw e;
		} finally {
			if (null != session)
				session.close();
		}
	}
	
	public void purgeQueue(String name, String queueName) throws Exception {
		// think it over, this should be done by user manually
		String moduleName = name.endsWith(SERVICE_MODULE_FILE_EXT) ? name : name + SERVICE_MODULE_FILE_EXT;
		Module module = modules.get(moduleName);
		if (null == module) {
			logger.warn(String.format("module %s is undeployed already", moduleName));
			return;
			//throw new RuntimeException(String.format("module %s is undeployed already", moduleName));
		}
		
		String url = module.getStringProperty(Module.ASSOCIATED_MQ_URL);
		String user = module.getStringProperty(Module.ASSOCIATED_MQ_USERNAME);
		String password = module.getStringProperty(Module.ASSOCIATED_MQ_PASSWORD);
		ConnectionFactory jmsConnectionFactory = new ActiveMQConnectionFactory(user, password, url);
		
		javax.jms.Connection connection = jmsConnectionFactory.createConnection();
		javax.jms.Session session = connection.createSession(false, javax.jms.Session.AUTO_ACKNOWLEDGE);
		javax.jms.MessageConsumer consumer = session.createConsumer(session.createQueue(queueName));
		connection.start();
		while (true) {
			javax.jms.Message msg = consumer.receive(1000);
			if (null == msg) {
				consumer.close();
				session.close();
				connection.close();
				return;
			}
		}
	}
	
	@Override
	public void loginRepository(String user, String pass) throws KettleException {
		KettleFileRepository main = (KettleFileRepository)Bootstrap.openRepository("main", "admin", null);
		System.out.println(main.getRepositoryMeta().getBaseDirectory());
		
		return;
	}	

	public static void main(String[] args) {

	}

	

}
