package deltad.core;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

/**
 * Enum representing a database type, such as DB2 or oracle.  The type also
 * contains a product name, which is expected to be the same as the product name
 * provided by the database driver's metadata.
 * 
 * @author haozhu
 */
public enum DatabaseType implements Serializable {	
	ORACLE("Oracle", "oracle.jdbc.driver.OracleDriver", "org.hibernate.dialect.Oracle10gDialect", "jdbc:oracle:thin:@%s:%d:%s"),
	POSTGRES("PostgreSQL", "org.postgresql.Driver", "org.hibernate.dialect.PostgreSQLDialect", "jdbc:postgresql://%s:%d/%s"),
	SQLSERVER("Microsoft SQL Server", "com.microsoft.jdbc.sqlserver.SQLServerDriver", "org.hibernate.dialect.SQLServer2008Dialect", "jdbc:sqlserver://%s:%d;databaseName=%s"),	
	//DB2("DB2", "com.ibm.db2.jdbc.app.DB2Driver", "org.hibernate.dialect.DB2Dialect"), 
	//SYBASE("Sybase", "com.sybase.jdbc.SybDriver", "org.hibernate.dialect.Sybase11Dialect"),
	//HSQL("HSQL Database Engine"),
	//DERBY("Apache Derby"), 
	//MYSQL("MySQL", "org.gjt.mm.MySQL.Driver", "org.hibernate.dialect.MySQLDialect"),
	GBASE("GBase8a", "com.gbase.jdbc.Driver", "deltad.core.GBaseDialect", "jdbc:gbase://%s:%d/%s?rewriteBatchedStatements=true");
	
	private static final Map<String, DatabaseType> nameMap;
	private static final Map<String, DatabaseType> dialectMap;
	
	static{
		nameMap = new HashMap<String, DatabaseType>();
		dialectMap = new HashMap<String, DatabaseType>();
		for(DatabaseType type: values()){
			nameMap.put(type.getProductName(), type);
			dialectMap.put(type.getDialect(), type);
		}
	}
	//A description is necessary due to the nature of database descriptions in metadata.
	private final String productName;
	private final String driverClass;
	private final String dialect;
	private final String urlFormat;
	
	private DatabaseType(String productName, String driverClass, String dialect, String urlFormat) {
		this.productName = productName;
		this.driverClass = driverClass;
		this.dialect = dialect;
		this.urlFormat = urlFormat;
	}
	
	public String getProductName() {
		return productName;
	}
	
	public String getDriverClass() {
		return this.driverClass;
	}
	
	public String getDialect() {
		return this.dialect;
	}
	
	public String getUrlFormat() {
		return this.urlFormat;
	}
	
	/**
	 * Static method to obtain a DatabaseType from the provided product name.
	 * 
	 * @param productName
	 * @return DatabaseType for given product name.
	 * @throws IllegalArgumentException if none is found.
	 */
	public static DatabaseType fromProductName(String productName){
		if(!nameMap.containsKey(productName)){
			throw new IllegalArgumentException("DatabaseType not found for product name: [" + 
					productName + "]");
		}
		else{
			return nameMap.get(productName);
		}
	}
	
	public static DatabaseType fromDialect(String dialect){
		if(!dialectMap.containsKey(dialect)){
			throw new IllegalArgumentException("DatabaseType not found for product name: [" + 
					dialect + "]");
		}
		else{
			return dialectMap.get(dialect);
		}
	}
	
	public static String[] getProductNames() {
		String[] productNames = new String[values().length];
		for(int i = 0; i < values().length; i++){
			productNames[i] = values()[i].productName;
		}
		return productNames;
		
	}
	
	public static void main(String[] args) {
		//System.out.println(getProductNames());
		System.out.println(fromDialect("deltad.core.GBaseDialect").getProductName());
	}
	
}