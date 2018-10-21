package deltad.core;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.util.Arrays;
import java.util.Scanner;
import java.util.Vector;
import java.util.concurrent.atomic.AtomicBoolean;

public class DeltadSQLPlus {	
	private static final String SQLPLUS = "sqlplus";
	private static final String DEFAULT_OPTION = "sys/sys as sysdba";
	private static final String DIB_KCCCP = "dib_kcccp";
	//private static final String samplelogincmd = "sqlplus user/password@10.1.74.111/orcl.ufsoft.com.cn";
	private static final String CREATE_VIEW = "create or replace view " + DIB_KCCCP +
		" as select \"ADDR\",\"INDX\",\"INST_ID\",\"CPTNO\", " +
		" \"CPSTA\",\"CPFLG\",\"CPDRT\",\"CPRDB\", " + 
		" \"CPLRBA_SEQ\",\"CPLRBA_BNO\",\"CPLRBA_BOF\", " + 
		" \"CPODR_SEQ\",\"CPODR_BNO\",\"CPODR_BOF\", " + 
		" \"CPODS\",\"CPODT\",\"CPODT_I\",\"CPHBT\", " + 
		" \"CPRLS\",\"CPRLC\",\"CPMID\",\"CPSDR_SEQ\", " + 
		" \"CPSDR_BNO\",\"CPSDR_ADB\" from x$kcccp;";
	
	private static final String CREATE_SYNONYM  = String.format("create or replace public synonym %s  for sys.%s;", DIB_KCCCP, DIB_KCCCP);
	private static final String GRANT_PERMISSION_DBA = "create user %s identified by %s default tablespace users;\n" + 
		"grant connect,resource,dba to %s;\n" +
		"grant select on " + DIB_KCCCP + " to %s;\n" +
		"grant select on v_$logmnr_logs to %s;\n" +
		"grant select on v_$archived_log to %s;\n" +
		"grant select on v_$log to %s;\n" +
		"grant select on v_$logfile to %s;\n" +
		"grant select on v_$database to %s;\n" +
		"grant execute on dbms_logmnr to %s;\n" +
		"grant alter system to %s;\n";
	
	private static final String GRANT_PERMISSION = "create user %s identified by %s default tablespace users;\n" + 
			"grant connect,resource to %s;\n" +
			"grant select on " + DIB_KCCCP + " to %s;\n" +
			"grant select on v_$logmnr_logs to %s;\n" +
			"grant select on v_$archived_log to %s;\n" +
			"grant select on v_$log to %s;\n" +
			"grant select on v_$logfile to %s;\n" +
			"grant select on v_$database to %s;\n" +
			"grant execute on dbms_logmnr to %s;\n" +
			
			"grant select any transaction to %s;\n" +
			"grant select on v_$logmnr_contents to %s;\n" +
			"grant select on gv_$instance to %s;\n" ;
	
	private static final String CREATE_SYSTEM_TABLE = "CREATE TABLE %s.%s (	" +
		"\"AGENTID\" VARCHAR2(32) NOT NULL ENABLE, " +
		"\"MODULENAME\" VARCHAR2(32) NOT NULL ENABLE, " +
		"\"SEG_OWNER\" VARCHAR2(32) NOT NULL ENABLE, " +
		"\"SEG_NAME\" VARCHAR2(256) NOT NULL ENABLE, " +
		" CONSTRAINT \"V_DIB_SYS_LOG_PK\" PRIMARY KEY (\"AGENTID\", \"MODULENAME\", \"SEG_OWNER\", \"SEG_NAME\"));";
	
	private static final String CONNECT_USER = "connect %s/%s@%s;"; 
	private static final String DIB_SYSLM = "dib_sys_ps_lm";
	
	private static final String CREATE_PROCEDURE = "create or replace procedure " + DIB_SYSLM + "(lowSCN integer, highSCN integer) is " + 
			"begin " + 
			"sys.dbms_logmnr.start_logmnr(startScn => lowSCN,endScn => highSCN,Options => 3088);" +
			"end " + DIB_SYSLM + ";"
			+ "\n /";
	
	private static final String EXIT = "exit;";
	
	private final String start_cmd;
	private final String start_option;
	private Process process;
	
	private OutputStream out;
	private InputStream in;
	private BufferedWriter writer;
	private BufferedReader reader;
	
	public DeltadSQLPlus(String cmd, String option) {
		start_cmd = cmd;
		start_option = option;
	}
	
	public void start() throws IOException {
		ProcessBuilder pb = new ProcessBuilder(start_cmd, start_option);
		java.util.Map<String, String> env = pb.environment();
		env.put("oracle_sid", "ORCL");

		pb.redirectErrorStream(true);
		process = pb.start();
		
		out = process.getOutputStream();
		in = process.getInputStream();
		writer = new BufferedWriter(new OutputStreamWriter(out));
		reader = new BufferedReader(new InputStreamReader(in));
		
		Thread thread = new Thread(t);
		thread.start();
	}
	
	public OutputStream getOutputStream() {
		return process.getOutputStream();
	}
	
	public InputStream getInputStream() {
		return process.getInputStream();
	}
	
	public BufferedWriter getWriter() {
		return writer;
	}
	
	public BufferedReader getReader() {
		return reader;
	}
	
	public void stop() {
		running.set(false);
//		try {
//			out.close();
//			in.close();
//		} catch (IOException e) {
//		}
		process.destroy();
	}
	
	public void sendCommand(String cmd, int wait_second) throws Exception {
		System.out.println(cmd);
		getWriter().write(cmd + "\n");
		getWriter().flush();
		Thread.sleep(wait_second * 1000);
	}
	
	public void sendCommand(String cmd) throws Exception {
		sendCommand(cmd, 1);
	}
	
	private final Vector<String> result = new Vector<String>();
	AtomicBoolean running = new AtomicBoolean(true);
	Runnable t = new Runnable() {

		@Override
		public void run() {
			String line = null;
			try {
				while (running.get() && (line = getReader().readLine()) != null) {
					result.add(line);
					//System.out.println(line);
				}
			} catch (IOException e) {
				e.printStackTrace();
			}			
		}
		
	};
	
	public String[] getResult() throws IOException {
		synchronized(result) {
			String[] r = result.toArray(new String[result.size()]);
			result.clear();
			return r;
		}		 
	}
	
	public static void printResult(String[] result) {
		for(String line : result)
			System.out.println(line);
	}
	
	private static char[] readPassword(String msg) {
		java.io.Console console = System.console();
		return console.readPassword("%s", msg);
	}
	
	private static String validPassword() {
		char[] dibSysPassword;
		char[] dibSysPassword2;
		do {
			dibSysPassword = readPassword("please input password: ");
			dibSysPassword2 = readPassword("comfirm password: ");
		} while (null == dibSysPassword || dibSysPassword.length < 6 || !Arrays.equals(dibSysPassword, dibSysPassword2));
		return String.valueOf(dibSysPassword);
	}
	
	private static String DEFAULT_DIBADMIN= "DIBADMIN";
	public static void main(String[] args) throws Exception {		
		System.out.println("please input sqlplus login option to initial the datatase\n");
		Scanner scanner = new Scanner(System.in);
		String dbaline = scanner.nextLine();

		DeltadSQLPlus sqlplus = new DeltadSQLPlus(SQLPLUS, dbaline);
		sqlplus.start();
		String[] result;
		
		sqlplus.sendCommand(CREATE_VIEW);
		result = sqlplus.getResult();
		printResult(result);
		
		sqlplus.sendCommand(CREATE_SYNONYM);
		result = sqlplus.getResult();
		printResult(result);
		
		System.out.println(String.format("Create DIB System User, please input user name, default is %s\n", DEFAULT_DIBADMIN));
		String inputDibAdmin = scanner.nextLine();
		String dibSystemAdmin = (null == inputDibAdmin || inputDibAdmin.length() == 0) ? DEFAULT_DIBADMIN : inputDibAdmin;				
		String dibSysPassword = validPassword();				
		System.out.println(dibSystemAdmin + "/" + dibSysPassword);
		
		sqlplus.sendCommand(String.format(GRANT_PERMISSION, dibSystemAdmin, dibSysPassword, dibSystemAdmin, dibSystemAdmin, dibSystemAdmin, dibSystemAdmin, dibSystemAdmin, dibSystemAdmin, dibSystemAdmin, dibSystemAdmin, dibSystemAdmin,dibSystemAdmin, dibSystemAdmin));
		result = sqlplus.getResult();
		printResult(result);
		
		System.out.println("please input Database SID:");
		String databaseName;
		do {
			databaseName = scanner.nextLine();
		} while(null == databaseName || databaseName.length() == 0);
		sqlplus.sendCommand(String.format(CONNECT_USER, dibSystemAdmin, dibSysPassword, databaseName));
		result = sqlplus.getResult();
		printResult(result);
		
		sqlplus.sendCommand(String.format(CREATE_SYSTEM_TABLE, dibSystemAdmin, "V_DIB_SYS_LOG"));
		result = sqlplus.getResult();
		printResult(result);
		
		sqlplus.sendCommand(CREATE_PROCEDURE, 1);
		result = sqlplus.getResult();
		printResult(result);	
		
		sqlplus.sendCommand("connect " + dbaline);
		result = sqlplus.getResult();
		printResult(result);
		
		sqlplus.sendCommand("select log_mode from v$database;", 1);
		result = sqlplus.getResult();
		printResult(result);		
		
		sqlplus.sendCommand("archive log list");
		result = sqlplus.getResult();
		printResult(result);
		
		sqlplus.sendCommand("shutdown immediate", 10);
		result = sqlplus.getResult();
		printResult(result);
		
		sqlplus.sendCommand("startup mount", 10);
		result = sqlplus.getResult();
		printResult(result);
		
		//------------------------------------------------------
		sqlplus.sendCommand("alter database archivelog;", 10);
		result = sqlplus.getResult();
		printResult(result);
		
		sqlplus.sendCommand("alter database force logging;", 10);
		result = sqlplus.getResult();
		printResult(result);
		
		sqlplus.sendCommand("select log_mode from v$database;", 10);
		result = sqlplus.getResult();
		printResult(result);
		
		//--------------------------------------------------
		
		sqlplus.sendCommand("alter database open;", 10);
		result = sqlplus.getResult();
		printResult(result);
		
		sqlplus.sendCommand("alter database add supplemental log data;");
		result = sqlplus.getResult();
		printResult(result);
		
		sqlplus.sendCommand("alter database add supplemental log data (primary key,unique index) columns;");
		result = sqlplus.getResult();
		printResult(result);
		
		sqlplus.sendCommand(EXIT, 5);
		result = sqlplus.getResult();
		printResult(result);
		
		sqlplus.stop();
	}
}
