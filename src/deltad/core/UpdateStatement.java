package deltad.core;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.util.Properties;

/**
 * UpdateStatement class
 * @author haozhu
 */
public class UpdateStatement {
	public static final int BASE = 0;
	public static final int INDENTIFIER = 1;//keyword
	public static final int COLUMN_NAME = 2;
	public static final int VALUE = 3;
	public static final int STRING = 4;
	public static final int ESCAPE = 5;
	
	public static final String vTemp = "vTemp";
	public static final String HEXTORAW_PREFIX = "HEXTORAW('";
	public static final String END = " END;;";
	
	private final String updateSQL;
	private final String tableOwner;
	private final String tableName;
	
	private String updateClause = null;
	private String whereClause = null;
	
	private Properties updateMap = new Properties();
	
	public UpdateStatement(String updateSQL, String seg_owner, String seg_name) {
		this.updateSQL = updateSQL;
		this.tableOwner = seg_owner;
		this.tableName = seg_name;
		init();
	}
	
	private void init() {
		final StringBuffer updateBuffer = new StringBuffer();
		updateBuffer.append("update ").append("\"").append(tableOwner).append("\"");
		updateBuffer.append(".").append("\"").append(tableName).append("\"").append(" set ");
		String updateSet = updateBuffer.toString();
		int i = updateSQL.indexOf(updateSet);
		if (i >= 0)
			updateClause = updateSQL.substring(0, i + updateSet.length());
		String set_where = updateSQL.substring(updateClause.length());
		int j = parserSetStatement(set_where, updateMap);
		whereClause = "where " + set_where.substring(j + 1);
	}
	
	public String getUpdateClause() {
		return this.updateClause;
	}
	
	public Properties getUpdateMap() {
		return this.updateMap;
	}
	
	public String getWhereClause() {
		return this.whereClause;
	}
	
	//parser set statement with state machine
	private static int parserSetStatement(String sql, Properties map) {
		StringBuffer buffer = new StringBuffer();
		String key = null, value;		
		char curr, next;
		int state = BASE;
		int i = 0;
		for (; i < sql.length(); i++) {
			curr = sql.charAt(i);
			next = sql.charAt(i + 1);
			switch (state) {
			case BASE:
				if (curr == '\"') {
					state = COLUMN_NAME;
				} else if (curr == '=') {
					while (sql.charAt(++i) == ' ');
					if (sql.charAt(i) == '\'')
					    state = STRING;
					else 
						state = VALUE;
					buffer.append(sql.charAt(i));
					
				} else if (curr == ' ' || curr == '\n' || curr == ',') {
					break;
				} else {
					state = INDENTIFIER;
					buffer.append(curr);
				}
				break;
			case INDENTIFIER:// keyword
				if (curr == ' ') {
					if ("where".equalsIgnoreCase(buffer.toString())) {
						printProperties(map);
						return i;
					}
					state = BASE;
				} else
					buffer.append(curr);

				break;
			case COLUMN_NAME:
				if (curr == '\"') {
					key = buffer.toString();
					buffer.delete(0, buffer.length());
					state = BASE;
				} else {
					buffer.append(curr);
				}
				break;
			case VALUE:
				if (curr == ',' || curr == ' ') {
					value = buffer.toString();
					map.setProperty(key, value);
					buffer.delete(0, buffer.length());
					state = BASE;
				} else {
					buffer.append(curr);
				}
				break;
			case STRING:
				if (curr == '\'') {
					if (next == '\'') { //escape
						buffer.append(curr);						
						state = ESCAPE;
					} else {// end of a string
						buffer.append(curr);
						value = buffer.toString();
						map.setProperty(key, value);
						buffer.delete(0, buffer.length());
						state = BASE;
					}
				} else {
					buffer.append(curr);
				}
				break;
			case ESCAPE :
				if (curr == '\'') {
					state = STRING;
				}
				buffer.append(curr);
				break;

			}
		}
		return i;
	}
	
	//for debug
	private static void printProperties(Properties map) {
		for (String key : map.stringPropertyNames())
			System.out.println(key + "=" + map.getProperty(key));
	}
	
	public static String escapeCLOB(String clob) {
		StringBuffer destBuffer = new StringBuffer().append('\'');
		for (int i = 1; i < clob.length() - 1; i++) {
			char c = clob.charAt(i);
			if (c == '\'') {
				destBuffer.append('\'');
			}
			destBuffer.append(c);
		}
		return destBuffer.append('\'').toString();
	}
	
	// create DeclareClause And Binding Variable to map, map is input as well as output
	// replace 2k~4k data string with a variable vTemp
	public String createDeclareClauseAndBindingVariable() {
		int i = 0;
		StringBuffer declareBuffer = new StringBuffer("declare ");
		StringBuffer beginBuffer = new StringBuffer("begin ");
		for (String name : updateMap.stringPropertyNames()) {
			String value = updateMap.getProperty(name).trim();
			if (value.startsWith(HEXTORAW_PREFIX)) {
				String var = vTemp + i++;
				declareBuffer.append(var).append(" ").append("blob; ");
				beginBuffer.append(var).append(" := ").append(value).append(";");
				updateMap.setProperty(name, var);
			}
		}
		
		return i > 0 ? declareBuffer.toString() + beginBuffer.toString() : "";
	}

	public String createSetClause() {
		StringBuffer setBuffer = new StringBuffer();
		for (String name : updateMap.stringPropertyNames()) {
			String value = updateMap.getProperty(name).trim();
			setBuffer.append(name).append(" = ").append(value).append(",");
		}
		setBuffer.setCharAt(setBuffer.length() - 1, ' ');
		return setBuffer.toString();
	}
	
	public String toString() {
		return createDeclareClauseAndBindingVariable() + getUpdateClause() +  createSetClause() + getWhereClause() + END;
	}


	public static void main(String[] args) throws Exception {
		BufferedReader reader = new BufferedReader(new FileReader("d:/srcsql.txt"));
		StringBuffer sb = new StringBuffer();
		String line = reader.readLine();
		while (line != null) {
			sb.append(line);
			line = reader.readLine();
		}
		System.out.println(sb.toString());
		UpdateStatement updatestat = new UpdateStatement(sb.toString(), "NCTEST", "TBL_LOB");
		System.out.println(updatestat);
		FileWriter out = new FileWriter(new File("d:/sqlout.txt"));
		out.write(updatestat.toString(), 0, updatestat.toString().length());
		out.close();

	}

}
