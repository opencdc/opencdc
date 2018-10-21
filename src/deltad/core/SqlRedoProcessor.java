package deltad.core;

import java.io.StringReader;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.operators.relational.ExpressionList;
import net.sf.jsqlparser.expression.operators.relational.ItemsList;
import net.sf.jsqlparser.expression.operators.relational.ItemsListVisitor;
import net.sf.jsqlparser.parser.CCJSqlParserManager;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.delete.Delete;
import net.sf.jsqlparser.statement.insert.Insert;
import net.sf.jsqlparser.statement.select.SubSelect;
import net.sf.jsqlparser.statement.update.Update;
public class SqlRedoProcessor {

//	public static String replaceTarget(MirroringDescription md, String sql) {
//		if (null == md)
//			return null;
//		if (null == sql)
//			return null;
//		
//		if (md.getSrcSchema().equals(md.getTargetSchema()))
//			return sql;
//		else
//			return sql.replace(md.getSrcSchema(), md.getTargetSchema());
//	}
	
	public static String trimDoubleQuotation(String s) {
		if (null == s)
			return "";
		
		if (s.startsWith("\"") && s.endsWith("\""))
			return s.substring(1, s.length() - 1);
		else
			return s;
	}
	
	@SuppressWarnings("rawtypes")
	private static String removeInsertDoubleQuotation(Insert insert, boolean retainSchema) throws JSQLParserException {
		String tableName = insert.getTable().getName();
		String schema = retainSchema ? trimDoubleQuotation(insert.getTable().getSchemaName()) : null;		
		insert.setTable(new Table(schema, trimDoubleQuotation(tableName)));

		List cols = insert.getColumns();
		if (null != cols) {
			List<String> list = new ArrayList<String>();
			for (Object col : cols) {
				if (col instanceof Column) {
					Column column = (Column) col;
					list.add(trimDoubleQuotation(column.getColumnName()));
				}
			}
			insert.setColumns(list);
		}
		return insert.toString();

	}
	
	private static String removeDeleteDoubleQuotation(Delete delete, boolean retainSchema) throws JSQLParserException {
		String tableName = delete.getTable().getName();
		String schema = retainSchema ? trimDoubleQuotation(delete.getTable().getSchemaName()) : null;
		delete.setTable(new Table(schema, trimDoubleQuotation(tableName)));
		
		Expression exp = delete.getWhere();
		if (null != exp)
			exp.accept(new GBaseExpressionVisitor());
		return delete.toString();
	}
	
	
	@SuppressWarnings("rawtypes")
	private static String removeUpdateDoubleQuotation(Update update, boolean retainSchema) throws JSQLParserException {
		String tableName = update.getTable().getName();
		String schema = retainSchema ? trimDoubleQuotation(update.getTable().getSchemaName()) : null;
		update.setTable(new Table(schema, trimDoubleQuotation(tableName)));
		
		List cols = update.getColumns();
		List<String> list = new ArrayList<String>();
		for (Object col : cols) {
			if (col instanceof Column) {
				Column column = (Column) col;
				list.add(trimDoubleQuotation(column.getColumnName()));
			}
		}
		update.setColumns(list);
		
		Expression exp = update.getWhere();
		if (null != exp)
			exp.accept(new GBaseExpressionVisitor());
		
		return update.toString();
	}
	
	//remove username as well
	public static String removeDoubleQuotation(String sql, boolean retainSchema) throws Exception {
		if (null == sql || sql.length() == 0) {
			return null;
		}
		CCJSqlParserManager parserManager = new CCJSqlParserManager();
		try {
			Statement statement = parserManager.parse(new StringReader(sql));
			if (statement instanceof Insert) {
				return removeInsertDoubleQuotation((Insert)statement, retainSchema);
			} else if (statement instanceof Update) {
				return removeUpdateDoubleQuotation((Update)statement, retainSchema);
			} else if (statement instanceof Delete) {
				return removeDeleteDoubleQuotation((Delete)statement, retainSchema);
			} else {
				return sql;
			}
		} catch (JSQLParserException e) {
			throw new RuntimeException("parse sql failed: " + sql, e);
		}
		
	}
	
	public static final String HEXTORAW_PREFIX = "HEXTORAW('";
	public static final String vTemp = "vTemp";
	public static final String END = " END;";
	
	public static String createSetClause(Map<String, String> updateMap) {
		StringBuffer setBuffer = new StringBuffer();
		for (String name : updateMap.keySet()) {
			String value = updateMap.get(name).trim();
			setBuffer.append(name).append(" = ").append(value).append(",");
		}
		setBuffer.setCharAt(setBuffer.length() - 1, ' ');
		return setBuffer.toString();
	}
	
	public static String createDeclareClauseAndBindingVariable(Map<String, String> setMap) {
		int i = 0;
		StringBuffer declareBuffer = new StringBuffer("declare ");
		StringBuffer beginBuffer = new StringBuffer("begin ");
		for (String name : setMap.keySet()) {
			String value = setMap.get(name).trim();
			if (value.startsWith(HEXTORAW_PREFIX)) {
				String var = vTemp + i++;
				declareBuffer.append(var).append(" ").append("blob; ");
				beginBuffer.append(var).append(" := ").append(value).append(";");
				setMap.put(name, var);
			}
		}
		
		return i > 0 ? declareBuffer.toString() + beginBuffer.toString() : "";
	}
	
	public static String dmlToBlock(String dml) throws Exception {
		CCJSqlParserManager parserManager = new CCJSqlParserManager();
		Statement statement = parserManager.parse(new StringReader(dml));
		
		if (statement instanceof Update) {
			return updateToBlock((Update)statement);
		} else if (statement instanceof Insert) {
			return insertToBlock((Insert)statement);
		} else {
			throw new IllegalArgumentException("dml type is not update/insert: " + dml);
		}
	}
	
	public static String insertToBlock(Insert insert) throws Exception {
		final List cols = insert.getColumns();
		ItemsList itemslist = insert.getItemsList();
		final LinkedHashMap<String, String> insertMap = new LinkedHashMap<String, String>();
		itemslist.accept(new ItemsListVisitor() {

			@Override
			public void visit(SubSelect arg0) {
				// TODO Auto-generated method stub				
			}

			@Override
			public void visit(ExpressionList exlist) {
				List expressions = exlist.getExpressions();
				for (int i=0; i < cols.size(); i++) {
					Object col = cols.get(i);
					if (col instanceof Column) {
						insertMap.put(col.toString(), expressions.get(i).toString());
					}
				}				
			}});
		
		StringBuffer sb = new StringBuffer();
		sb.append(createDeclareClauseAndBindingVariable(insertMap));
		sb.append(createInsertValues(insert.getTable().getName(), insertMap));
		sb.append(END);
		return sb.toString();
	}
	
	private static String createInsertValues(String table, LinkedHashMap<String, String> insertMap) {
		StringBuffer sb = new StringBuffer(" insert into " + table);
		StringBuffer sbCols = new StringBuffer("(");
		StringBuffer sbValues = new StringBuffer("(");
		for (String key : insertMap.keySet()) {
			String value = insertMap.get(key);
			sbCols.append(key).append(",");
			sbValues.append(value).append(",");
		}
		sbCols.deleteCharAt(sbCols.length() - 1);
		sbValues.deleteCharAt(sbValues.length() - 1);
		sbCols.append(")");
		sbValues.append(")");
		return sb.append(sbCols).append(" values").append(sbValues).append(";").toString();
	}
	
	@SuppressWarnings("rawtypes")
	public static String updateToBlock(Update update) throws Exception {		
		List cols = update.getColumns();
		List exps = update.getExpressions();
		LinkedHashMap<String, String> setMap = new LinkedHashMap<String, String>();
		for (int i=0; i < cols.size(); i++) {
			Object col = cols.get(i);
			if (col instanceof Column) {
				setMap.put(col.toString(), exps.get(i).toString());
			}
		}
		StringBuffer sb = new StringBuffer();
		sb.append(createDeclareClauseAndBindingVariable(setMap));
		sb.append(String.format(" update %s set %s", update.getTable(), createSetClause(setMap)));
		sb.append(String.format("where %s;", update.getWhere())).append(END);
		return sb.toString();
	}
	
	//for test only
	@SuppressWarnings("unused")
	public static void main(String[] args) throws Exception {
//		java.io.BufferedReader reader = new java.io.BufferedReader(new java.io.FileReader("d:/zzz.sql"));
//		String line = reader.readLine();
//		StringBuffer sb = new StringBuffer();
//		while(null != line) {
//			sb.append(line);
//			line = reader.readLine();
//		}
//		CCJSqlParserManager parserManager = new CCJSqlParserManager();
//		Statement statement = parserManager.parse(new StringReader(sb.toString()));
		
		String delete_statement = " delete from \"BYM57_LSN1118\".\"PUB_WF_DEF\" " 
            + " where \"BILLMAKER\" = '0001661000000000I05F' "
            + " and \"BILLMAKER_NAME\" = 'nn' "
            + " and \"BILLMAKER_TYPE\" = 'OPERATOR' "
            + " and \"SEALFLAG\" = 'OPERATOR' ";
		
		String update_statement =  " update \"BYM57_LSN1118\".\"PUB_WF_DEF\" " 
            + " set \"CONTENT\" = HEXTORAW('aced0005740bfa3c3f3e0d0a'), aaa=6 "
            + " where \"BILLMAKER\" = '0001661000000000I05F' "
            + " and \"BILLMAKER_NAME\" = 'nn' "
            + " and \"BILLMAKER_TYPE\" = 'OPERATOR' "
            + " and \"SEALFLAG\" IS NULL ";
		
		String insert_blob = "Insert into table1(a,blobcol) values(1, HEXTORAW('aced0005740bfa3c3f3e0d0a'))";
		
		String update_statement2 = "update \"DIBSA\".\"MYTEST\" set "
			+ " \"SEG_OWNER\" = '郝铸', \"SEG_NAME\" = 'ddddd', \"AGE\" = '4' " + 
			" where \"SEG_OWNER\" = 'aaa' and \"SEG_NAME\" = '中国' and \"AGE\" = '2' ";
		
		String insert_statement = "insert into \"dibsa\".\"MYTEST\"(\"SEG_OWNER\",\"SEG_NAME\",\"AGE\") values('haozhu','aaa','10')";
		//System.out.println(removeDoubleQuotation(delete_statement));
		//System.out.println(removeDoubleQuotation(insert_statement));
		
		String ss = "insert into \"dibsa\".\"bd_areacl_en\"( AREACLCODE ,PK_AREACL,PK_CORP) values('1中国','22','sd')";
		String sql = "insert into \"DIBSA\".\"BLOB_TABLE\"(\"COLUMN1\",\"BLOB_CL\") values ('t2',EMPTY_BLOB());";
		String sql2 = "INSERT INTO dim_corp (pk_corp,code1,name1,code2,name2,code3,name3) select t3.pk_corp pk_corp,t1.innercode code1,t1.unitname name1 ,t2.innercode code2,t2.unitname name2,t3.l3 code3,t3.unitname name3 from bd_corp t1,bd_corp t2,(select pk_corp,       substr(innercode, 1, 2) l1,       substr(innercode, 1, 4) l2,       innercode l3,       unitname  from bd_corp) t3 where t1.innercode = t3.l1 and t2.innercode = t3.l2";
		//System.out.println(removeDoubleQuotation(insert_statement, true));
		System.out.println(dmlToBlock(insert_blob));
		
	}

}
