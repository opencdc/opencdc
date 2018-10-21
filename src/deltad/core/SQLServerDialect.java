package deltad.core;

import java.sql.Types;

public class SQLServerDialect extends org.hibernate.dialect.SQLServer2008Dialect {

	public SQLServerDialect() {
		registerColumnType(Types.BIGINT, "bigint");
		registerColumnType(Types.BIT, "bit");
		registerColumnType(Types.CHAR, "nchar(1)");
		registerColumnType(Types.NCHAR, 4000, "nchar($l)");
		registerColumnType(Types.VARCHAR, 4000, "nvarchar($l)");
		registerColumnType(Types.VARCHAR, "nvarchar(max)");
		registerColumnType(Types.VARBINARY, 4000, "varbinary($1)");
		registerColumnType(Types.VARBINARY, "varbinary(max)");
		registerColumnType(Types.BLOB, "varbinary(max)");
		registerColumnType(Types.CLOB, "nvarchar(max)");
	}
}
