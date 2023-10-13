package prerna.reactor.utils;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.ds.OwlTemporalEngineMeta;
import prerna.ds.rdbms.h2.H2Frame;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Constants;
import prerna.util.Utility;
import prerna.util.sql.AbstractSqlQueryUtil;
import prerna.util.sql.RdbmsTypeEnum;
import prerna.util.sql.SqlQueryUtilFactory;

public class ExternalDatabaseProfileReactor extends AbstractReactor {
	
	private static final Logger logger = LogManager.getLogger(ExternalDatabaseProfileReactor.class);

	public ExternalDatabaseProfileReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.DB_DRIVER_KEY.getKey(), ReactorKeysEnum.HOST.getKey(),
				ReactorKeysEnum.PORT.getKey(), ReactorKeysEnum.USERNAME.getKey(), ReactorKeysEnum.PASSWORD.getKey(),
				ReactorKeysEnum.SCHEMA.getKey() };
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();
		// output frame
		String[] headers = new String[] { "table_name", "column_name", "numOfBlanks", "numOfUniqueValues", "min", "average", "max", "sum" , "numOfNullValues"};
		String[] dataTypes = new String[] { "String", "String", "DOUBLE", "DOUBLE", "DOUBLE", "DOUBLE", "DOUBLE", "DOUBLE" , "DOUBLE"};
		H2Frame frame = (H2Frame) this.insight.getDataMaker();
		String tableName = frame.getName();
		
		// add headers to metadata output frame
		OwlTemporalEngineMeta metaData = frame.getMetaData();
		for (int i = 0; i < headers.length; i++) {
			String alias = headers[i];
			String dataType = dataTypes[i];
			String uniqueHeader = tableName + "__" + alias;
			metaData.addProperty(tableName, uniqueHeader);
			metaData.setAliasToProperty(uniqueHeader, alias);
			metaData.setDataTypeToProperty(uniqueHeader, dataType);
		}
		
		Connection con = null;
		String driver = this.keyValue.get(this.keysToGet[0]);
		if(driver == null) {
			throw new IllegalArgumentException("Must pass in the rdbms type");
		}
		RdbmsTypeEnum dbType = RdbmsTypeEnum.getEnumFromString(driver);
		if(dbType == null) {
			// try one more time
			dbType =  RdbmsTypeEnum.getEnumFromDriver(driver);
			if(dbType == null) {
				throw new IllegalArgumentException("Unable to find driver for rdbms type = " + driver);
			}
		}
		AbstractSqlQueryUtil queryUtil = SqlQueryUtilFactory.initialize(dbType);
		
		String host = this.keyValue.get(this.keysToGet[1]);
		String port = this.keyValue.get(this.keysToGet[2]);
		String username = this.keyValue.get(this.keysToGet[3]);
		String password = this.keyValue.get(this.keysToGet[4]);
		String schema = this.keyValue.get(this.keysToGet[5]);
		ResultSet tables = null;
		ResultSet columns = null;
		try {
			Map<String, Object> conDetails = new HashMap<>();
			conDetails.put(AbstractSqlQueryUtil.HOSTNAME, host);
			conDetails.put(AbstractSqlQueryUtil.PORT, port);
			conDetails.put(AbstractSqlQueryUtil.SCHEMA, schema);
			String connectionUrl = queryUtil.setConnectionDetailsfromMap(conDetails);
			con = AbstractSqlQueryUtil.makeConnection(dbType, connectionUrl, username, password);
			DatabaseMetaData meta = con.getMetaData();
			tables = meta.getTables(null, null, null, new String[] { "TABLE" });
			while (tables.next()) {
				String table = tables.getString("table_name");
				columns = meta.getColumns(null, null, table, null);
				while (columns.next()) {
					String colName = columns.getString("column_name");
					String type = columns.getString("type_name");
					if (Utility.isNumericType(type)) {
						// will need to get min, average, max, sum
						String[] cells = new String[9];
						// table name
						cells[0] = table;
						// column name
						cells[1] = colName;
						// # of blanks
						String query = "SELECT COUNT(*) FROM " + table + " WHERE " + colName + " in('');";
						long count = execAndClose(con, query);
						cells[2] = count + "";
						// # of unique values
						query = "SELECT DISTINCT COUNT(" + colName + ") FROM " + table + ";";
						long uniqueNRow = execAndClose(con, query);
						cells[3] = uniqueNRow + "";
						// min
						query = "SELECT MIN(" + colName + ") FROM " + table + ";";
						long min = execAndClose(con, query);
						cells[4] = min + "";
						// average
						query = "SELECT AVG(" + colName + ") FROM " + table + ";";
						long avg = execAndClose(con, query);
						cells[5] = avg + "";
						// max
						query = "SELECT MAX(" + colName + ") FROM " + table + ";";
						long max = execAndClose(con, query);
						cells[6] = max + "";
						// sum
						query = "SELECT SUM(" + colName + ") FROM " + table + ";";
						long sum = execAndClose(con, query);
						cells[7] = sum + "";
						// # of null values
						query = "SELECT COUNT(*) FROM " + table + " WHERE " + colName + " is null;";
						long countNull = execAndClose(con, query);
						cells[8] = countNull + "";
						// add data to frame
						frame.addRow(tableName, headers,  cells, dataTypes);
					} else {
						// assume string
						if (Utility.isStringType(type)) {
							String[] cells = new String[9];
							// table name
							cells[0] = table;
							// column name
							cells[1] = colName;
							String query = "SELECT COUNT(*) FROM " + table + " WHERE " + colName + " in('');";
							long count = execAndClose(con, query);
							cells[2] = count + "";
							// # of unique values
							query = "SELECT DISTINCT COUNT(" + colName + ") FROM " + table + ";";
							long uniqueNRow = execAndClose(con, query);
							cells[3] = uniqueNRow + "";
							// # of null values
							query = "SELECT COUNT(*) FROM " + table + " WHERE " + colName + " is null;";
							long countNull = execAndClose(con, query);
							cells[8] = countNull + "";
							// add data to frame
							frame.addRow(tableName, headers,  cells, dataTypes);
						}
					}
				}
			}

		} catch (SQLException e) {
			logger.error(Constants.STACKTRACE, e);
		} finally {
			try {
				if (con != null) {
					con.close();
				}
			} catch (SQLException e) {
				logger.error(Constants.STACKTRACE, e);
			}
			try {
				if (tables != null) {
					tables.close();
				}
			} catch (SQLException e) {
				logger.error(Constants.STACKTRACE, e);
			}
			try {
				if (columns != null) {
					columns.close();
				}
			} catch (SQLException e) {
				logger.error(Constants.STACKTRACE, e);
			}
		}
		return new NounMetadata(frame, PixelDataType.FRAME, PixelOperationType.FRAME_HEADERS_CHANGE);
	}
	
	/**
	 * Closing the opened statemnts/result sets
	 * @param con
	 * @param query
	 * @return
	 * @throws SQLException
	 */
	private long execAndClose(Connection con, String query) throws SQLException {
		Statement stmt = null;
		ResultSet rs = null;

		try {
			stmt = con.createStatement();
			rs = stmt.executeQuery(query);
			rs.next();
			return rs.getLong(1);
		} finally {
			if(rs != null) {
				try {
					rs.close();
				} catch (SQLException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
			if(stmt != null) {
				try {
					stmt.close();
				} catch (SQLException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		}
	}
	
}
