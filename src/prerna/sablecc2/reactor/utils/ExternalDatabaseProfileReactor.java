package prerna.sablecc2.reactor.utils;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;

import prerna.ds.OwlTemporalEngineMeta;
import prerna.ds.h2.H2Frame;
import prerna.engine.impl.rdbms.RdbmsConnectionHelper;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.reactor.AbstractReactor;
import prerna.util.Utility;

public class ExternalDatabaseProfileReactor extends AbstractReactor {

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
		String dbDriver = this.keyValue.get(this.keysToGet[0]);
		String host = this.keyValue.get(this.keysToGet[1]);
		String port = this.keyValue.get(this.keysToGet[2]);
		String username = this.keyValue.get(this.keysToGet[3]);
		String password = this.keyValue.get(this.keysToGet[4]);
		String schema = this.keyValue.get(this.keysToGet[5]);
		ResultSet tables = null;
		ResultSet columns = null;
		ResultSet rs = null;
		try {
			con = RdbmsConnectionHelper.buildConnection(dbDriver, host, port, username, password, schema, null);
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
						rs = con.createStatement().executeQuery(query);
						rs.next();
						long count = rs.getLong(1);
						cells[2] = count + "";
						// # of unique values
						query = "SELECT DISTINCT COUNT(" + colName + ") FROM " + table + ";";
						rs = con.createStatement().executeQuery(query);
						rs.next();
						long uniqueNRow = rs.getLong(1);
						cells[3] = uniqueNRow + "";
						// min
						query = "SELECT MIN(" + colName + ") FROM " + table + ";";
						rs = con.createStatement().executeQuery(query);
						rs.next();
						long min = rs.getLong(1);
						cells[4] = min + "";
						// average
						query = "SELECT AVG(" + colName + ") FROM " + table + ";";
						rs = con.createStatement().executeQuery(query);
						rs.next();
						long avg = rs.getLong(1);
						cells[5] = avg + "";
						// max
						query = "SELECT MAX(" + colName + ") FROM " + table + ";";
						rs = con.createStatement().executeQuery(query);
						rs.next();
						long max = rs.getLong(1);
						cells[6] = max + "";
						// sum
						query = "SELECT SUM(" + colName + ") FROM " + table + ";";
						rs = con.createStatement().executeQuery(query);
						rs.next();
						long sum = rs.getLong(1);
						cells[7] = sum + "";
						// # of null values
						query = "SELECT COUNT(*) FROM " + table + " WHERE " + colName + " is null;";
						rs = con.createStatement().executeQuery(query);
						rs.next();
						count = rs.getLong(1);
						cells[8] = count + "";
						// add data to frame
						frame.addRow(tableName, cells, headers, dataTypes);
					} else {
						// assume string
						if (Utility.isStringType(type)) {
							String[] cells = new String[9];
							// table name
							cells[0] = table;
							// column name
							cells[1] = colName;
							String query = "SELECT COUNT(*) FROM " + table + " WHERE " + colName + " in('');";
							rs = con.createStatement().executeQuery(query);
							rs.next();
							long count = rs.getLong(1);
							cells[2] = count + "";
							// # of unique values
							query = "SELECT DISTINCT COUNT(" + colName + ") FROM " + table + ";";
							rs = con.createStatement().executeQuery(query);
							rs.next();
							long uniqueNRow = rs.getLong(1);
							cells[3] = uniqueNRow + "";
							// # of null values
							query = "SELECT COUNT(*) FROM " + table + " WHERE " + colName + " is null;";
							rs = con.createStatement().executeQuery(query);
							rs.next();
							count = rs.getLong(1);
							cells[8] = count + "";
							// add data to frame
							frame.addRow(tableName, cells, headers, dataTypes);
						}
					}
				}
			}

		} catch (SQLException e) {
			e.printStackTrace();
		} finally {
			try {
				con.close();
			} catch (SQLException e) {
				e.printStackTrace();
			}
			try {
				tables.close();
			} catch (SQLException e) {
				e.printStackTrace();
			}
			try {
				columns.close();
			} catch (SQLException e) {
				e.printStackTrace();
			}
			try {
				rs.close();
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
		return new NounMetadata(frame, PixelDataType.FRAME, PixelOperationType.FRAME_HEADERS_CHANGE);
	}
}