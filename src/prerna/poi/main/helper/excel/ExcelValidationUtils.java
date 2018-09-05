package prerna.poi.main.helper.excel;

import java.sql.Array;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Vector;

import prerna.ds.util.RdbmsQueryBuilder;
import prerna.engine.api.IEngine;
import prerna.engine.api.IEngine.ACTION_TYPE;
import prerna.engine.impl.rdbms.RDBMSNativeEngine;

public class ExcelValidationUtils {
	private static String[] colNames = new String[] { "header", "type","validationType", "range", "emptyCells", "operation", "values", "f1", "f2" };
	private static String[] types = new String[] { "VARCHAR(800)", "VARCHAR(800)", "VARCHAR(800)", "VARCHAR(800)", "BOOLEAN", "VARCHAR(800)", "ARRAY", "VARCHAR(800)", "VARCHAR(800)" };

	private static String getDataValidationTableName(String sheetName) {
		return sheetName + "_Data_Validation";
	}

	/**
	 * Insert validation map into engine
	 * 
	 * @param sheetName
	 * @param dataValidationMap
	 */
	public static void insertValidationMap(IEngine engine, String sheetName, Map<String, Object> dataValidationMap) {
		String tableName = getDataValidationTableName(sheetName);
		Object[] cols = new Object[] { tableName, "header", "type", "validationType", "range", "emptyCells", "operation", "values", "f1", "f2" };
		// make create statement
		String create = RdbmsQueryBuilder.makeOptionalCreate(tableName, colNames, types);
		engine.insertData(create);

		// create the prepared statement using the sql query defined
		PreparedStatement ps = (PreparedStatement) engine.doAction(ACTION_TYPE.BULK_INSERT, (cols));
		try {
			for (String header : dataValidationMap.keySet()) {
				int i = 1;
				ps.setString(i++, header);
				Map headerMeta = (Map) dataValidationMap.get(header);
				String type = (String) dataValidationMap.get("type");
				ps.setString(i++, type);
				String validationType = (String) headerMeta.get("validationType");
				ps.setString(i++, validationType);
				String range = (String) headerMeta.get("range");
				ps.setString(i++, range);
				boolean emptyCells = (boolean) headerMeta.get("emptyCells");
				ps.setBoolean(i++, emptyCells);
				String operation = (String) headerMeta.get("operator");
				ps.setString(i++, operation);
				Vector values = (Vector) headerMeta.get("values");
				Array array = null;
				if (values != null) {
					array = ps.getConnection().createArrayOf("VARCHAR", values.toArray());
				}
				ps.setArray(i++, array);
				String f1 = (String) headerMeta.get("f1");
				ps.setString(i++, f1);
				String f2 = (String) headerMeta.get("f2");
				ps.setString(i++, f2);
				ps.addBatch();
			}
			ps.executeBatch();
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

	public static Map<String, Object> getDataValidationMap(IEngine engine, String sheetName) {
		String tableName = getDataValidationTableName(sheetName);
		Map<String, Object> retMap = new HashMap<>();
		try {
			String query = RdbmsQueryBuilder.makeSelect(tableName, Arrays.asList(colNames), true);
			Map<String, Object> engineMap = (Map<String, Object>) engine.execQuery(query);
			ResultSet rs = (ResultSet) engineMap.get(RDBMSNativeEngine.RESULTSET_OBJECT);
			while (rs.next()) {
				int i =1;
				Map<String, Object> headerMap = new HashMap<>();
				String header = rs.getString(i++);

				String validationType = rs.getString(i++);
				headerMap.put("validationType", validationType);
				String range = rs.getString(i++);
				headerMap.put("range", range);
				boolean emptyCells = rs.getBoolean(i++);
				headerMap.put("emptyCells", emptyCells);
				String operation = rs.getString(i++);
				if (operation != null) {
					headerMap.put("operation", operation);
				}
				Array values = rs.getArray(i++);
				if (values != null) {
					Object[] objects = (Object[]) values.getArray();
					headerMap.put("values", objects);
				}
				String f1 = rs.getString(i++);
				if (f1 != null) {
					headerMap.put("f1", f1);
				}
				String f2 = rs.getString(i++);
				if (f2 != null) {
					headerMap.put("f2", f2);
				}
				retMap.put(header, headerMap);
			}

		} catch (SQLException e) {
			e.printStackTrace();
		}
		return retMap;
	}
	
}
