package prerna.rpa.quartz.jobs.jdbc;

import java.sql.JDBCType;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.quartz.JobExecutionException;

public class JDBCUtil {

	private static final Logger LOGGER = LogManager.getLogger(JDBCUtil.class.getName());
		
	public static String generateCreateTableSQL(ResultSet rs, String tableName) throws SQLException {
		ResultSetMetaData rsmd = rs.getMetaData();
		int nCol = rsmd.getColumnCount();
		StringBuilder createTableSQL = new StringBuilder();
		createTableSQL.append("CREATE TABLE ");
		createTableSQL.append(tableName);
		createTableSQL.append(" (\n");
		for (int c = 1; c <= nCol; c++) {
			if (c > 1) createTableSQL.append(", \n");
			
			// Preserve AS, for example col_1 AS column_1 will be created as column_1
			createTableSQL.append(rsmd.getColumnLabel(c));
			createTableSQL.append(" ");
			
			// Get the type and precision
			String columnType = JDBCType.valueOf(rsmd.getColumnType(c)).getName();
			int precision = rsmd.getPrecision(c);
			
			// Don't allow VARCHAR over the max of 65535, instead use TEXT
			if (precision > 65535 && columnType.equals("VARCHAR")) {
				columnType = "TEXT";
				precision = 0;
			}
			
			// Append the type and precision
			createTableSQL.append(columnType);

			// 0 returned when not applicable
			if (precision != 0) {
				createTableSQL.append("(");
				createTableSQL.append(precision);
				createTableSQL.append(")");
			}
		}
		createTableSQL.append("\n);");
		return createTableSQL.toString();
	}

	public static String generateInsertSQL(ResultSet rs, String tableName) throws SQLException {
		List<String> colNames = JDBCUtil.retrieveColumnNames(rs);
		StringBuilder insertSQL = new StringBuilder();
		insertSQL.append("INSERT INTO ");
		insertSQL.append(tableName);
		insertSQL.append(" (");
		insertSQL.append(colNames.stream().collect(Collectors.joining(", ")));
		insertSQL.append(") VALUES (");
		insertSQL.append(colNames.stream().map(c -> "?").collect(Collectors.joining(", ")));
		insertSQL.append(");");
		return insertSQL.toString();
	}
	
	public static List<String> retrieveColumnNames(ResultSet rs) throws SQLException {
		List<String> colNames = new ArrayList<String>();
		ResultSetMetaData rsmd = rs.getMetaData();
		int nCol = rsmd.getColumnCount();
		for (int c = 1; c <= nCol; c++) {
			colNames.add(rsmd.getColumnLabel(c));
		}
		return colNames;
	}
	
	public static final int printToConsole(ResultSet results) throws SQLException {
		ResultSetMetaData metaData = results.getMetaData();
		int nCol = metaData.getColumnCount();
		String[] header = new String[nCol];
		for (int c = 1; c <= nCol; c++) {
			header[c - 1] = metaData.getColumnLabel(c);
		}
		printRow(header);
		int nrow = 0;
		while (results.next()) {
			String[] row = new String[nCol];
			for (int c = 1; c <= nCol; c++) {
				row[c - 1] = results.getString(c);
			}
			printRow(row);
			nrow++;
		}
		printRow(header);
		return nrow;
	}

	public static final void printRow(Object[] row) {
		int length = 15;
		System.out.print("|");
		for (Object element : row) {
			System.out.print(String.format("%-" + length + "." + (length - 2) + "s|", element));
		}
		System.out.println();
	}
	
	public static final void loadDriver(String driver) throws JobExecutionException {
		try {
			Class.forName(driver);
		} catch (ClassNotFoundException e) {
			String failedToLoadDriverMessage = "Failed to load the driver " + driver + ". " + driver;
			LOGGER.error(failedToLoadDriverMessage);
			throw new JobExecutionException(failedToLoadDriverMessage, e);
		}
	}
	
}
