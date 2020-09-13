package prerna.rpa.db.jdbc;

import java.sql.JDBCType;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.quartz.JobExecutionException;

import prerna.util.Utility;

public class JDBCUtil {

	private static final Logger LOGGER = LogManager.getLogger(JDBCUtil.class.getName());

	private static final String NEW_LINE = System.lineSeparator();
	
	private JDBCUtil() {
		throw new IllegalStateException("Utility class");
	}
	
	public static String generateCreateTableSQL(String[] columnHeaders, String[] columnTypes, String tableName) {
		if (columnHeaders.length != columnTypes.length) {
			throw new IllegalArgumentException("The number of column headers must equal the number of column types.");
		}
		int nCol = columnHeaders.length;
		StringBuilder createTableSQL = new StringBuilder();
		createTableSQL.append("CREATE TABLE ");
		createTableSQL.append(tableName);
		createTableSQL.append(" (");
		createTableSQL.append(NEW_LINE);
		for (int c = 0; c < nCol; c++) {
			if (c > 0) {
				createTableSQL.append(", ");
				createTableSQL.append(NEW_LINE);
			}
			createTableSQL.append(columnHeaders[c]);
			createTableSQL.append(" ");
			createTableSQL.append(columnTypes[c]);
		}
		createTableSQL.append(NEW_LINE);
		createTableSQL.append(");");
		return createTableSQL.toString();
	}
	
	public static String generateCreateTableSQL(ResultSet rs, String tableName) throws SQLException {
		
		// First get the column headers and types from the result set, 
		// then pass into function to make the create table sql
		
		// Get meta data from the result set
		ResultSetMetaData rsmd = rs.getMetaData();
		int nCol = rsmd.getColumnCount();
		
		// Get the column headers and types
		String[] columnHeaders = new String[nCol];
		String[] columnTypes = new String[nCol];
		
		// Grab these from the meta data
		for (int c = 0; c < nCol; c++) {
			
			// Header is easy
			columnHeaders[c] = rsmd.getColumnLabel(c + 1);
			
			// Type requires some extra logic
			
			// Get the type, precision, and scale
			StringBuilder columnTypeString = new StringBuilder();
			String columnType = JDBCType.valueOf(rsmd.getColumnType(c + 1)).getName();
			int precision = rsmd.getPrecision(c + 1);
			int scale = rsmd.getScale(c + 1);
			
			// Extra logic for VARCHAR
			if (columnType.equals("VARCHAR")) {
				
				// For VARCHAR use display size
				precision = rsmd.getColumnDisplaySize(c + 1);
				
				// Don't allow VARCHAR over 4000
				if (precision > 4000) {
					precision = 4000;
				}
			}
			
			// Append the type and precision
			columnTypeString.append(columnType);

			// 0 returned when not applicable
			if (precision != 0) {
				columnTypeString.append("(");
				columnTypeString.append(precision);
				if (scale != 0) {
					columnTypeString.append(",");
					columnTypeString.append(scale);
				}
				columnTypeString.append(")");
			}
			columnTypes[c] = columnTypeString.toString();			
		}
		return generateCreateTableSQL(columnHeaders, columnTypes, tableName);
	}

	public static String generateInsertSQL(String[] columnHeaders, String tableName) {
		StringBuilder insertSQL = new StringBuilder();
		insertSQL.append("INSERT INTO ");
		insertSQL.append(tableName);
		insertSQL.append(" (");
		insertSQL.append(String.join(", ",  columnHeaders));
		insertSQL.append(") VALUES (");
		for (int i = 0; i < columnHeaders.length; i++) {
			insertSQL.append("?");
			if (i < columnHeaders.length - 1) insertSQL.append(", ");	
		}
		insertSQL.append(");");
		return insertSQL.toString();
	}
	
	public static String generateInsertSQL(ResultSet rs, String tableName) throws SQLException {
		ResultSetMetaData rsmd = rs.getMetaData();
		int nCol = rsmd.getColumnCount();
		String[] columnHeaders = new String[nCol];
		for (int c = 0; c < nCol; c++) {
			columnHeaders[c] = rsmd.getColumnLabel(c + 1);
		}
		return generateInsertSQL(columnHeaders, tableName);
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
	
	public static final int logResults(ResultSet results, int columnWidth) throws SQLException {
		ResultSetMetaData rsmd = results.getMetaData();
		int nCol = rsmd.getColumnCount();
		
		// Get the header
		String[] columnHeaders = new String[nCol];
		for (int c = 0; c < nCol; c++) {
			columnHeaders[c] = rsmd.getColumnLabel(c + 1);
		}
		
		// Print the header
		StringBuilder resultsString = new StringBuilder();
		resultsString.append(NEW_LINE);
		resultsString.append(formatRow(columnHeaders, columnWidth));
		
		// Print the rows
		List<String[]> rows = getRows(results);
		for (String[] row : rows) {
			String rowString = formatRow(row, columnWidth);
			resultsString.append(rowString);
		}
		
		// Print the header again
		resultsString.append(formatRow(columnHeaders, columnWidth));
		LOGGER.info(Utility.cleanLogString(resultsString.toString()));
		return rows.size();
	}

	public static List<String[]> getRows(ResultSet results) throws SQLException {
		ResultSetMetaData rsmd = results.getMetaData();
		int nCol = rsmd.getColumnCount();
				
		// Populate the list
		List<String[]> rows = new ArrayList<>();
		while (results.next()) {
			String[] row = new String[nCol];
			for (int c = 0; c < nCol; c++) {
				row[c] = results.getString(c + 1);
			}
			rows.add(row);
		}
		return rows;
	}
	
	private static final String formatRow(String[] row, int columnWidth) {
		String formatterString = "%-" + columnWidth + "." + (columnWidth - 2) + "s|";
		StringBuilder rowString = new StringBuilder();
		rowString.append("|");
		for (Object element : row) {
			rowString.append(String.format(formatterString, element));
		}
		rowString.append(NEW_LINE);
		return rowString.toString();
	}
	
}
