package prerna.ds;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Map;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import prerna.ds.util.H2FilterHash;
import prerna.ds.util.RdbmsFrameUtility;

import java.util.HashMap;
import java.util.List;

public class RdbmsTableMetaData {

	private static final Logger LOGGER = LogManager.getLogger(RdbmsTableMetaData.class.getName());
	
	private static final String DEFAULT_OPTIONS = ":LOG=0;CACHE_SIZE=65536;LOCK_MODE=1;UNDO_LOG=0";
	private static final String DEFAULT_SCHEMA = "test";
	private static final String DEFAULT_IN_MEM_CONNECTION_PREFIX = "jdbc:h2:mem:";
	
	private final String tableName;
	private Connection connection;
	private H2FilterHash filterHash;
	private String schema;
	private String options;
	private Map<String, String> columnIndexMap;
	
	public RdbmsTableMetaData() {
		setDefaultValues();
		this.tableName = RdbmsFrameUtility.getNewTableName();
	}
	
	public RdbmsTableMetaData(Connection conn) {
		setDefaultValues();
		this.tableName = RdbmsFrameUtility.getNewTableName();
		this.connection = conn;
	}
	
	public RdbmsTableMetaData(String tableName, Connection conn) {
		setDefaultValues();
		this.tableName = tableName;
		this.connection = conn;
	}
	
	private void setDefaultValues() {
		this.options = DEFAULT_OPTIONS;
		this.schema = DEFAULT_SCHEMA;
		this.columnIndexMap = new HashMap<>();
		this.filterHash = new H2FilterHash();
	}
	
	public String getTableName() {
		return tableName;
	}
	
	public String getViewTableName() {
		return tableName;
	}
	
	public void setConnection(Connection conn) {
		this.connection = conn;
	}
	
	public Connection getConnection() {
		if (this.connection == null) {
			try {

				Class.forName("org.h2.Driver");
				// jdbc:h2:~/test

				// this will have to update
				String url = DEFAULT_IN_MEM_CONNECTION_PREFIX + this.schema + options;
				this.connection = DriverManager.getConnection(url, "sa", "");
				System.out.println("The connection is.. " + url);
				// getConnection("jdbc:h2:C:/Users/pkapaleeswaran/h2/test.db;LOG=0;CACHE_SIZE=65536;LOCK_MODE=0;UNDO_LOG=0",
				// "sa", "");

				// Class.forName("nl.cwi.monetdb.jdbc.MonetDriver");
				// conn =
				// DriverManager.getConnection("jdbc:monetdb://localhost:50000/demo", "monetdb", "monetdb");
				// ResultSet rs = conn.createStatement().executeQuery("Select count(*) from voyages");

			} catch (ClassNotFoundException e) {
				e.printStackTrace();
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
		return this.connection;
	}
	
	public String getSchema() {
		return this.schema;
	}
	
	public void setSchema(String schema) {
		if (schema != null) {
			if (!this.schema.equals(schema)) {
				LOGGER.info("Schema being modified from: '" + this.schema + "' to new schema for user: '" + schema + "'");
				
				this.schema = schema;
				if (schema.equalsIgnoreCase("-1")) {
					this.schema = "test";
				}
				System.err.println("SCHEMA NOW... >>> " + this.schema);
				this.connection = null;
				getConnection();
			}
		}
	}
	
	public boolean isJoined() {
		return false;
	}
	
	public boolean hasColumnIndex(String tableName, String columnName) {
		return columnIndexMap.containsKey(tableName+"+++"+columnName);
	}
	
	public Map<String, String> getColumnIndexMap() {
		return this.columnIndexMap;
	}
	
	public void clearColumnIndices() {
		this.columnIndexMap.clear();
	}
	
	public String cleanColumn(String columnHeader) {
		return columnHeader;
	}
	
	public List<String> cleanColumns(List<String> columns) {
		return columns;
	}
	
	public H2FilterHash getFilters() {
		return this.filterHash;
	}
	
	public boolean isInMem() {
		try {
			return getConnection().getMetaData().getURL().startsWith(DEFAULT_IN_MEM_CONNECTION_PREFIX);
		} catch (SQLException e) {
			e.printStackTrace();
			return true; //what's by default?
		}
	}
}
