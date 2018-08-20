package prerna.engine.impl.rdbms;

import java.io.File;
import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.Vector;

import org.h2.tools.Server;

import prerna.ds.util.RdbmsQueryBuilder;
import prerna.engine.api.IEngine;
import prerna.engine.impl.SmssUtilities;
import prerna.query.querystruct.filters.GenRowFilters;
import prerna.query.querystruct.filters.SimpleQueryFilter;
import prerna.query.querystruct.filters.SimpleQueryFilter.FILTER_TYPE;
import prerna.query.querystruct.selectors.IQuerySelector;
import prerna.query.querystruct.selectors.QueryColumnSelector;
import prerna.query.querystruct.update.UpdateQueryStruct;
import prerna.util.Constants;
import prerna.util.DIHelper;
import prerna.util.Utility;

public class AuditDatabase {

	private static final String DIR_SEPARATOR = java.nio.file.FileSystems.getDefault().getSeparator();
	private static final int INSERT_SIZE = 10;
	
	private Connection conn;
	private Server server;
	private String serverUrl;
	
	private IEngine engine;
	private String engineId;
	private String engineName;
	
	private Map<String, String[]> primaryKeyCache = new HashMap<String, String[]>();

	public AuditDatabase() {
		
	}
	
	/**
	 * First method that needs to be run to generate the actual connection details
	 * @param engineId
	 * @param engineName
	 */
	public void init(IEngine engine, String engineId, String engineName) {
		this.engine = engine;
		this.engineId = engineId;
		this.engineName = engineName;
		
		String dbFolder = DIHelper.getInstance().getProperty(Constants.BASE_FOLDER);
		dbFolder += DIR_SEPARATOR + "db" + DIR_SEPARATOR + SmssUtilities.getUniqueName(engineName, engineId);
		
		String fileLocation = dbFolder + DIR_SEPARATOR + "audit_log_database";
		File f = new File(fileLocation + ".mv.db");
		if(!f.exists()) {
			try {
				f.createNewFile();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		
		RdbmsConnectionBuilder builder = new RdbmsConnectionBuilder(RdbmsConnectionBuilder.CONN_TYPE.DIRECT_CONN_URL);
		try {
			String port = Utility.findOpenPort();
			// create a random user and password
			// get the connection object and start up the frame
			server = Server.createTcpServer("-tcpPort", port, "-tcpAllowOthers");
			serverUrl = "jdbc:h2:" + server.getURL() + "/nio:" + fileLocation;
			server.start();
			
			// update the builder
			builder.setConnectionUrl(serverUrl);
		} catch (SQLException e) {
			e.printStackTrace();
		}
		
		builder.setDriver("H2_DB");
		builder.setUserName("sa");
		builder.setPassword("");
		
		System.out.println("Audit connection url is " + builder.getConnectionUrl());
		System.out.println("Audit connection url is " + builder.getConnectionUrl());
		System.out.println("Audit connection url is " + builder.getConnectionUrl());

		try {
			this.conn = builder.build();
		} catch (SQLException e) {
			e.printStackTrace();
		}
		
		// create the tables if necessary
		String[] headers = new String[]{"AUTO_INCREMENT", "ID", "TYPE", "TABLE", "KEY_COLUMN", "KEY_COLUMN_VALUE", "ALTERED_COLUMN", "OLD_VALUE", "NEW_VALUE", "TIMESTAMP", "USER"};
		String[] types = new String[]{"IDENTITY", "VARCHAR(50)", "VARCHAR(50)", "VARCHAR(200)", "VARCHAR(200)", "VARCHAR(200)", "VARCHAR(200)", "VARCHAR(200)", "VARCHAR(200)", "TIMESTAMP", "VARCHAR(200)"};
		execQ(RdbmsQueryBuilder.makeOptionalCreate("TEST_TABLE", headers, types));
	}
	
	public synchronized void auditUpdateQuery(UpdateQueryStruct updateQs) {
		List<IQuerySelector> selectors = updateQs.getSelectors();
		int numUpdates = selectors.size();
		List<String> selectorQsName = new Vector<String>(numUpdates);
		List<Object> values = updateQs.getValues();
		
		// get all the columns of the selectors
		for(int i = 0; i < selectors.size(); i++) {
			selectorQsName.add(((QueryColumnSelector) selectors.get(i)).getQueryStructName());
		}
		
		// let us collect all the constraints
		// if this is just a primary key constraint
		// it will just be key_qs_name to key_column_value
		Map<String, String> constraintMap = new HashMap<String, String>();

		GenRowFilters grf = updateQs.getCombinedFilters();
		List<SimpleQueryFilter> filters = grf.getAllSimpleQueryFilters();
		for(SimpleQueryFilter f : filters) {
			// grab the values from the filter
			IQuerySelector col = null;
			Object colVal = null;
			if(f.getFilterType() == FILTER_TYPE.COL_TO_VALUES) {
				col = (IQuerySelector) f.getLComparison().getValue();
				colVal = f.getRComparison().getValue();
			} else if(f.getFilterType() == FILTER_TYPE.VALUES_TO_COL) {
				col = (IQuerySelector) f.getRComparison().getValue();
				colVal = f.getLComparison().getValue();
			}
		
			String qsname = null;
			String val = null;
			
			if(colVal instanceof List) {
				if(((List) colVal).size() == 1) {
					val = ((List) colVal).get(0).toString();
				} else {
					val = colVal.toString();
				}
			} else {
				val = colVal.toString();
			}
			
			qsname = col.getQueryStructName();
			constraintMap.put(qsname, val);
		}
		
		// the filter column that is not in the update
		// is going to be the primary key
		String primaryKeyTable = null;
		String primaryKeyColumn = null;
		String primaryKeyValue = null;
		
		for(String filterQsName : constraintMap.keySet()) {
			if(!selectorQsName.contains(filterQsName)) {
				// i guess you are the primary key 
				String[] split = null;
				if(filterQsName.contains("__")) {
					split = filterQsName.split("__");
				} else {
					split = getPrimKey(filterQsName);
				}
				primaryKeyTable = split[0];
				primaryKeyColumn = split[1];				
				primaryKeyValue = constraintMap.get(filterQsName) + "";
			}
		}
		
		StringBuilder inserts = new StringBuilder();
		
		String id = UUID.randomUUID().toString();
		String time = getTime();

		for(int i = 0; i < numUpdates; i++) {
			Object[] insert = new Object[INSERT_SIZE];
			insert[0] = id;
			insert[1] = "UPDATE";
			insert[2] = primaryKeyTable;
			insert[3] = primaryKeyColumn;
			insert[4] = primaryKeyValue;
			
			IQuerySelector selector = selectors.get(i);
			String alteredColumn = ((QueryColumnSelector) selector).getColumn();
			String newValue = values.get(i) + "";

			String qsname = selector.getQueryStructName();
			String oldValue = constraintMap.get(qsname);

			insert[5] = alteredColumn;
			insert[6] = oldValue;
			insert[7] = newValue;
			insert[8] = time;
			insert[9] = "themaherkhalil";
			
			// get a combination of all the insert
			inserts.append(getAuditInsert(insert));
			inserts.append(";");
		}
		
		// execute the inserts
		execQ(inserts.toString());
	}
	
	public String getAuditInsert(Object[] data) {
		String[] headers = new String[]{"ID", "TYPE", "TABLE", "KEY_COLUMN", "KEY_COLUMN_VALUE", "ALTERED_COLUMN", "OLD_VALUE", "NEW_VALUE", "TIMESTAMP", "USER"};
		String[] types = new String[]{"VARCHAR(50)", "VARCHAR(50)", "VARCHAR(200)", "VARCHAR(200)", "VARCHAR(200)", "VARCHAR(200)", "VARCHAR(200)", "VARCHAR(200)", "TIMESTAMP", "VARCHAR(200)"};
		return RdbmsQueryBuilder.makeInsert("TEST_TABLE", headers, types, data);
	}
	
	/**
	 * 
	 * @param q
	 */
	private void execQ(String q) {
		Statement stmt = null;
		try {
			stmt = this.conn.createStatement();
			stmt.execute(q);
		} catch (SQLException e) {
			e.printStackTrace();
		} finally {
			if(stmt != null) {
				try {
					stmt.close();
				} catch (SQLException e) {
					e.printStackTrace();
				}
			}
		}
	}
	
	private String getTime() {
		java.sql.Timestamp t = java.sql.Timestamp.valueOf(LocalDateTime.now());
		return t.toString();
	}
	
	private String[] getPrimKey(String conceptualName) {
		if(primaryKeyCache.containsKey(conceptualName)){
			return primaryKeyCache.get(conceptualName);
		}

		// we dont have it.. so query for it
		String conceptualURI = "http://semoss.org/ontologies/Concept/" + conceptualName;
		String uri = this.engine.getPhysicalUriFromConceptualUri(conceptualURI);

		// since we also have the URI, just store it
		String colName = Utility.getClassName(uri);
		String tableName = Utility.getInstanceName(uri);
		String[] split = new String[]{tableName, colName};
		
		// store the value
		primaryKeyCache.put(conceptualName, split);
		return split;
	}
}
