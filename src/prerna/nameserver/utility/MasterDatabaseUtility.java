package prerna.nameserver.utility;

import java.lang.reflect.Modifier;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.Vector;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import prerna.engine.api.IEngine;
import prerna.engine.api.IRawSelectWrapper;
import prerna.engine.impl.rdbms.RDBMSNativeEngine;
import prerna.rdf.engine.wrappers.WrapperManager;
import prerna.test.TestUtilityMethods;
import prerna.util.Constants;
import prerna.util.DIHelper;
import prerna.util.Utility;
import prerna.util.sql.AbstractRdbmsQueryUtil;

/**
 * @author pkapaleeswaran
 *
 */
public class MasterDatabaseUtility {

	// -----------------------------------------   RDBMS CALLS ---------------------------------------
	
	public static void initLocalMaster() throws SQLException {
		RDBMSNativeEngine engine = (RDBMSNativeEngine) Utility.getEngine(Constants.LOCAL_MASTER_DB_NAME);
		Connection conn = engine.makeConnection();
		
		String [] colNames = null;
		String [] types = null;
		
		String schema = engine.getSchema();
		AbstractRdbmsQueryUtil queryUtil = engine.getQueryUtil();
		boolean allowIfExistsTable = queryUtil.allowsIfExistsTableSyntax();
		boolean allowIfExistsIndexs = queryUtil.allowIfExistsIndexSyntax();
		
		// engine table
		colNames = new String[]{"ID", "ENGINENAME", "MODIFIEDDATE", "TYPE"};
		types = new String[]{"varchar(800)", "varchar(800)", "timestamp", "varchar(800)"};
		if(allowIfExistsTable) {
			executeSql(conn, queryUtil.createTableIfNotExists("ENGINE", colNames, types));
		} else {
			// see if table exists
			if(!tableExists(engine, queryUtil, "ENGINE", schema)) {
				// make the table
				executeSql(conn, queryUtil.createTable("ENGINE", colNames, types));
			}
		}
		// add index
		if(allowIfExistsIndexs) {
			executeSql(conn, queryUtil.createIndexIfNotExists("ENGINE_ID_INDEX", "ENGINE", "ID"));
		} else {
			// see if index exists
			if(!indexExists(engine, queryUtil, "ENGINE_ID_INDEX", "ENGINE", schema)) {
				executeSql(conn, queryUtil.createIndex("ENGINE_ID_INDEX", "ENGINE", "ID"));
			}
		}
		
		// engine concept table
		colNames = new String[]{"ENGINE", "PHYSICALNAME", "PARENTPHYSICALID", "PHYSICALNAMEID", "LOCALCONCEPTID", "PK", "PROPERTY", "ORIGINAL_TYPE", "PROPERTY_TYPE", "ADDITIONAL_TYPE"};
		types = new String[]{"varchar(800)", "varchar(800)", "varchar(800)", "varchar(800)", "varchar(800)", "boolean", "boolean", "varchar(800)", "varchar(800)", "varchar(800)"};
		if(allowIfExistsTable) {
			executeSql(conn, queryUtil.createTableIfNotExists("ENGINECONCEPT", colNames, types));
		} else {
			// see if table exists
			if(!tableExists(engine, queryUtil, "ENGINECONCEPT", schema)) {
				// make the table
				executeSql(conn, queryUtil.createTable("ENGINECONCEPT", colNames, types));
			}
		}
		// add index
		if(allowIfExistsIndexs) {
			List<String> iCols = new Vector<String>();
			iCols.add("ENGINE");
			iCols.add("LOCALCONCEPTID");
			executeSql(conn, queryUtil.createIndexIfNotExists("ENGINE_CONCEPT_ENGINE_LOCAL_CONCEPT_ID", "ENGINECONCEPT", iCols));
		} else {
			// see if index exists
			if(!indexExists(engine, queryUtil, "ENGINE_CONCEPT_ENGINE_LOCAL_CONCEPT_ID", "ENGINECONCEPT", schema)) {
				List<String> iCols = new Vector<String>();
				iCols.add("ENGINE");
				iCols.add("LOCALCONCEPTID");
				executeSql(conn, queryUtil.createIndex("ENGINE_CONCEPT_ENGINE_LOCAL_CONCEPT_ID", "ENGINECONCEPT", iCols));
			}
		}
		
		// concept table
		colNames = new String[]{"LOCALCONCEPTID", "CONCEPTUALNAME", "LOGICALNAME", "DOMAINNAME", "GLOBALID"};
		types = new String[]{"varchar(800)", "varchar(800)", "varchar(800)", "varchar(800)", "varchar(800)"};
		if(allowIfExistsTable) {
			executeSql(conn, queryUtil.createTableIfNotExists("CONCEPT", colNames, types));
		} else {
			// see if table exists
			if(!tableExists(engine, queryUtil, "CONCEPT", schema)) {
				// make the table
				executeSql(conn, queryUtil.createTable("CONCEPT", colNames, types));
			}
		}
		// add index
		if(allowIfExistsIndexs) {
			executeSql(conn, queryUtil.createIndexIfNotExists("CONCEPT_ID_INDEX", "CONCEPT", "LOCALCONCEPTID"));
		} else {
			// see if index exists
			if(!indexExists(engine, queryUtil, "CONCEPT_ID_INDEX", "CONCEPT", schema)) {
				executeSql(conn, queryUtil.createIndex("CONCEPT_ID_INDEX", "CONCEPT", "LOCALCONCEPTID"));
			}
		}
		
		// relation table
		colNames = new String[]{"ID", "SOURCEID", "TARGETID", "GLOBALID"};
		types = new String[]{"varchar(800)", "varchar(800)", "varchar(800)", "varchar(800)"};
		if(allowIfExistsTable) {
			executeSql(conn, queryUtil.createTableIfNotExists("RELATION", colNames, types));
		} else {
			// see if table exists
			if(!tableExists(engine, queryUtil, "RELATION", schema)) {
				// make the table
				executeSql(conn, queryUtil.createTable("RELATION", colNames, types));
			}
		}
		
		// engine relation table
		colNames = new String[]{"ENGINE", "RELATIONID", "INSTANCERELATIONID", "SOURCECONCEPTID", "TARGETCONCEPTID", "SOURCEPROPERTY", "TARGETPROPERTY", "RELATIONNAME"};
		types = new String[]{"varchar(800)", "varchar(800)","varchar(800)", "varchar(800)", "varchar(800)", "varchar(800)", "varchar(800)", "varchar(800)"};
		if(allowIfExistsTable) {
			executeSql(conn, queryUtil.createTableIfNotExists("ENGINERELATION", colNames, types));
		} else {
			// see if table exists
			if(!tableExists(engine, queryUtil, "ENGINERELATION", schema)) {
				// make the table
				executeSql(conn, queryUtil.createTable("ENGINERELATION", colNames, types));
			}
		}
		
		// kv store
		colNames = new String[]{"K","V"};
		types = new String[]{"varchar(800)", "varchar(800)"};
		if(allowIfExistsTable) {
			executeSql(conn, queryUtil.createTableIfNotExists("KVSTORE", colNames, types));
		} else {
			// see if table exists
			if(!tableExists(engine, queryUtil, "KVSTORE", schema)) {
				// make the table
				executeSql(conn, queryUtil.createTable("KVSTORE", colNames, types));
			}
		}
		
		// concept metadata
		updateMetadataTable(engine, conn, queryUtil, Constants.CONCEPT_METADATA_TABLE, schema);
		colNames = new String[] {Constants.PHYSICAL_NAME_ID, Constants.KEY, Constants.VALUE };
		types = new String[] { "varchar(800)", "varchar(800)", "varchar(20000)" };
		if(allowIfExistsTable) {
			executeSql(conn, queryUtil.createTableIfNotExists(Constants.CONCEPT_METADATA_TABLE, colNames, types));
		} else {
			// see if table exists
			if(!tableExists(engine, queryUtil, Constants.CONCEPT_METADATA_TABLE, schema)) {
				// make the table
				executeSql(conn, queryUtil.createTable(Constants.CONCEPT_METADATA_TABLE, colNames, types));
			}
		}
		// add index
		if(allowIfExistsIndexs) {
			executeSql(conn, queryUtil.createIndexIfNotExists("CONCEPTMETADATA_KEY_INDEX", "CONCEPTMETADATA", "KEY"));
		} else {
			// see if index exists
			if(!indexExists(engine, queryUtil, "CONCEPTMETADATA_KEY_INDEX", "CONCEPTMETADATA", schema)) {
				executeSql(conn, queryUtil.createIndex("CONCEPTMETADATA_KEY_INDEX", "CONCEPTMETADATA", "KEY"));
			}
		}
				
		// x-ray config
		colNames = new String[]{"FILENAME", "CONFIG" };
		types = new String[]{"varchar(800)", "varchar(20000)" };
		if(allowIfExistsTable) {
			executeSql(conn, queryUtil.createTableIfNotExists("XRAYCONFIGS", colNames, types));
		} else {
			// see if table exists
			if(!tableExists(engine, queryUtil, "XRAYCONFIGS", schema)) {
				// make the table
				executeSql(conn, queryUtil.createTable("XRAYCONFIGS", colNames, types));
			}
		}
		
		// bitly
		colNames = new String[]{"FANCY", "EMBED"};
		types = new String[]{"varchar(800)", "varchar(20000)" };
		if(allowIfExistsTable) {
			executeSql(conn, queryUtil.createTableIfNotExists("BITLY", colNames, types));
		} else {
			// see if table exists
			if(!tableExists(engine, queryUtil, "BITLY", schema)) {
				// make the table
				executeSql(conn, queryUtil.createTable("BITLY", colNames, types));
			}
		}
	}
	
	@Deprecated
	private static void updateMetadataTable(RDBMSNativeEngine engine, Connection conn, AbstractRdbmsQueryUtil queryUtil, String tableName, String schema) throws SQLException {
		if(tableExists(engine, queryUtil, tableName, schema)) {
			boolean allowIfExists = queryUtil.allowIfExistsModifyColumnSyntax();
			if(queryUtil.allowDropColumn()) {
				if(allowIfExists) {
					executeSql(conn, queryUtil.alterTableDropColumnIfExists(tableName, "LOCALCONCEPTID"));
				} else {
					// check column exists in table
				}
			}
			if(queryUtil.allowAddColumn()) {
				if(allowIfExists) {
					executeSql(conn, queryUtil.alterTableAddColumnIfNotExists(tableName, "PHYSICALNAMEID", "VARCHAR(800)"));
				} else {
					// check column exists in table
					
				}
			}
			engine.commit();
		}
	}
	
	/**
	 * Helper method to see if a table exits based on Query Utility class
	 * @param queryUtil
	 * @param tableName
	 * @param schema
	 * @return
	 */
	private static boolean tableExists(RDBMSNativeEngine engine, AbstractRdbmsQueryUtil queryUtil, String tableName, String schema) {
		String tableCheckQ = queryUtil.tableExistsQuery(tableName, schema);
		IRawSelectWrapper wrapper = WrapperManager.getInstance().getRawWrapper(engine, tableCheckQ);
		try {
			if(wrapper.hasNext()) {
				return true;
			}
			return false;
		} finally {
			wrapper.cleanUp();
		}
	}
	
	/**
	 * Helper method to see if an index exists based on Query Utility class
	 * @param queryUtil
	 * @param indexName
	 * @param tableName
	 * @param schema
	 * @return
	 */
	private static boolean indexExists(RDBMSNativeEngine engine, AbstractRdbmsQueryUtil queryUtil, String indexName, String tableName, String schema) {
		String indexCheckQ = queryUtil.getIndexDetails(indexName, tableName, schema);
		IRawSelectWrapper wrapper = WrapperManager.getInstance().getRawWrapper(engine, indexCheckQ);
		try {
			if(wrapper.hasNext()) {
				return true;
			}
			return false;
		} finally {
			wrapper.cleanUp();
		}
	}
	
	private static void executeSql(Connection conn, String sql) throws SQLException {
		Statement stmt = null;
		try {
			stmt = conn.createStatement();
			stmt.execute(sql);
		} finally {
			try {
				stmt.close();
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
	}
	
	/**
	 * Return all the logical names for a given conceptual name
	 * @param conceptualName
	 * @return
	 */
	public static Set<String> getAllLogicalNamesFromConceptualRDBMS(String conceptualName) {
		RDBMSNativeEngine engine = (RDBMSNativeEngine) Utility.getEngine(Constants.LOCAL_MASTER_DB_NAME);
		Connection conn = engine.makeConnection();
		
		Set<String> logicalNames = new TreeSet<String>();
		ResultSet rs = null;
		Statement stmt = null;
		try {
			String logicalQuery = "select distinct c.logicalname from "
								+ "concept c, engineconcept ec, engine e where ec.localconceptid=c.localconceptid and "
								+ "c.conceptualname = '" + conceptualName + "' order by c.logicalname";
			stmt = conn.createStatement();
			rs = stmt.executeQuery(logicalQuery);
			while (rs.next()) {
				String logicalName = rs.getString(1);
				logicalNames.add(logicalName);
			}
		} catch(Exception ex) {
			ex.printStackTrace();
		} finally {
			closeStreams(stmt, rs);
		}
		return logicalNames;
	}
	
	/**
	 * Return all the logical names for a given conceptual name
	 * @param conceptualName
	 * @return
	 */
	public static List<String> getAllLogicalNamesFromConceptualRDBMS(List<String> conceptualName, String engineFilter) {
		RDBMSNativeEngine engine = (RDBMSNativeEngine) Utility.getEngine(Constants.LOCAL_MASTER_DB_NAME);
		Connection conn = engine.makeConnection();
		
		//select logicalname from concept where conceptualname='MovieBudget' and conceptualname != logicalname
		//select distinct c.conceptualname, ec.physicalname from concept c, engineconcept ec, engine e where ec.localconceptid=c.localconceptid and ec.physicalname in ('Title', 'Actor');
		
		String engineFilterStr = "";
		if(engineFilter != null && !engineFilter.isEmpty()) {
			engineFilterStr = " and e.enginename = '" + engineFilter + "'";
		}
		
		String conceptList = makeListToString(conceptualName);
		List <String> logicalNames = new ArrayList<String>();
		ResultSet rs = null;
		Statement stmt = null;
		try {
			String logicalQuery = "select distinct c.logicalname from "
								+ "concept c, engineconcept ec, engine e where ec.localconceptid=c.localconceptid and "
								+ "c.conceptualname in " + conceptList + engineFilterStr + " order by c.logicalname";
			stmt = conn.createStatement();
			rs = stmt.executeQuery(logicalQuery);
			while (rs.next()) {
				String logicalName = rs.getString(1);
				logicalNames.add(logicalName);
			}
		} catch(Exception ex) {
			ex.printStackTrace();
		} finally {
			closeStreams(stmt, rs);
		}
		return logicalNames;
	}

	/**
	 * Get a list of arrays containing [table, column, type] for a given database
	 * @param engineId
	 * @return
	 */
	public static List<Object[]> getAllTablesAndColumns(String engineId) {
		String query = "select distinct c.conceptualname as column, c2.conceptualname as table, ec.property_type as type, ec.pk as pk "
				+ "from engineconcept ec, engineconcept ec2, concept c, concept c2 "
				+ "where ec.engine='" + engineId + "' "
				+ "and ec.localconceptid = c.localconceptid "
				+ "and ec.parentphysicalid = ec2.physicalnameid "
				+ "and ec2.localconceptid = c2.localconceptid "
				+ "order by table, pk desc, column, type";

		List<Object[]> data = new Vector<Object[]>();
		
		RDBMSNativeEngine engine = (RDBMSNativeEngine) Utility.getEngine(Constants.LOCAL_MASTER_DB_NAME);
		Connection conn = engine.makeConnection();
		Statement stmt = null;
		ResultSet rs = null;
		try {
			stmt = conn.createStatement();
			rs = stmt.executeQuery(query);
			while(rs.next()) {
				String column = rs.getString(1);
				String table = rs.getString(2);
				String type = rs.getString(3);
				if(type.equals("DOUBLE") || type.equals("INT")) {
					type = "NUMBER";
				}
				boolean pk = rs.getBoolean(4);
				
				data.add(new Object[]{table, column, type, pk});
			}
			
		} catch (SQLException e) {
			e.printStackTrace();
		} finally {
			closeStreams(stmt, rs);
		}
		
		return data;
	}
	
	/**
	 * Get a list of arrays containing [table, column, type] for a given database
	 * @param engineId
	 * @return
	 */
	public static List<Object[]> getAllTablesAndColumns(Collection<String> engineIds) {
		String engineFilters = makeListToString(engineIds);
		
		String query = "select distinct ec.engine, c.conceptualname as column, c2.conceptualname as table, ec.property_type as type, ec.pk as pk "
				+ "from engineconcept ec, engineconcept ec2, concept c, concept c2 "
				+ "where "
				// if no filters defined, get for all engines
				+ ( engineFilters.equals("()") ? "" : "ec.engine in " + engineFilters + " " )
				+ "and ec.localconceptid = c.localconceptid "
				+ "and ec.parentphysicalid = ec2.physicalnameid "
				+ "and ec2.localconceptid = c2.localconceptid "
				+ "order by table, pk desc, column, type";

		List<Object[]> data = new Vector<Object[]>();
		
		RDBMSNativeEngine engine = (RDBMSNativeEngine) Utility.getEngine(Constants.LOCAL_MASTER_DB_NAME);
		Connection conn = engine.makeConnection();
		Statement stmt = null;
		ResultSet rs = null;
		try {
			stmt = conn.createStatement();
			rs = stmt.executeQuery(query);
			while(rs.next()) {
				String appId = rs.getString(1);
				String column = rs.getString(2);
				String table = rs.getString(3);
				String type = rs.getString(4);
				if(type.equals("DOUBLE") || type.equals("INT")) {
					type = "NUMBER";
				}
				boolean pk = rs.getBoolean(5);
				
				data.add(new Object[]{appId, table, column, type, pk});
			}
		} catch (SQLException e) {
			e.printStackTrace();
		} finally {
			closeStreams(stmt, rs);
		}
		
		return data;
	}
	
	public static List<String[]> getRelationships(Collection<String> engineIds) {
		String engineFilters = makeListToString(engineIds);
		
		String query = "select distinct er.engine, er.sourceproperty, er.targetproperty, er.relationname "
				+ "from enginerelation er "
				// if no filters defined, get for all engines
				+ ( engineFilters.equals("()") ? "" : "where er.engine in " + engineFilters );
		
		List<String[]> data = new Vector<String[]>();

		RDBMSNativeEngine engine = (RDBMSNativeEngine) Utility.getEngine(Constants.LOCAL_MASTER_DB_NAME);
		Connection conn = engine.makeConnection();
		Statement stmt = null;
		ResultSet rs = null;
		try {
			stmt = conn.createStatement();
			rs = stmt.executeQuery(query);
			while(rs.next()) {
				String appId = rs.getString(1);
				String source = rs.getString(2);
				String target = rs.getString(3);
				String rel = rs.getString(4);
				
				data.add(new String[]{appId, source, target, rel});
			}
		} catch (SQLException e) {
			e.printStackTrace();
		} finally {
			closeStreams(stmt, rs);
		}
		
		return data;
	}
	
	/**
	 * Get a list of connections for a given logical name
	 * @param engineFilter 
	 * @param engineId
	 * @return
	 */
	public static List<Map<String, Object>> getDatabaseConnections(List<String> logicalNames, List<String> engineFilter) {
		StringBuilder sb = new StringBuilder();
		int size = logicalNames.size();
		for(int i = 0; i < size; i++) {
			sb.append("'").append(logicalNames.get(i)).append("'");
			if( (i+1) < size) {
				sb.append(",");
			}
		}
		
		// NOTE ::: IMPORTANT THAT THIS MATCHES ALL THE BELOW QUERY NAMES!!!
		String engineFilterStr = "";
		if(engineFilter != null && !engineFilter.isEmpty()) {
			StringBuilder b = new StringBuilder();
			for(int i = 0; i < engineFilter.size(); i++) {
				b.append("'").append(engineFilter.get(i)).append("'");
				if( (i+1) != engineFilter.size()) {
					b.append(",");
				}
			}
			engineFilterStr = " and ec.engine in (" + b.toString() + ")";
		}
		
		/*
		 * Grab all the matching tables and columns based on the logical names
		 * Once we have those, we will grab all the relationships for the tables
		 * and all the other columns that we can traverse to
		 */
		
		// this will store the equivalent table ids to the table and column
		Map<String, Object[]> parentEquivMap = new HashMap<String, Object[]>();
		
		// this will store the ids for the columns we have
		List<String> equivIds = new Vector<String>();
		
		// this will store the parent physical name ids
		List<String> tablePhysicalIds = new Vector<String>();
		
		
		// this will give me all the tables that have the logical name or 
		// have a column with the logical name 
		
		String query = "select distinct ec.parentphysicalid as table, ec2.physicalnameid as equivTableId, ec.physicalnameid equivColumnId,"
				+ " c2.conceptualname as equivConceptTableName, c.conceptualname as equivConceptColumnName, ec.pk as pk" 
				+ " from engineconcept ec, engineconcept ec2, concept c, concept c2"
				+ " where ec.localconceptid in (select localconceptid from concept where logicalname in (" + sb.toString() + "))"
				+ engineFilterStr
				+ " and ec.localconceptid = c.localconceptid"
				+ " and ec.parentphysicalid = ec2.physicalnameid"
				+ " and ec2.localconceptid = c2.localconceptid";
		
		RDBMSNativeEngine engine = (RDBMSNativeEngine) Utility.getEngine(Constants.LOCAL_MASTER_DB_NAME);
		Connection conn = engine.makeConnection();
		Statement stmt = null;
		ResultSet rs = null;
		try {
			stmt = conn.createStatement();
			rs = stmt.executeQuery(query);
			while(rs.next()) {
				String tableId = rs.getString(1);
				tablePhysicalIds.add(tableId);

				String equivTableId = rs.getString(2);
				String equivColumnId = rs.getString(3);
				String equivTableName = rs.getString(4);
				String equivColumnName = rs.getString(5);
				boolean equivPk = rs.getBoolean(6);
				
				// store parent table to what we are joining on
				// so we can extend from one property to other properties within the same table
				parentEquivMap.put(equivTableId, new Object[]{equivTableName, equivColumnName, equivPk});
				
				// store the physical id for the table or column
				// so we can find relaitonships from this
				equivIds.add(equivColumnId);
			}
		} catch (SQLException e) {
			e.printStackTrace();
		} finally {
			closeStreams(stmt, rs);
		}
		
		// now that we have the list
		// we will go ahead and create a filter string
		sb = new StringBuilder();
		size = tablePhysicalIds.size();
		for(int i = 0; i < size; i++) {
			sb.append("'").append(tablePhysicalIds.get(i)).append("'");
			if( (i+1) < size) {
				sb.append(",");
			}
		}
		
		// let us first go ahead and get the properties we can connect to
		query = "select distinct e.id, e.enginename, c2.conceptualname as table, c.conceptualname as column, ec.property_type as type, ec.pk as pk, ec2.physicalnameid"
				+ " from engine e, engineconcept ec, engineconcept ec2, concept c, concept c2"
				+ " where ec2.physicalnameid in (" + sb.toString() + ")"
				+ engineFilterStr
				+ " and e.id = ec.engine"
				+ " and ec.localconceptid = c.localconceptid"
				+ " and ec.parentphysicalid = ec2.physicalnameid"
				+ " and ec2.localconceptid = c2.localconceptid"
				+ " order by table, pk desc, column, type";
		
		List<Map<String, Object>> returnData = new Vector<Map<String, Object>>();
		
		try {
			stmt = conn.createStatement();
			rs = stmt.executeQuery(query);
			while(rs.next()) {
				String engineId = rs.getString(1);
				String engineName = rs.getString(2);
				String table = rs.getString(3);
				String column = rs.getString(4);
				String type = rs.getString(5);
				boolean pk = rs.getBoolean(6);
				String equivId = rs.getString(7);
				
				Object[] equivTableCol = parentEquivMap.get(equivId);
				
				// above query will return the actual match columns as well
				if(equivTableCol[0].toString().equals(table) && equivTableCol[1].toString().equals(column)) {
					continue;
				}
				
				// if we passed the above test, add the valid connection
				Map<String, Object> row = new HashMap<String, Object>();
				// TODO: delete after FE updates payload acceptance
				row.put("app", engineName);
				
				row.put("app_id", engineId);
				row.put("app_name", engineName);

				row.put("table", table);
				row.put("column", column);
				row.put("pk", pk);
				row.put("dataType", type);
				row.put("type", "property");
				row.put("equivTable", equivTableCol[0]);
				row.put("equivColumn", equivTableCol[1]);
				row.put("equivPk", equivTableCol[2]);
				returnData.add(row);
			}
		} catch (SQLException e) {
			e.printStackTrace();
		} finally {
			closeStreams(stmt, rs);
		}
		
		
		// yay, we are done adding the properties
		// let us now go and add the relationships
		sb = new StringBuilder();
		size = equivIds.size();
		for(int i = 0; i < size; i++) {
			sb.append("'").append(equivIds.get(i)).append("'");
			if( (i+1) < size) {
				sb.append(",");
			}
		}
		
		// let me find up and downstream connections for my equivalent concepts
		query = "select distinct e.id, e.enginename, c.conceptualname, c2.conceptualname, ec2.property_type "
				+ " from enginerelation er, engine e, engineconcept ec, engineconcept ec2, concept c, concept c2"
				+ " where er.sourceconceptid in (" + sb.toString() + ")"
				+ engineFilterStr
				+ " and e.id = er.engine "
				+ " and er.sourceconceptid = ec.physicalnameid "
				+ " and ec.localconceptid = c.localconceptid "
				+ " and er.targetconceptid = ec2.physicalnameid "
				+ " and ec2.localconceptid = c2.localconceptid";
		
		try {
			stmt = conn.createStatement();
			rs = stmt.executeQuery(query);
			while(rs.next()) {
				String engineId = rs.getString(1);
				String engineName = rs.getString(2);
				String upstream = rs.getString(3);
				String downstream = rs.getString(4);
				String type = rs.getString(5);
				
				// the downstream nodes
				// mean that the source is the equivalent concept
				
				// if we passed the above test, add the valid connection
				Map<String, Object> row = new HashMap<String, Object>();
				// TODO: delete after FE updates payload acceptance
				row.put("app", engineName);
				
				row.put("app_id", engineId);
				row.put("app_name", engineName);
				row.put("equiv", upstream);
				row.put("table", downstream);
				row.put("dataType", type);
				row.put("type", "downstream");

				returnData.add(row);
			}
		} catch (SQLException e) {
			e.printStackTrace();
		} finally {
			closeStreams(stmt, rs);
		}
		
		// let me repeat for my upstream
		query = "select distinct e.id, e.enginename, c.conceptualname, c2.conceptualname, ec2.property_type "
				+ " from enginerelation er, engine e, engineconcept ec, engineconcept ec2, concept c, concept c2"
				+ " where er.targetconceptid in (" + sb.toString() + ")"
				+ engineFilterStr
				+ " and e.id = er.engine "
				+ " and er.sourceconceptid = ec.physicalnameid "
				+ " and ec.localconceptid = c.localconceptid "
				+ " and er.targetconceptid = ec2.physicalnameid "
				+ " and ec2.localconceptid = c2.localconceptid";
		
		try {
			stmt = conn.createStatement();
			rs = stmt.executeQuery(query);
			while(rs.next()) {
				String engineId = rs.getString(1);
				String engineName = rs.getString(2);
				String upstream = rs.getString(3);
				String downstream = rs.getString(4);
				String type = rs.getString(5);
				
				// the downstream nodes
				// mean that the source is the equivalent concept
				
				// if we passed the above test, add the valid connection
				Map<String, Object> row = new HashMap<String, Object>();
				// TODO: delete after FE updates payload acceptance
				row.put("app", engineName);
				
				row.put("app_id", engineId);
				row.put("app_name", engineName);
				row.put("equiv", downstream);
				row.put("table", upstream);
				row.put("dataType", type);
				row.put("type", "upstream");

				returnData.add(row);
			}
		} catch (SQLException e) {
			e.printStackTrace();
		} finally {
			closeStreams(stmt, rs);
		}
		
		return returnData;
	}

	public static Map<String, Object> getMetamodelRDBMS(String engineId, boolean includeDataTypes) {
		// this needs to be moved to the name server
		// and this needs to be based on local master database
		// need this to be a simple OWL data
		// I dont know if it is worth it to load the engine at this point ?
		// or should I just load it ?
		// need to get local master and pump out the metamodel

		// need to get all the concepts first
		// get the edges next
		
		RDBMSNativeEngine engine = (RDBMSNativeEngine) Utility.getEngine(Constants.LOCAL_MASTER_DB_NAME);
		Connection conn = engine.makeConnection();
		Statement stmt = null;
		ResultSet rs = null;
		
		Map<String, Object> finalHash = new Hashtable<String, Object>();
		
		// idHash - physical ID to the name of the node
		Hashtable <String, String> idHash = new Hashtable<String, String>();
		Hashtable <String, MetamodelVertex> nodeHash = new Hashtable <String, MetamodelVertex>();
		
		Map<String, String> physicalDataTypes = new HashMap<String, String>();
		Map<String, String> dataTypes = new HashMap<String, String>();
		Map<String, String> additionalDataTypes = new HashMap<String, String>();
		
		try {
			String nodeQuery = "";
			if(includeDataTypes) {
				nodeQuery = "select c.conceptualname, ec.physicalname, ec.localconceptid, ec.physicalnameid, ec.parentphysicalid, ec.property, ec.original_type, ec.property_type, ec.additional_type "
					+ "from engineconcept ec, concept c "
					 + "where ec.engine='" + engineId + "' "
					 + "and c.localconceptid=ec.localconceptid order by ec.property";
			} else {
				nodeQuery = "select c.conceptualname, ec.physicalname, ec.localconceptid, ec.physicalnameid, ec.parentphysicalid, ec.property "
						+ "from engineconcept ec, concept c "
						 + "where ec.engine='" + engineId + "' "
						 + "and c.localconceptid=ec.localconceptid order by ec.property";
			}
			stmt = conn.createStatement();
			rs = stmt.executeQuery(nodeQuery);
			while(rs.next()) {
				String conceptualName = rs.getString(1);
				String physicalName = rs.getString(2);
				String physicalId = rs.getString(4);
				String parentPhysicalId = rs.getString(5); 
				
				// sets the physical id to conceptual name
				idHash.put(physicalId, conceptualName);
				
				// gets the conceptual name
				String conceptName = idHash.get(physicalId);
				// because it is ordered by property, this would already be there
				String parentName = idHash.get(parentPhysicalId);
				
				MetamodelVertex node = null;
				// if already there, should we still add it ?
				if(nodeHash.containsKey(parentName)) {
					node = nodeHash.get(parentName);
				} else {
					node = new MetamodelVertex(parentName);
					nodeHash.put(conceptualName, node);
				}
				
				String uniqueName = conceptName;
				
				if(!conceptName.equalsIgnoreCase(parentName)) {
					// store the property
					node.addProperty(conceptName);
					// update the unique name in case we need to include data types
					uniqueName = parentName + "__" + conceptName;					
				} else {
					// store the key
					node.addKey(conceptName);
				}
				
				if(includeDataTypes) {
					String origType = rs.getString(7);
					String cleanType = rs.getString(8);
					String additionalType = rs.getString(9);
					
					if(origType.contains("TYPE:")) {
						origType = origType.replace("TYPE:", "");
					}
					physicalDataTypes.put(uniqueName, origType);
					dataTypes.put(uniqueName, cleanType);
					additionalDataTypes.put(uniqueName, additionalType);
				}
			}
		} catch(SQLException ex) {
			ex.printStackTrace();
		} finally {
			// do not close the stmt
			// reuse it below
			closeStreams(null, rs);
		}
		
		Hashtable <String, Hashtable> edgeHash = new Hashtable<String, Hashtable>();
		try {
			// get the edges next
			//SELECT er.sourceconceptid, er.targetconceptid FROM ENGINERELATION er, engine e where e.id = er.engine and e.enginename = 'Mv1'
			String edgeQuery = "SELECT er.sourceconceptid, er.targetconceptid "
					+ "from enginerelation er "
					+ "where er.engine='" + engineId + "'";

			if(stmt == null) {
				stmt = conn.createStatement();
			}
			rs = stmt.executeQuery(edgeQuery);

			while(rs.next())
			{
				String startId = rs.getString(1);
				String endId = rs.getString(2);
				
				Hashtable newEdge = new Hashtable();
				// need to check to see if the idHash has it else put it in
				String sourceName = idHash.get(startId);
				String targetName = idHash.get(endId);
				newEdge.put("source", sourceName);
				newEdge.put("target", targetName);
				
				//if(nodeHash.containsKey(toId))
				
				boolean foundNode = true;
				if(!nodeHash.containsKey(sourceName)) {
					foundNode = false;
					System.out.println("Unable to find node " + sourceName);
				}
				if(!nodeHash.containsKey(targetName)) {
					foundNode = false;
					System.out.println("Unable to find node " + targetName);
				}
				
				if(foundNode) {
					edgeHash.put(startId + "-" + endId, newEdge);
				}
			}
		} catch(SQLException ex) {
			ex.printStackTrace();
		} finally {
			closeStreams(stmt, rs);
		}
		
		finalHash.put("nodes", nodeHash.values().toArray());
		finalHash.put("edges", edgeHash.values().toArray());
		if(includeDataTypes) {
			finalHash.put("physicalTypes", physicalDataTypes);
			finalHash.put("dataTypes", dataTypes);
			finalHash.put("additionalDataTypes", additionalDataTypes);
		}
		return finalHash;
	}
	
	/**
	 * specific format for xray merging db.tablename for nodes
	 * 
	 * @param engineName
	 * @return
	 */
	public static Map<String, Object> getXrayExisitingMetamodelRDBMS(String engineName) {
		// this needs to be moved to the name server
		// and this needs to be based on local master database
		// need this to be a simple OWL data
		// I dont know if it is worth it to load the engine at this point ?
		// or should I just load it ?
		// need to get local master and pump out the metamodel

		// need to get all the concepts first
		// get the edges next

		RDBMSNativeEngine engine = (RDBMSNativeEngine) Utility.getEngine(Constants.LOCAL_MASTER_DB_NAME);
		Connection conn = engine.makeConnection();
		Statement stmt = null;
		ResultSet rs = null;
		// creates e-c-p node names for fe to parse
		String delim = "-";
		Map<String, Object> finalHash = new Hashtable<String, Object>();

		// idHash - physical ID to the name of the node
		Hashtable<String, String> idHash = new Hashtable<String, String>();
		Hashtable<String, MetamodelVertex> nodeHash = new Hashtable<String, MetamodelVertex>();

		try {
			String nodeQuery = "select c.conceptualname, ec.physicalname, ec.localconceptid, ec.physicalnameid, ec.parentphysicalid, ec.property from "
					+ "engineconcept ec, concept c, engine e " + "where ec.engine=e.id " + "and e.enginename='"
					+ engineName + "' " + "and c.localconceptid=ec.localconceptid order by ec.property";
			stmt = conn.createStatement();
			rs = stmt.executeQuery(nodeQuery);
			while (rs.next()) {
				String conceptualName = rs.getString(1);
				String physicalName = rs.getString(2);
				String physicalId = rs.getString(4);
				String parentPhysicalId = rs.getString(5);

				// sets the physical id to conceptual name
				idHash.put(physicalId, conceptualName);

				MetamodelVertex node = null;

				// gets the conceptual name
				String conceptName = idHash.get(physicalId);

				// because it is ordered by property, this would already be
				// there
				String parentName = idHash.get(parentPhysicalId);

				// if already there, should we still add it ?
				if (nodeHash.containsKey(engineName + delim + parentName))
					node = nodeHash.get(engineName + delim + parentName);
				else {
					node = new MetamodelVertex(engineName + delim + parentName);
					nodeHash.put(engineName + delim + conceptualName, node);
				}

//				if (!conceptName.equalsIgnoreCase(parentName)) {
					// might be this or might not be
					// node.addProperty(engineName+ "." +conceptName);
					node.addProperty(conceptName);
//				}
			}
		} catch (SQLException ex) {
			ex.printStackTrace();
		} finally {
			// do not close the stmt
			// reuse it below
			closeStreams(null, rs);
		}

		try {
			// get the edges next
			// SELECT er.sourceconceptid, er.targetconceptid FROM ENGINERELATION
			// er, engine e where e.id = er.engine and e.enginename = 'Mv1'
			String edgeQuery = "SELECT er.sourceconceptid, er.targetconceptid FROM ENGINERELATION er, engine e where e.id = er.engine and "
					+ "e.enginename = '" + engineName + "'";

			if (stmt == null) {
				stmt = conn.createStatement();
			}
			rs = stmt.executeQuery(edgeQuery);

			Hashtable<String, Hashtable> edgeHash = new Hashtable<String, Hashtable>();
			while (rs.next()) {
				String startId = rs.getString(1);
				String endId = rs.getString(2);

				Hashtable newEdge = new Hashtable();
				// need to check to see if the idHash has it else put it in
				String sourceName = idHash.get(startId);
				String targetName = idHash.get(endId);
				newEdge.put("source", engineName + delim + sourceName + delim + sourceName);
				newEdge.put("target", engineName + delim + targetName + delim + targetName);

				// if(nodeHash.containsKey(toId))

				boolean foundNode = true;
				if (!nodeHash.containsKey(engineName + delim + sourceName)) {
					foundNode = false;
					System.out.println("Unable to find node " + sourceName);
				}
				if (!nodeHash.containsKey(engineName + delim + targetName)) {
					foundNode = false;
					System.out.println("Unable to find node " + targetName);
				}

				if (foundNode) {
					edgeHash.put(engineName + delim + sourceName + delim + sourceName + delim + engineName + delim+ targetName + delim + targetName, newEdge);
				}
			}
			finalHash.put("nodes", nodeHash);
			finalHash.put("edges", edgeHash);

		} catch (SQLException ex) {
			ex.printStackTrace();
		} finally {
			closeStreams(stmt, rs);
		}

		return finalHash;
	}
	
	/**
	 * Get the properties for a given concept for a specific database
	 * @param conceptName
	 * @param engineId
	 * @return
	 */
	public static Map<String, List<String>>  getConceptProperties(List<String> conceptLogicalNames, String engineFilter) {
		// get the bindings based on the input list
		String conceptString = makeListToString(conceptLogicalNames);
		String engineString = " and e.id= '" + engineFilter +"' ";
		if(engineFilter == null || engineFilter.isEmpty()) {
			engineString = "";
		}
		
		String propQuery = "select distinct e.enginename, c.conceptualname, ec.physicalname, ec.parentphysicalid, ec.physicalnameid, ec.property "
				+ "from engineconcept ec, concept c, engine e where ec.parentphysicalid in "
				+ "(select physicalnameid from engineconcept ec where localconceptid in (select localconceptid from concept where conceptualname in" +  conceptString.toString() + ") )" 
				+ engineString
				+ " and ec.engine=e.id and c.localconceptid=ec.localconceptid order by ec.property";
	
		Map<String, List<String>> queryData = new HashMap<>();
		RDBMSNativeEngine engine = (RDBMSNativeEngine) Utility.getEngine(Constants.LOCAL_MASTER_DB_NAME);
		Connection conn = engine.makeConnection();
		Statement stmt = null;
		ResultSet rs = null;
		
		try {
			stmt = conn.createStatement();
			rs = stmt.executeQuery(propQuery);
			// keeps the id to the concept name
			Hashtable <String, String> parentHash = new Hashtable<String, String>();
			while(rs.next()) {
				String propName = rs.getString(2);
				String parentId = rs.getString(4);
				String propId = rs.getString(5);
				if(parentId.equalsIgnoreCase(propId)) {
					parentHash.put(parentId, propName);
				}
				
				String parentName = parentHash.get(parentId);
				if(!propName.equalsIgnoreCase(parentName)) {

					List<String> propList = null;
					if(queryData.containsKey(parentName)) {
						propList = queryData.get(parentName);
					} else {
						propList = new ArrayList<>();
					}
					propList.add(propName);
					// add to the engine map
					queryData.put(parentName, propList);
				}
			}
		} catch (SQLException e) {
			e.printStackTrace();
		} finally {
			closeStreams(stmt, rs);
		}
		return queryData;
	}
	
	/**
	 * Get the properties for a given concept across all the databases
	 * @param conceptName
	 * @param engineId		optional filter for the properties
	 * @return
	 */
	public static Map<String, Object[]> getConceptPropertiesRDBMS(List<String> conceptLogicalNames, List<String> engineFilter) {
		// get the bindings based on the input list
		String conceptString = makeListToString(conceptLogicalNames);
		String engineString = " and ec.engine in " + makeListToString(engineFilter) +" ";
		if(engineFilter == null || engineFilter.isEmpty()) {
			engineString = "";
		}
		
		String propQuery = "select distinct ec.engine, c.conceptualname, ec.physicalname, ec.parentphysicalid, ec.physicalnameid, ec.property "
				+ "from engineconcept ec, concept c "
				+ "where ec.parentphysicalid in "
				+ "(select physicalnameid from engineconcept ec where localconceptid in (select localconceptid from concept where conceptualname in" +  conceptString.toString() + ") )" 
				+ engineString
				+ " and c.localconceptid=ec.localconceptid order by ec.property";
	
		Map<String, Object[]> returnHash = new TreeMap<String, Object[]>();;
		Map<String, Map<String, MetamodelVertex>> queryData = new TreeMap<String, Map<String, MetamodelVertex>>();

		RDBMSNativeEngine engine = (RDBMSNativeEngine) Utility.getEngine(Constants.LOCAL_MASTER_DB_NAME);
		Connection conn = engine.makeConnection();
		Statement stmt = null;
		ResultSet rs = null;
		
		try {
			stmt = conn.createStatement();
			rs = stmt.executeQuery(propQuery);
			// keeps the id to the concept name
			Hashtable <String, String> parentHash = new Hashtable<String, String>();

			while(rs.next()) {
				String engineName = rs.getString(1);
				String propName = rs.getString(2);
				String parentId = rs.getString(4);
				String propId = rs.getString(5);

				if(parentId.equalsIgnoreCase(propId)) {
					parentHash.put(parentId, propName);
				}
				
				String parentName = parentHash.get(parentId);
				if(!propName.equalsIgnoreCase(parentName)) {
					Map<String, MetamodelVertex> engineMap = null;
					if(queryData.containsKey(engineName)) {
						engineMap = queryData.get(engineName);
					} else {
						engineMap = new TreeMap<String, MetamodelVertex>();
						// add to query data map
						queryData.put(engineName, engineMap);
					}

					// get or create the vertex
					MetamodelVertex vert = null;
					if(engineMap.containsKey(parentName)) {
						vert = engineMap.get(parentName);
					} else {
						vert = new MetamodelVertex(parentName);
						// add to the engine map
						engineMap.put(parentName, vert);
					}

					// add the property conceptual name
					vert.addProperty(propName);
				}
			}
		} catch (SQLException e) {
			e.printStackTrace();
		} finally {
			closeStreams(stmt, rs);
		}
		
		
		for(String engineName : queryData.keySet()) {
			returnHash.put(engineName, queryData.get(engineName).values().toArray());
		}

		return returnHash;
	}
	
	private static String makeListToString(Collection<String> filterList) {
		StringBuilder conceptString = new StringBuilder("(");
		if(filterList != null && !filterList.isEmpty()) {
			Iterator<String> iterator = filterList.iterator();
			if(iterator.hasNext()) {
				conceptString.append("'" + iterator.next() + "'");
			}
			while(iterator.hasNext()) {
				conceptString.append(", '" + iterator.next() + "'");
			}
		}
		conceptString.append(")");
		return conceptString.toString();
	}

	/**
	 * Get the list of  connected concepts for a given concept
	 * 
	 * Direction upstream/downstream is always in reference to the node being searched
	 * For example, if the relationship in the direction Title -> Genre
	 * The result would be { upstream -> [Genre] } because Title is upstream of Genre
	 * 
	 * @param conceptType
	 * @return
	 */
	public static Map getConnectedConceptsRDBMS(List<String> conceptLogicalNames, List<String> engineFilters) {
		// get the bindings based on the input list
		String conceptString = makeListToString(conceptLogicalNames);
		String engineString = makeListToString(engineFilters);
		
		RDBMSNativeEngine engine = (RDBMSNativeEngine) Utility.getEngine(Constants.LOCAL_MASTER_DB_NAME);
		Connection conn = engine.makeConnection();
		Statement stmt = null;
		ResultSet rs = null;
		
		// id to concept
		Hashtable <String, String> idToName = new Hashtable <String, String>();
		
		// this is the final return object
		// engine > concept > downstream > items
		// retMap > conceptSpecific > stream
		Map<String, Map> retMap = new TreeMap<String, Map>();


		// I technically need to do 3 queries
		// first one is get the localconceptid / physicalids for all of these
		// second is the upstream
		// third is the downstream
		//select e.enginename, ec.engine, c.logicalname, ec.physicalnameid from concept c, engineconcept ec, engine e where c.logicalname in ('Title') and c.localconceptid=ec.localconceptid and e.id = ec.engine

		String conceptMasterQuery = "select ec.engine, c.conceptualname, ec.physicalnameid, ec.physicalname "
				+ "from concept c, engineconcept ec "
				+ "where c.logicalname in " + conceptString
				+ (engineFilters != null ? (" and ec.engine in " + engineString) + " " : "")
				+ "and c.localconceptid=ec.localconceptid";
		try {
			stmt = conn.createStatement();
			rs = stmt.executeQuery(conceptMasterQuery);
			while(rs.next()) {
				String engineId = rs.getString(1);
				String conceptualName = rs.getString(2);
				String physicalNameId = rs.getString(3);
				String equivalentConcept = rs.getString(4);

				// put the id for future reference
				// no reason why we cannot cache but.. 
				idToName.put(physicalNameId, conceptualName);

				Map <String, Object> conceptSpecific = null;
				if(retMap.containsKey(engineId)) {
					conceptSpecific = retMap.get(engineId);
				} else {
					conceptSpecific = new TreeMap<String, Object>();
				}
				retMap.put(engineId, conceptSpecific);

				Hashtable <String, String> stream = new Hashtable<String, String>();
				stream.put("equivalentConcept", equivalentConcept);

				conceptSpecific.put(conceptualName, stream);
				retMap.put(engineId, conceptSpecific);
			}
		} catch(SQLException e) {
			e.printStackTrace();
		} finally {
			// do not close the stmt
			// reuse it below
			closeStreams(null, rs);
		}


		//select distinct  e.enginename, 'Title', 'downstream' as downstream,  er.relationname,  c.logicalname , er.engine, er.targetconceptid, ec.physicalname from enginerelation er, engineconcept ec, concept c, engine e where er.sourceconceptid in (select physicalnameid from engineconcept where localconceptid in (select localconceptid from concept where logicalname in ('Title'))) 
		//and ec.physicalnameid=er.targetconceptid and c.localconceptid=ec.localconceptid and e.id=er.engine;

		// now time to run the upstream and downstream queries
		String downstreamQuery = "select distinct ec.engine, er.sourceconceptid, 'upstream' as upstream, "
				+ "er.relationname, c.conceptualname , er.engine, er.targetconceptid, ec.physicalname "
				+ "from enginerelation er, engineconcept ec, concept c "
				+ "where "
				+ (engineFilters != null ? (" ec.engine in " + engineString + " and ") : "")
				+ "er.sourceconceptid in (select physicalnameid from engineconcept where localconceptid in "
				+ "(select localconceptid from concept where logicalname in " + conceptString + ")) "
				+ "and ec.physicalnameid=er.targetconceptid and c.localconceptid=ec.localconceptid;";

		try {
			if(stmt == null) {
				stmt = conn.createStatement();
			}
			rs = stmt.executeQuery(downstreamQuery);
			while(rs.next()) {
				String engineId = rs.getString(1);
				String coreConceptId = rs.getString(2);
				String relationName = rs.getString(4);
				String streamConceptName = rs.getString(5);
				String streamPhysicalName = rs.getString(8);

				// this is the main concept
				String coreConceptName = idToName.get(coreConceptId);

				Map <String, Map> engineSpecific = retMap.get(engineId);
				Map <String, Object> conceptSpecific = engineSpecific.get(coreConceptName);

				Set<String> downstreams = new TreeSet<String>();
				Set<String> physicalNames = new TreeSet<String>();

				if(conceptSpecific.containsKey("upstream"))
					downstreams = (Set<String>)conceptSpecific.get("upstream");
				downstreams.add(streamConceptName);
				if(conceptSpecific.containsKey("physical"))
					physicalNames = (Set<String>)conceptSpecific.get("physical");
				physicalNames.add(streamPhysicalName);
				conceptSpecific.put("upstream", downstreams);
				conceptSpecific.put("physical", physicalNames);
				engineSpecific.put(coreConceptName, conceptSpecific);
				retMap.put(engineId, engineSpecific);
			}
		} catch(SQLException e) {
			e.printStackTrace();
		} finally {
			closeStreams(null, rs);
		}

		// now time to run the upstream and downstream queries
		String upstreamQuery = "select distinct ec.engine, er.targetconceptid, 'downstream' as downstream,  "
				+ "er.relationname,  c.conceptualname , er.engine, er.sourceconceptid, ec.physicalname "
				+ "from enginerelation er, engineconcept ec, concept c "
				+ "where "
				+ (engineFilters != null ? (" ec.engine in " + engineString + " and ") : "")
				+ "er.targetconceptid in (select physicalnameid from engineconcept where localconceptid in "
				+ "(select localconceptid from concept where logicalname in " + conceptString + ")) "
				+ "and ec.physicalnameid=er.sourceconceptid and c.localconceptid=ec.localconceptid";
		try {
			if(stmt == null) {
				stmt = conn.createStatement();
			}
			rs = stmt.executeQuery(upstreamQuery);
			while(rs.next())
			{
				String engineName = rs.getString(1);
				String coreConceptId = rs.getString(2);
				String relationName = rs.getString(4);
				String streamConceptName = rs.getString(5);
				String streamPhysicalName = rs.getString(8);

				String coreConceptName = idToName.get(coreConceptId);

				Map <String, Map> engineSpecific = retMap.get(engineName);
				Map <String, Object> conceptSpecific = engineSpecific.get(coreConceptName);

				Set<String> upstreams = new TreeSet<String>();
				Set<String> physicalNames = new TreeSet<String>();

				if(conceptSpecific.containsKey("downstream"))
					upstreams = (Set<String>)conceptSpecific.get("downstream");
				upstreams.add(streamConceptName);
				if(conceptSpecific.containsKey("physical"))
					physicalNames = (Set<String>)conceptSpecific.get("physical");
				physicalNames.add(streamPhysicalName);
				conceptSpecific.put("downstream", upstreams);
				conceptSpecific.put("physical", physicalNames);
				engineSpecific.put(coreConceptName, conceptSpecific);
				retMap.put(engineName, engineSpecific);
			}
		} catch(SQLException ex) {
			ex.printStackTrace();
		} finally {
			closeStreams(stmt, rs);
		}
		return retMap;
	}
	
	/**
	 * Get the list of unique engine ids
	 * @return
	 */
	public static List<String> getAllEngineIds() {
		RDBMSNativeEngine engine = (RDBMSNativeEngine) Utility.getEngine(Constants.LOCAL_MASTER_DB_NAME);
		Connection conn = engine.makeConnection();
		Statement stmt = null;
		ResultSet rs = null;
		
		List <String> retList = new ArrayList<String>();
		if(conn != null) {
			try {
				String conceptQuery = "select distinct e.id from engine e"; 
				stmt = conn.createStatement();
				rs = stmt.executeQuery(conceptQuery);
				while(rs.next()) {
					String engineName = rs.getString(1);
					retList.add(engineName);
				}		
			} catch(SQLException ex) {
				ex.printStackTrace();
			} finally {
				closeStreams(stmt, rs);
			}
		}
		return retList;
	}
	
	/**
	 * Get an engine alias for an id
	 * @return
	 */
	public static String getEngineAliasForId(String id) {
		RDBMSNativeEngine engine = (RDBMSNativeEngine) Utility.getEngine(Constants.LOCAL_MASTER_DB_NAME);
		Connection conn = engine.makeConnection();
		Statement stmt = null;
		ResultSet rs = null;
		
		if(conn != null) {
			try {
				String query = "select e.enginename from engine e where e.id='" + id + "'";
				stmt = conn.createStatement();
				rs = stmt.executeQuery(query);
				while(rs.next()) {
					String engineName = rs.getString(1);
					return engineName;
				}		
			} catch(SQLException ex) {
				ex.printStackTrace();
			} finally {
				closeStreams(stmt, rs);
			}
		}
		
		return null;
	}

	/**
	 * Get the list of concepts for a given engine
	 * @param engineId
	 * @return
	 */
	public static Set<String> getConceptsWithinEngineRDBMS(String engineId) {
		/*
		 * Grab the local master engine and query for the concepts
		 * We do not want to load the engine until the user actually wants to use it
		 * 
		 */
		//select distinct c.logicalname, ec.physicalname from concept c, engineconcept ec, engine e where ec.localconceptid=c.localconceptid and e.id=ec.engine and e.enginename = 'actor';
		// select distinct c.logicalname, ec.physicalname from concept c, engineconcept ec, engine e where ec.localconceptid=c.localconceptid and e.id=ec.engine and ec.property=false and e.enginename = 'actor';
		
		RDBMSNativeEngine engine = (RDBMSNativeEngine) Utility.getEngine(Constants.LOCAL_MASTER_DB_NAME);
		Connection conn = engine.makeConnection();
		Statement stmt = null;
		ResultSet rs = null;
		Set<String> conceptsList = new TreeSet<String>();
		try {
			String query = "select distinct c.conceptualname "
					+ "from concept c, engineconcept ec "
					+ "where ec.localconceptid=c.localconceptid and ec.property=false and ec.engine ='" + engineId + "'";
			
			stmt = conn.createStatement();
			rs = stmt.executeQuery(query);
			while(rs.next()) {
				String logName = rs.getString(1);
				conceptsList.add(logName);
			}
		} catch(SQLException ex) {
			ex.printStackTrace();
		} finally {
			closeStreams(stmt, rs);
		}
		return conceptsList;
	}
	
	/**
	 * Execute a query to get the table name for a column
	 * @param engineId
	 * @param column
	 * @return
	 */
	public static String getTableForColumn(String engineId, String column) {
		// select ec.physicalname from engineconcept as ec inner join engineconcept ec2 on ec.physicalnameid=ec2.parentphysicalid where ec.engine='acd7bdc8-67a0-4fa7-8b30-7c39f5c0fc62' and ec2.physicalname='MOVIE_DATA' and ec2.pk = false;
		String query = "select ec.physicalname "
				+ "from engineconcept as ec "
				+ "inner join engineconcept ec2 on ec.physicalnameid=ec2.parentphysicalid "
				+ "where ec2.pk = false "
				+ "and ec.engine='" + engineId + "' "
				+ "and ec2.physicalname='" + column + "'";
		
		RDBMSNativeEngine engine = (RDBMSNativeEngine) Utility.getEngine(Constants.LOCAL_MASTER_DB_NAME);
		Connection conn = engine.makeConnection();
		Statement stmt = null;
		ResultSet rs = null;
		try {
			stmt = conn.createStatement();
			rs = stmt.executeQuery(query);
			while(rs.next()) {
				String parentName = rs.getString(1);
				return parentName;
			}
		} catch(SQLException ex) {
			ex.printStackTrace();
		} finally {
			closeStreams(stmt, rs);
		}
		
		return null;
	}
	
	public static String getBasicDataType(String engineId, String conceptualName, String parentConceptualName) {
		String query = null;
		if(parentConceptualName == null || parentConceptualName.isEmpty()) {
			query = "select distinct ec.property_type "
					+ "from engineconcept ec "
					+ "inner join concept c on ec.localconceptid = c.localconceptid "
					+ "where ec.engine = '" + engineId + "' and c.conceptualname = '" + conceptualName + "'";
		} else {
			query = "select distinct ec.property_type "
					+ "from engine e "
					+ "inner join engineconcept ec on e.id = ec.engine "
					+ "inner join concept c on ec.localconceptid = c.localconceptid "
					+ "where ec.engine = '" + engineId + "' and "
					+ "c.conceptualname = '" + conceptualName + "' and "
					+ "ec.parentphysicalid in "
					+ "(select distinct ec.physicalnameid "
					+ "from engineconcept ec "
					+ "inner join concept c on c.localconceptid = ec.localconceptid "
					+ "where ec.engine = '" + engineId + "' and "
					+ "c.conceptualname = '" + parentConceptualName + "')";
		}
		
		RDBMSNativeEngine engine = (RDBMSNativeEngine) Utility.getEngine(Constants.LOCAL_MASTER_DB_NAME);
		Connection conn = engine.makeConnection();
		Statement stmt = null;
		ResultSet rs = null;
		
		try {
			stmt = conn.createStatement();
			rs = stmt.executeQuery(query);
			while(rs.next())
			{
				String dataType = rs.getString(1);
				return dataType;
			}
		} catch (SQLException e) {
			e.printStackTrace();
		} finally {
			closeStreams(stmt, rs);
		}
		
		return null;
	}
	
	public static String getAdditionalDataType(String engineId, String conceptualName, String parentConceptualName) {
		String query = null;
		if(parentConceptualName == null || parentConceptualName.isEmpty()) {
			query = "select distinct ec.additional_type "
					+ "from engineconcept ec "
					+ "inner join concept c on ec.localconceptid = c.localconceptid "
					+ "where ec.engine = '" + engineId + "' and c.conceptualname = '" + conceptualName + "'";
		} else {
			query = "select distinct ec.additional_type "
					+ "from engine e "
					+ "inner join engineconcept ec on e.id = ec.engine "
					+ "inner join concept c on ec.localconceptid = c.localconceptid "
					+ "where ec.engine = '" + engineId + "' and "
					+ "c.conceptualname = '" + conceptualName + "' and "
					+ "ec.parentphysicalid in "
					+ "(select distinct ec.physicalnameid "
					+ "from engineconcept ec "
					+ "inner join concept c on c.localconceptid = ec.localconceptid "
					+ "where ec.engine = '" + engineId + "' and "
					+ "c.conceptualname = '" + parentConceptualName + "')";
		}
		
		RDBMSNativeEngine engine = (RDBMSNativeEngine) Utility.getEngine(Constants.LOCAL_MASTER_DB_NAME);
		Connection conn = engine.makeConnection();
		Statement stmt = null;
		ResultSet rs = null;
		
		try {
			stmt = conn.createStatement();
			rs = stmt.executeQuery(query);
			while(rs.next())
			{
				String adtlType = rs.getString(1);
				return adtlType;
			}
		} catch (SQLException e) {
			e.printStackTrace();
		} finally {
			closeStreams(stmt, rs);
		}
		
		return null;
	}
	
	/**
	 * Adds logical name to concept from engine
	 * 
	 * @param engineId
	 * @param concept
	 * @param logicalName
	 * @return
	 */
	public static boolean addLogicalName(String engineId, String concept, String logicalName) {
		RDBMSNativeEngine engine = (RDBMSNativeEngine) Utility.getEngine(Constants.LOCAL_MASTER_DB_NAME);
		Connection masterConn = engine.makeConnection();
		Statement stmt = null;
		ResultSet rs = null;
		int size = 0;
		try {
			String duplicateQueryCheck = "select localconceptid, conceptualname, logicalname, "
					+ "domainname, globalid from concept "
					+ "where localconceptid in (select localconceptid from engineconcept "
					+ "where engine='" + engineId + "') "
					+ "and conceptualname='" + concept + "' and logicalname='" + logicalName + "';";
			stmt = masterConn.createStatement();
			rs = stmt.executeQuery(duplicateQueryCheck);
			if (rs != null) {
				rs.beforeFirst();
				rs.last();
				size = rs.getRow();
			}
		} catch (SQLException e) {
			e.printStackTrace();
		} finally {
			closeStreams(stmt, rs);
		}
		
		try {
			if (size == 0) {
				String sourceLogicalInfo = "select localconceptid, conceptualname, logicalname, "
						+ "domainname, globalid from concept "
						+ "where localconceptid in (select localconceptid from engineconcept "
						+ "where engine='" + engineId + "') "
						+ "and conceptualname='" + concept + "'";
				if (stmt == null || stmt.isClosed()) {
					stmt = masterConn.createStatement();
				}
				rs = stmt.executeQuery(sourceLogicalInfo);
				while (rs.next()) {
					String localConceptID = rs.getString(1);
					String conceptualName = rs.getString(2);
					String oldLogicalName = rs.getString(3);
					String domainName = rs.getString(4);
					String globalID = rs.getString(5);
					if (conceptualName.equals(concept)) {
						// insert target CN as logical name
						String insertString = "insert into concept " + "values('" + localConceptID + "', '"
								+ conceptualName + "', '" + logicalName + "\', \'" + domainName + "', '"
								+ globalID.toString() + "');";
						int validInsert = masterConn.createStatement().executeUpdate(insertString);
						if (validInsert > 0) {
							try {
								engine.commitRDBMS();
								return true;
							} catch (Exception e) {
								e.printStackTrace();
							}
						}
					}
				}
			} else {
				return true;
			}
		} catch (SQLException e) {
			e.printStackTrace();
		} finally {
			closeStreams(stmt, rs);
		}
		return false;
	}

	/**
	 * Removes logical name for a concept from an engine
	 * 
	 * @param engineName
	 * @param concept
	 * @param logicalName
	 * @return success
	 */
	public static boolean removeLogicalName(String engineName, String concept, String logicalName) {
		RDBMSNativeEngine engine = (RDBMSNativeEngine) Utility.getEngine(Constants.LOCAL_MASTER_DB_NAME);
		Connection masterConn = engine.makeConnection();
		Statement stmt = null;
		
		try {
			String deleteQuery = "delete from concept "
					+ "where localconceptid in (select localconceptid from engineconcept "
					+ "where engine='" + engineName + "')"
					+ "and conceptualname='" + concept + "' and logicalname='" + logicalName + "'";
			stmt = masterConn.createStatement();
			int updateCount = stmt.executeUpdate(deleteQuery);
			if (updateCount == 1) {
				return true;
			}
		} catch (SQLException e) {
			e.printStackTrace();
		} finally {
			closeStreams(stmt, null);
		}
		return false;
	}

	/**
	 * Get logical names for a specific engine and concept
	 * 
	 * @param engineId
	 * @param concept
	 * @return logicalNames
	 */
	public static List<String> getLogicalNames(String engineId, String concept) {
		List<String> logicalNames = new ArrayList<String>();
		
		RDBMSNativeEngine engine = (RDBMSNativeEngine) Utility.getEngine(Constants.LOCAL_MASTER_DB_NAME);
		Connection masterConn = engine.makeConnection();
		Statement stmt = null;
		ResultSet rs = null;

		try {
			String query = "select logicalname from concept "
					+ "where localconceptid in (select localconceptid from engineconcept "
					+ "where engine='" + engineId + "')"
					+ "and conceptualname='" + concept + "'";
			
			stmt = masterConn.createStatement();
			rs = stmt.executeQuery(query);
			while (rs.next()) {
				String logicalName = rs.getString(1);
				logicalNames.add(logicalName);
			}
		} catch (SQLException e) {
			e.printStackTrace();
		} finally {
			closeStreams(stmt, rs);
		}
		return logicalNames;
	}
	
	public static Map<String, List<String>> getEngineLogicalNames(String engineId) {
		// select ec2.physicalname as parentPhysical, ec.physicalname as physicalname, c.conceptualname as conceptualname, c.logicalname as logicalname, ec.pk as isPrim from engineconcept ec inner join engineconcept ec2 on ec.parentphysicalid=ec2.physicalnameid inner join concept c on ec.localconceptid=c.localconceptid where ec.engine='89850ba1-bafe-45a5-84ef-df8e21669267' order by isprim desc;		
		
		Map<String, List<String>> engineLogicalNames = new HashMap<String, List<String>>();
		Map<String, String> parentPhysicalToConceptual = getParentPhysicalToConceptual(engineId);

		RDBMSNativeEngine engine = (RDBMSNativeEngine) Utility.getEngine(Constants.LOCAL_MASTER_DB_NAME);
		Connection masterConn = engine.makeConnection();
		Statement stmt = null;
		ResultSet rs = null;

		try {
			String query = "select distinct ec2.physicalname as parentPhysical, "
					+ "c.conceptualname as conceptualname, "
					+ "cmd.value as logicalname, "
					+ "ec.pk as isPrim "
					+ "from engineconcept ec "
					+ "inner join engineconcept ec2 on ec.parentphysicalid=ec2.physicalnameid "
					+ "inner join concept c on ec.localconceptid=c.localconceptid "
					+ "inner join conceptmetadata cmd on ec.physicalnameid=cmd.physicalnameid "
					+ "where ec.engine='"+ engineId + "' and cmd.key='logical'";
			
			stmt = masterConn.createStatement();
			rs = stmt.executeQuery(query);
			while (rs.next()) {
				String parentPhysical = rs.getString(1);
				String conceptualName = rs.getString(2);
				String logicalName = rs.getString(3);
				boolean isPk = rs.getBoolean(4);
				
				String uniqueName = conceptualName;
				if(!isPk) {
					uniqueName = parentPhysicalToConceptual.get(parentPhysical) + "__" + conceptualName;
				}
				
				List<String> logicalNames = null;
				if(engineLogicalNames.containsKey(uniqueName)) {
					logicalNames = engineLogicalNames.get(uniqueName);
				} else {
					logicalNames = new Vector<String>();
					// store in the map
					engineLogicalNames.put(uniqueName, logicalNames);
				}
				// add the new value
				logicalNames.add(logicalName);
			}
		} catch (SQLException e) {
			e.printStackTrace();
		} finally {
			closeStreams(stmt, rs);
		}
		
		return engineLogicalNames;
	}
	
	public static Map<String, String> getEngineDescriptions(String engineId) {
		// select ec2.physicalname as parentPhysical, ec.localconceptid, ec.physicalname as physicalname, c.conceptualname as conceptualname, cmd.value as description, ec.pk as isPrim from engineconcept ec inner join engineconcept ec2 on ec.parentphysicalid=ec2.physicalnameid inner join concept c on ec.localconceptid=c.localconceptid inner join conceptmetadata cmd on ec.localconceptid=cmd.localconceptid where ec.engine='89850ba1-bafe-45a5-84ef-df8e21669267' order by isprim desc;
		
		Map<String, String> engineDescriptions = new HashMap<String, String>();
		Map<String, String> parentPhysicalToConceptual = getParentPhysicalToConceptual(engineId);

		RDBMSNativeEngine engine = (RDBMSNativeEngine) Utility.getEngine(Constants.LOCAL_MASTER_DB_NAME);
		Connection masterConn = engine.makeConnection();
		Statement stmt = null;
		ResultSet rs = null;

		try {
			String query = "select distinct ec2.physicalname as parentPhysical, "
					+ "c.conceptualname as conceptualname, "
					+ "cmd.value as description, "
					+ "ec.pk as isPrim "
					+ "from engineconcept ec "
					+ "inner join engineconcept ec2 on ec.parentphysicalid=ec2.physicalnameid "
					+ "inner join concept c on ec.localconceptid=c.localconceptid "
					+ "inner join conceptmetadata cmd on ec.physicalnameid=cmd.physicalnameid "
					+ "where ec.engine='"+ engineId + "' and cmd.key='description'";
			
			stmt = masterConn.createStatement();
			rs = stmt.executeQuery(query);
			while (rs.next()) {
				String parentPhysical = rs.getString(1);
				String conceptualName = rs.getString(2);
				String description = rs.getString(3);
				boolean isPk = rs.getBoolean(4);

				String uniqueName = conceptualName;
				if(!isPk) {
					uniqueName = parentPhysicalToConceptual.get(parentPhysical) + "__" + conceptualName;
				}
				
				engineDescriptions.put(uniqueName, description);
			}
		} catch (SQLException e) {
			e.printStackTrace();
		} finally {
			closeStreams(stmt, rs);
		}
		
		return engineDescriptions;
	}
	
	private static Map<String, String> getParentPhysicalToConceptual(String engineId) {
		// select distinct ec.physicalname, c.conceptualname from engineconcept ec inner join concept c on ec.localconceptid=c.localconceptid where ec.pk=true and ec.engine='ffa65a5d-f8e1-4d66-a1da-2d87a27b343b';
		
		Map<String, String> parentPhysicalToConceptual = new HashMap<String, String>();
		
		RDBMSNativeEngine engine = (RDBMSNativeEngine) Utility.getEngine(Constants.LOCAL_MASTER_DB_NAME);
		Connection masterConn = engine.makeConnection();
		Statement stmt = null;
		ResultSet rs = null;

		try {
			String query = "select distinct ec.physicalname, c.conceptualname "
					+ "from engineconcept ec "
					+ "inner join concept c on ec.localconceptid=c.localconceptid "
					+ "where ec.pk=true and ec.engine='" + engineId + "';";
			
			stmt = masterConn.createStatement();
			rs = stmt.executeQuery(query);
			while (rs.next()) {
				String physical = rs.getString(1);
				String conceptual = rs.getString(2);
				parentPhysicalToConceptual.put(physical, conceptual);
			}
		} catch (SQLException e) {
			e.printStackTrace();
		} finally {
			closeStreams(stmt, rs);
		}
		
		return parentPhysicalToConceptual;
	}
	
	/**
	 * Get the properties for a given concept
	 * @param conceptName
	 * @param engineId		the engine to get the properties for
	 * @return
	 */
	public static List<String> getSpecificConceptPropertiesRDBMS(String conceptString, String engineId) {
		// get the bindings based on the input list
		if(engineId == null || engineId.isEmpty()) {
			throw new IllegalArgumentException("Must define engineName");
		}
		String engineString = "ec.engine='" + engineId +"' ";

		String propQuery = "select distinct ec.physicalname "
				+ "from engineconcept ec, concept c "
				+ "where " + engineString 
				+ " and c.localconceptid=ec.localconceptid "
				+ " and ec.parentphysicalid in (select physicalnameid from engineconcept ec "
				+ " where localconceptid in (select localconceptid from concept where conceptualname in ('" +  conceptString + "')) )"
				+ " order by ec.physicalname "; 
		
		List<String> properties = new Vector<String>();

		RDBMSNativeEngine engine = (RDBMSNativeEngine) Utility.getEngine(Constants.LOCAL_MASTER_DB_NAME);
		Connection conn = engine.makeConnection();
		Statement stmt = null;
		ResultSet rs = null;
		try {
			stmt = conn.createStatement();
			rs = stmt.executeQuery(propQuery);
			while(rs.next()) {
				properties.add(rs.getString(1));
			}
		} catch (SQLException e) {
			e.printStackTrace();
		} finally {
			closeStreams(stmt, rs);
		}
		
		return properties;
	}
	
	
	/**
	 * Returns Xray config files
	 * 
	 * @return
	 */
	public static HashMap<String, Object> getXrayConfigList() {
		RDBMSNativeEngine engine = (RDBMSNativeEngine) Utility.getEngine(Constants.LOCAL_MASTER_DB_NAME);
		Connection conn = engine.makeConnection();
		Statement stmt = null;
		ResultSet rs = null;
		HashMap<String, Object> configMap = new HashMap<String, Object>();
		try {
			String query = "select distinct filename FROM xrayconfigs;";
			stmt = conn.createStatement();
			rs = stmt.executeQuery(query);
			ArrayList<HashMap<String, Object>> configList = new ArrayList<>();
			while (rs.next()) {
				HashMap<String, Object> rsMap = new HashMap<>();
				String fileName = rs.getString(1);
				rsMap.put("fileName", fileName);
				configList.add(rsMap);
			}
			configMap.put("configList", configList);
		} catch (SQLException ex) {
			// Don't print stack trace... xrayConfigList table is missing if no config file exists
		} finally {
			closeStreams(stmt, rs);
		}
		return configMap;

	}

	/**
	 * Gets the xray config file
	 * 
	 * @param filename
	 * @return
	 */
	public static String getXrayConfigFile(String filename) {
		RDBMSNativeEngine engine = (RDBMSNativeEngine) Utility.getEngine(Constants.LOCAL_MASTER_DB_NAME);
		Connection conn = engine.makeConnection();
		Statement stmt = null;
		ResultSet rs = null;
		String configFile = "";
		try {
			String query = "select config from xrayconfigs where filename = \'" + filename + "\';";
			stmt = conn.createStatement();
			rs = stmt.executeQuery(query);
			while (rs.next()) {
				configFile = rs.getString(1);
			}
		} catch (Exception ex) {
			ex.printStackTrace();
		} finally {
			closeStreams(stmt, rs);
		}
		return configFile;
	}

	/**
	 * Get local concept id
	 * 
	 * @param engineId
	 * @param concept
	 * @return
	 */
	public static String getLocalConceptID(String engineId, String concept) {
		String localConceptID = null;
		RDBMSNativeEngine engine = (RDBMSNativeEngine) Utility.getEngine(Constants.LOCAL_MASTER_DB_NAME);
		Connection conn = engine.makeConnection();
		Statement stmt = null;
		ResultSet rs = null;
		try {
			String query = "select localconceptid from concept "
					+ "where localconceptid in (select localconceptid from engineconcept "
					+ "where engine='" + engineId + "') "
					+ "and conceptualname='" + concept + "';";
			stmt = conn.createStatement();
			rs = stmt.executeQuery(query);
			while (rs.next()) {
				localConceptID = rs.getString(1);
			}
		} catch (Exception ex) {
			ex.printStackTrace();
		} finally {
			closeStreams(stmt, rs);
		}
		if (localConceptID == null) {
			throw new IllegalArgumentException("Unable to get concept ID for " + engineId + " " + concept);
		}
		return localConceptID;
	}
	
	public static String getPhysicalConceptId(String engineId, String conceptualName) {
		// SELECT engineconcept.physicalnameid FROM engineconcept INNER JOIN concept ON concept.localconceptid=engineconcept.localconceptid WHERE engineconcept.engine='' AND concept.conceptualname='Title'
		String physicalNameId = null;
		RDBMSNativeEngine engine = (RDBMSNativeEngine) Utility.getEngine(Constants.LOCAL_MASTER_DB_NAME);
		Connection conn = engine.makeConnection();
		Statement stmt = null;
		ResultSet rs = null;
		try {
			String query = "SELECT engineconcept.physicalnameid "
					+ "FROM engineconcept "
					+ "INNER JOIN concept ON concept.localconceptid=engineconcept.localconceptid "
					+ "WHERE engineconcept.engine='" + engineId + "' "
					+ "AND concept.conceptualname='" + conceptualName + "';";
			stmt = conn.createStatement();
			rs = stmt.executeQuery(query);
			while (rs.next()) {
				physicalNameId = rs.getString(1);
			}
		} catch (Exception ex) {
			ex.printStackTrace();
		} finally {
			closeStreams(stmt, rs);
		}
		if (physicalNameId == null) {
			throw new IllegalArgumentException("Unable to get physical name id for " + engineId + " " + conceptualName);
		}
		return physicalNameId;
	}
	
	/**
	 * Get concept metadata value for a key
	 * 
	 * @param engineId
	 * @param concept
	 * @param key
	 * @return
	 */
	public static String getMetadataValue(String engineId, String concept, String key) {
		String value = null;
		RDBMSNativeEngine engine = (RDBMSNativeEngine) Utility.getEngine(Constants.LOCAL_MASTER_DB_NAME);
		Connection conn = engine.makeConnection();
		Statement stmt = null;
		ResultSet rs = null;
		try {
			String query = "select " + Constants.VALUE + " from " + Constants.CONCEPT_METADATA_TABLE
					+ " where localconceptid in (select localconceptid from concept "
					+ "where localconceptid in (select localconceptid from engineconcept "
					+ "where engine='" + engineId + "') "
					+ "and conceptualname='" + concept + "') and " + Constants.KEY + "='" + key + "';";
			stmt = conn.createStatement();
			rs = stmt.executeQuery(query);

			while (rs.next()) {
				value = rs.getString(1);
			}
		} catch (Exception ex) {
			 ex.printStackTrace();
		} finally {
			closeStreams(stmt, rs);
		}
		return value;
	}

	public static boolean deleteMetaValue(String engineName, String concept, String key) {
		boolean deleted = false;
		String localConceptID = MasterDatabaseUtility.getLocalConceptID(engineName, concept);
		RDBMSNativeEngine engine = (RDBMSNativeEngine) Utility.getEngine(Constants.LOCAL_MASTER_DB_NAME);
		Connection conn = engine.makeConnection();
		Statement stmt = null;
		int count = 0;
		try {
			String deleteQuery = "DELETE FROM " + Constants.CONCEPT_METADATA_TABLE + " WHERE "
					+ Constants.PHYSICAL_NAME_ID + " = \'" + localConceptID + "\' and " + Constants.KEY + " = \'" + key
					+ "\';";
			stmt = conn.createStatement();
			count = stmt.executeUpdate(deleteQuery);
			if (count > 0) {
				deleted = true;
			}
		} catch (Exception ex) {
			ex.printStackTrace();
		} finally {
			closeStreams(stmt, null);
		}

		return deleted;
	}

	public static boolean deleteMetaValue(String engineName, String concept, String key, String value) {
		boolean deleted = false;
		String localConceptID = MasterDatabaseUtility.getPhysicalConceptId(engineName, concept);
		RDBMSNativeEngine engine = (RDBMSNativeEngine) Utility.getEngine(Constants.LOCAL_MASTER_DB_NAME);
		Connection conn = engine.makeConnection();
		Statement stmt = null;
		int count = 0;
		try {
			String deleteQuery = "DELETE FROM " + Constants.CONCEPT_METADATA_TABLE + " WHERE "
					+ Constants.PHYSICAL_NAME_ID + " = \'" + localConceptID + "\' and " + Constants.KEY + " = \'" + key
					+ "\' and " + Constants.VALUE + " = \'" + value + "\';";
			stmt = conn.createStatement();
			count = stmt.executeUpdate(deleteQuery);
			if (count > 0) {
				deleted = true;
			}
		} catch (Exception ex) {
			ex.printStackTrace();
		} finally {
			try {
				if (stmt != null) {
					stmt.close();
				}
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}

		return deleted;
	}
	
	/**
	 * Get all engine alias to id combinations
	 * @return
	 */
	public static Map<String, String> getEngineAliasToId() {
		Map<String, String> aliasToId = new HashMap<String, String>();

		RDBMSNativeEngine engine = (RDBMSNativeEngine) Utility.getEngine(Constants.LOCAL_MASTER_DB_NAME);
		if(engine == null) {
			return aliasToId;
		}
		
		Connection conn = engine.makeConnection();
		ResultSet rs = null;
		Statement stmt = null;

		try {
			String sql = "select e.enginename, e.id from engine e";
			stmt = conn.createStatement();
			rs = stmt.executeQuery(sql);
			while(rs.next()) {
				String alias = rs.getString(1);
				String id = rs.getString(2);
				aliasToId.put(alias, id);
			}
		} catch(Exception ex) {
			ex.printStackTrace();
		} finally {
			closeStreams(stmt, rs);
		}
		
		return aliasToId;
	}
	
	/**
	 * Get all engine alias to id combinations
	 * @return
	 */
	public static List<String> getEngineIdsForAlias(String alias) {
		List<String> ids = new Vector<String>();

		RDBMSNativeEngine engine = (RDBMSNativeEngine) Utility.getEngine(Constants.LOCAL_MASTER_DB_NAME);
		if(engine == null) {
			return ids;
		}
		
		Connection conn = engine.makeConnection();
		ResultSet rs = null;
		Statement stmt = null;

		try {
			String sql = "select distinct e.id, e.enginename from engine e where e.enginename='" + alias + "'";
			stmt = conn.createStatement();
			rs = stmt.executeQuery(sql);
			while(rs.next()) {
				ids.add(rs.getString(1));
			}
		} catch(Exception ex) {
			ex.printStackTrace();
		} finally {
			closeStreams(stmt, rs);
		}
		
		return ids;
	}
	
	/**
	 * Try to reconcile and get the engine id
	 * @param engineId
	 * @return
	 */
	public static String testEngineIdIfAlias(String engineId) {
		List<String> appIds = MasterDatabaseUtility.getEngineIdsForAlias(engineId);
		if(appIds.size() == 1) {
			// actually received an app name
//			IReactor.printReactorStackTrace();
			engineId = appIds.get(0);
		} else if(appIds.size() > 1) {
			throw new IllegalArgumentException("There are 2 databases with the name " + engineId + ". Please pass in the correct id to know which source you want to load from");
		}
		
		// i guess the input was the actual id
		return engineId;
	}
	
	private static void closeStreams(Statement stmt, ResultSet rs) {
		try {
			if(rs != null) {
				rs.close();
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}
		try {
			if(stmt != null) {
				stmt.close();
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
	
//	/**
//	 * Get the properties for a given concept
//	 * 
//	 * @param conceptName
//	 * @param engineName
//	 *            filter for the properties
//	 * @return
//	 */
//	public static List<String> getAllConceptProperties(String engineName) {
//		// get the bindings based on the input list
//		if (engineName == null || engineName.isEmpty()) {
//			throw new IllegalArgumentException("Must define engineName");
//		}
//		String propQuery = "select distinct ec.physicalname, ec.property "
//				+ "from engineconcept ec, concept c, engine e " + " WHERE e.enginename= '" + engineName + "' "
//				+ " and ec.engine=e.id and c.localconceptid=ec.localconceptid order by ec.property";
//		RDBMSNativeEngine engine = (RDBMSNativeEngine) Utility.getEngine(Constants.LOCAL_MASTER_DB_NAME);
//		Connection conn = engine.makeConnection();
//		Statement stmt = null;
//		ResultSet rs = null;
//		List<String> properties = new ArrayList<String>();
//		try {
//			stmt = conn.createStatement();
//			rs = stmt.executeQuery(propQuery);
//			while (rs.next()) {
//				String propName = rs.getString(1);
//				properties.add(propName);
//			}
//		} catch (SQLException e) {
//			e.printStackTrace();
//		} finally {
//			closeStreams(stmt, rs);
//		}
//		return properties;
//	}

//	/**
//	 * Maps concepts from sourceDB to concepts in targetDB linked by logical
//	 * names
//	 * 
//	 * @param sourceDB
//	 * @param concepts
//	 *            optional: specify concepts or get all concepts
//	 * @param targetDB
//	 * @return map with sourceDB concept - target DB concept
//	 */
//	public static Map<String, Object> getConceptMapping(String sourceDB, List<String> concepts, String targetDB) {
//		Map<String, Object> map = new HashMap<String, Object>();
//		List<String> conceptualName = null;
//		// map specified concepts
//		if (!(concepts == null) && !concepts.isEmpty()) {
//			conceptualName = concepts;
//		} else {
//			// get all concepts
//			conceptualName = getAllConceptProperties(sourceDB);
//		}
//		for (String concept : conceptualName) {
//			List<String> propeties = new Vector<>();
//			propeties.add(concept);
//			List<String> logicalNames = getAllLogicalNamesFromConceptualRDBMS(propeties, sourceDB);
//			for (String logicalName : logicalNames) {
//				List<String> conceptuals = getConceptualNameFromLogical(targetDB, logicalName);
//				if (conceptuals != null && !conceptuals.isEmpty()) {
//					map.put(concept, conceptuals);
//				}
//			}
//		}
//		return map;
//	}
	
//	/**
//	 * Get map of physical to conceptual name for an engine
//	 * 
//	 * @param physicalNames
//	 * @param engineFilter
//	 * @return
//	 */
//	public static Map<String, String> getConceptualFromPhysical(List<String> physicalNames, String engineFilter) {
//		RDBMSNativeEngine engine = (RDBMSNativeEngine) Utility.getEngine(Constants.LOCAL_MASTER_DB_NAME);
//		Connection conn = engine.makeConnection();
//		// select logicalname from concept where conceptualname='MovieBudget'
//		// and conceptualname != logicalname
//		// select distinct c.conceptualname, ec.physicalname from concept c,
//		// engineconcept ec, engine e where ec.localconceptid=c.localconceptid
//		// and ec.physicalname in ('Title', 'Actor');
//		Map<String, String> physicalConceptMap = new HashMap<>();
//		String physicalList = makeListToString(physicalNames);
//		ResultSet rs = null;
//		Statement stmt = null;
//		try {
//			String query = "select distinct ec.physicalname, c.conceptualname "
//					+ "from engineconcept ec, concept c, engine e where ec.localconceptid=c.localconceptid "
//					+ " and e.enginename = '" + engineFilter + "' " + "and ec.engine=e.id " + "and ec.physicalname in "
//					+ physicalList + ";";
//			stmt = conn.createStatement();
//			rs = stmt.executeQuery(query);
//			while (rs.next()) {
//				String physical = rs.getString(1);
//				String concept = rs.getString(2);
//				physicalConceptMap.put(physical, concept);
//			}
//		} catch (Exception ex) {
//			ex.printStackTrace();
//		} finally {
//			closeStreams(stmt, rs);
//		}
//		return physicalConceptMap;
//	}
	
	public static Map<String, List<String>> databaseTranslator(String sourceDB, String targetDB) {
		RDBMSNativeEngine engine = (RDBMSNativeEngine) Utility.getEngine(Constants.LOCAL_MASTER_DB_NAME);
		Connection conn = engine.makeConnection();
		// select logicalname from concept where conceptualname='MovieBudget'
		// and conceptualname != logicalname
		// select distinct c.conceptualname, ec.physicalname from concept c,
		// engineconcept ec, engine e where ec.localconceptid=c.localconceptid
		// and ec.physicalname in ('Title', 'Actor');
		Map<String, List<String>> map = new HashMap<>();
		ResultSet rs = null;
		Statement stmt = null;
		try {
			String query = "SELECT e.engineName as sourceEngine, c.conceptualName as sourceConceptual, ec.physicalName as sourcePhysical, c.logicalName, "
					+ "targetEngine, targetConceptual, targetPhysical from engine e, engineconcept ec, concept c  "
					+ "INNER JOIN (SELECT e.engineName as targetEngine, c.conceptualName as targetConceptual, "
					+ "ec.physicalName as targetPhysical, c.logicalName as targetLogical "
					+ "from engine e, engineconcept ec, concept c WHERE e.id=ec.engine and ec.localConceptID = c.localConceptID and e.id = '"
					+ targetDB + "' " + "and c.conceptualName != c.logicalName) ON c.logicalName = targetLogical "
					+ "WHERE e.id=ec.engine and ec.localConceptID = c.localConceptID and e.id = '" + sourceDB
					+ "' and c.conceptualName != c.logicalName";
			stmt = conn.createStatement();
			rs = stmt.executeQuery(query);
			while (rs.next()) {
				String sourceEngine = rs.getString(1);
				String sourceConceptual = rs.getString(2);
				String sourcePhysical = rs.getString(3);
				String logicalName = rs.getString(4);
				String targetEngine = rs.getString(5);
				String targetConceptual = rs.getString(6);
				String targetPhysical = rs.getString(7);

				List<String> targetPhysicals = new Vector<String>();
				if (map.containsKey(sourcePhysical)) {
					targetPhysicals = map.get(sourcePhysical);
				}
				targetPhysicals.add(targetPhysical);
				
				map.put(sourcePhysical, targetPhysicals);
			}
		} catch (Exception ex) {
			ex.printStackTrace();
		} finally {
			closeStreams(stmt, rs);
		}
		return map;
	}
	
	public static Collection<String> getAllConceptualNames() {
		RDBMSNativeEngine engine = (RDBMSNativeEngine) Utility.getEngine(Constants.LOCAL_MASTER_DB_NAME);
		Connection conn = engine.makeConnection();
		
		List<String> results = new Vector<String>();

		String query = "select distinct conceptualname, lower(conceptualname) as lname from concept order by lname";
		ResultSet rs = null;
		Statement stmt = null;
		try {
			stmt = conn.createStatement();
			rs = stmt.executeQuery(query);
			while (rs.next()) {
				results.add(rs.getString(1));
			}
		} catch (Exception ex) {
			ex.printStackTrace();
		} finally {
			closeStreams(stmt, rs);
		}
		
		return results;
	}
	
	public static Collection<String> getAllConceptualNames(Collection<String> engineFilters) {
		RDBMSNativeEngine engine = (RDBMSNativeEngine) Utility.getEngine(Constants.LOCAL_MASTER_DB_NAME);
		Connection conn = engine.makeConnection();
		
		List<String> results = new Vector<String>();

		String query = "select distinct c.conceptualname, lower(c.conceptualname) as lname"
				+ " from concept c"
				+ " inner join engineconcept ec on c.localconceptid=ec.localconceptid"
				+ " where ec.engine in " + makeListToString(engineFilters)
				+ " order by lname";
		ResultSet rs = null;
		Statement stmt = null;
		try {
			stmt = conn.createStatement();
			rs = stmt.executeQuery(query);
			while (rs.next()) {
				results.add(rs.getString(1));
			}
		} catch (Exception ex) {
			ex.printStackTrace();
		} finally {
			closeStreams(stmt, rs);
		}
		
		return results;
	}
	
	public static List<String[]> getConceptualToLogicalToPhysicalModel(List<String> conceptualNames, Collection<String> engineFilters) {
		RDBMSNativeEngine engine = (RDBMSNativeEngine) Utility.getEngine(Constants.LOCAL_MASTER_DB_NAME);
		Connection conn = engine.makeConnection();
		
//		select c.conceptualname, c.logicalname, concat(ec2.physicalname, '__', ec.physicalname), e.enginename
//		from concept c 
//		inner join engineconcept ec on c.localconceptid=ec.localconceptid 
//		inner join engineconcept ec2 on ec.parentphysicalid=ec2.physicalnameid 
//		inner join engine e on ec.engine=e.id 
//		where ec2.pk=true and c.conceptualname='Title';
		
		List<String[]> results = new Vector<String[]>();

		String query = "select c.conceptualname, c.logicalname, concat(ec2.physicalname, '__', ec.physicalname), e.enginename "
				+ "from concept c "
				+ "inner join engineconcept ec on c.localconceptid=ec.localconceptid "
				+ "inner join engineconcept ec2 on ec.parentphysicalid=ec2.physicalnameid "
				+ "inner join engine e on ec.engine=e.id "
				+ "where ec2.pk=true and c.conceptualname in " + makeListToString(conceptualNames)
				+ (engineFilters == null ? "" : " and ec.engine in " + makeListToString(engineFilters))
				;
		
		ResultSet rs = null;
		Statement stmt = null;
		try {
			stmt = conn.createStatement();
			rs = stmt.executeQuery(query);
			while (rs.next()) {
				String[] row = new String[4];
				row[0] = rs.getString(1);
				row[1] = rs.getString(2);
				row[2] = rs.getString(3);
				row[3] = rs.getString(4);
				results.add(row);
			}
		} catch (Exception ex) {
			ex.printStackTrace();
		} finally {
			closeStreams(stmt, rs);
		}
		
		return results;
	}
	
	
	public static void main(String[] args) throws Exception {
		TestUtilityMethods.loadDIHelper("C:\\workspace\\Semoss_Dev\\RDF_Map.prop");
		String engineProp = "C:\\workspace\\Semoss_Dev\\db\\LocalMasterDatabase.smss";
		IEngine coreEngine = new RDBMSNativeEngine();
		coreEngine.setEngineId(Constants.LOCAL_MASTER_DB_NAME);
		coreEngine.openDB(engineProp);
		DIHelper.getInstance().setLocalProperty(Constants.LOCAL_MASTER_DB_NAME, coreEngine);
		
		List<String> logicalNames = new Vector<String>();
		logicalNames.add("MovieBudget");
		
		Gson gson = new GsonBuilder()
				.disableHtmlEscaping()
				.excludeFieldsWithModifiers(Modifier.STATIC, Modifier.TRANSIENT)
				.setPrettyPrinting()
				.create();
		
		System.out.println(gson.toJson(getDatabaseConnections(logicalNames, null)));
	}
	
}