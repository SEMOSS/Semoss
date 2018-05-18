package prerna.nameserver.utility;

import java.lang.reflect.Modifier;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.Vector;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import prerna.ds.util.RdbmsQueryBuilder;
import prerna.engine.api.IEngine;
import prerna.engine.impl.rdbms.RDBMSNativeEngine;
import prerna.test.TestUtilityMethods;
import prerna.util.Constants;
import prerna.util.DIHelper;
import prerna.util.Utility;

/**
 * @author pkapaleeswaran
 *
 */
public class MasterDatabaseUtility {

	// -----------------------------------------   RDBMS CALLS ---------------------------------------
	
	public static void initLocalMaster() {
		RDBMSNativeEngine engine = (RDBMSNativeEngine) Utility.getEngine(Constants.LOCAL_MASTER_DB_NAME);
		Connection conn = engine.makeConnection();
		String [] colNames = null;
		String [] types = null;
		
		// engine table
		colNames = new String[]{"ID", "EngineName", "ModifiedDate", "Type"};
		types = new String[]{"varchar(800)", "varchar(800)", "timestamp", "varchar(800)"};
		executeSql(conn, RdbmsQueryBuilder.makeOptionalCreate("engine", colNames, types));
		
		// engine concept table
		colNames = new String[]{"Engine", "PhysicalName", "ParentPhysicalID", "PhysicalNameID", "LocalConceptID", "PK", "Property", "Original_Type", "Property_Type"};
		types = new String[]{"varchar(800)", "varchar(800)", "varchar(800)", "varchar(800)", "varchar(800)", "boolean", "boolean", "varchar(800)","varchar(800)"};
		executeSql(conn, RdbmsQueryBuilder.makeOptionalCreate("engineconcept", colNames, types));
		
		// concept table
		colNames = new String[]{"LocalConceptID", "ConceptualName", "LogicalName", "DomainName", "GlobalID"};
		types = new String[]{"varchar(800)", "varchar(800)", "varchar(800)", "varchar(800)", "varchar(800)"};
		executeSql(conn, RdbmsQueryBuilder.makeOptionalCreate("concept", colNames, types));
		
		// relation table
		colNames = new String[]{"ID", "SourceID", "TargetID", "GlobalID"};
		types = new String[]{"varchar(800)", "varchar(800)", "varchar(800)", "varchar(800)"};
		executeSql(conn, RdbmsQueryBuilder.makeOptionalCreate("relation", colNames, types));
		
		// engine relation table
		colNames = new String[]{"Engine", "RelationID", "InstanceRelationID", "SourceConceptID", "TargetConceptID", "SourceProperty", "TargetProperty", "RelationName"}; //"DomainName"};
		types = new String[]{"varchar(800)", "varchar(800)","varchar(800)", "varchar(800)", "varchar(800)", "varchar(800)", "varchar(800)", "varchar(800)"};
		executeSql(conn, RdbmsQueryBuilder.makeOptionalCreate("enginerelation", colNames, types));
		
		// kv store
		colNames = new String[]{"K","V"};
		types = new String[]{"varchar(800)", "varchar(800)"};
		executeSql(conn, RdbmsQueryBuilder.makeOptionalCreate("kvstore", colNames, types));

		// concept metadata
		colNames = new String[] {Constants.LOCAL_CONCEPT_ID, Constants.KEY, Constants.VALUE };
		types = new String[] { "varchar(800)", "varchar(800)", "varchar(20000)" };
		executeSql(conn, RdbmsQueryBuilder.makeOptionalCreate(Constants.CONCEPT_METADATA_TABLE, colNames, types));
		
		// x-ray config
		colNames = new String[]{"filename", "config" };
		types = new String[]{"varchar(800)", "varchar(20000)" };
		executeSql(conn, RdbmsQueryBuilder.makeOptionalCreate("xrayconfigs", colNames, types));
	}
	
	private static void executeSql(Connection conn, String sql) {
		Statement stmt = null;
		try {
			stmt = conn.createStatement();
			stmt.execute(sql);
		} catch (SQLException e) {
			e.printStackTrace();
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
			String logicalQuery = "select distinct c.logicalname, ec.physicalname from "
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
	 * @param engineName
	 * @return
	 */
	public static List<Object[]> getAllTablesAndColumns(String engineName) {
		String query = "select c.conceptualname as column, c2.conceptualname as table, ec.property_type as type, ec.pk as pk "
				+ "from engine e, engineconcept ec, engineconcept ec2, concept c, concept c2 "
				+ "where e.id = ec.engine "
				+ "and e.enginename = '" + engineName + "' "
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
	 * Get a list of connections for a given logical name
	 * @param engineFilter 
	 * @param engineName
	 * @return
	 */
	public static List<Map<String, Object>> getDatabaseConnections(List<String> logicalNames, String engineFilter) {
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
			engineFilterStr = " and e.enginename = '" + engineFilter + "'";
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
		
		String query = "select ec.parentphysicalid as table, ec2.physicalnameid as equivTableId, ec.physicalnameid equivColumnId,"
				+ " c2.conceptualname as equivConceptTableName, c.conceptualname as equivConceptColumnName, ec.pk as pk" 
				+ " from engine e, engineconcept ec, engineconcept ec2, concept c, concept c2"
				+ " where ec.localconceptid in (select localconceptid from concept where logicalname in (" + sb.toString() + "))"
				+ engineFilterStr
				+ " and e.id = ec.engine"
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
		query = "select distinct e.enginename, c2.conceptualname as table, c.conceptualname as column, ec.property_type as type, ec.pk as pk, ec2.physicalnameid"
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
				String engineName = rs.getString(1);
				String table = rs.getString(2);
				String column = rs.getString(3);
				String type = rs.getString(4);
				boolean pk = rs.getBoolean(5);
				String equivId = rs.getString(6);
				
				Object[] equivTableCol = parentEquivMap.get(equivId);
				
				// above query will return the actual match columns as well
				if(equivTableCol[0].toString().equals(table) && equivTableCol[1].toString().equals(column)) {
					continue;
				}
				
				// if we passed the above test, add the valid connection
				Map<String, Object> row = new HashMap<String, Object>();
				row.put("app", engineName);
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
		query = "select distinct e.enginename, c.conceptualname, c2.conceptualname, ec2.property_type "
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
				String engineName = rs.getString(1);
				String upstream = rs.getString(2);
				String downstream = rs.getString(3);
				String type = rs.getString(4);
				
				// the downstream nodes
				// mean that the source is the equivalent concept
				
				// if we passed the above test, add the valid connection
				Map<String, Object> row = new HashMap<String, Object>();
				row.put("app", engineName);
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
		query = "select distinct e.enginename, c.conceptualname, c2.conceptualname, ec2.property_type "
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
				String engineName = rs.getString(1);
				String upstream = rs.getString(2);
				String downstream = rs.getString(3);
				String type = rs.getString(4);
				
				// the downstream nodes
				// mean that the source is the equivalent concept
				
				// if we passed the above test, add the valid connection
				Map<String, Object> row = new HashMap<String, Object>();
				row.put("app", engineName);
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

	public static Map<String, Object[]> getMetamodelRDBMS(String engineName) {
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
		
		Map<String, Object[]> finalHash = new Hashtable<String, Object[]>();
		
		// idHash - physical ID to the name of the node
		Hashtable <String, String> idHash = new Hashtable<String, String>();
		Hashtable <String, MetamodelVertex> nodeHash = new Hashtable <String, MetamodelVertex>();
		
		try {
			String nodeQuery = "select c.conceptualname, ec.physicalname, ec.localconceptid, ec.physicalnameid, ec.parentphysicalid, ec.property from "
					 + "engineconcept ec, concept c, engine e "
					 + "where ec.engine=e.id "
					 + "and e.enginename='" + engineName + "' "
					 + "and c.localconceptid=ec.localconceptid order by ec.property";
			stmt = conn.createStatement();
			rs = stmt.executeQuery(nodeQuery);
			while(rs.next()) {
				String conceptualName = rs.getString(1);
				String physicalName = rs.getString(2);
				String physicalId = rs.getString(4);
				String parentPhysicalId = rs.getString(5); 
				
				// sets the physical id to conceptual name
				idHash.put(physicalId, conceptualName);
				
				MetamodelVertex node = null;
				
				// gets the conceptual name
				String conceptName = idHash.get(physicalId);
				
				// because it is ordered by property, this would already be there
				String parentName = idHash.get(parentPhysicalId);

				// if already there, should we still add it ?
				if(nodeHash.containsKey(parentName))
					node = nodeHash.get(parentName);
				else
				{
					node = new MetamodelVertex(parentName);
					nodeHash.put(conceptualName, node);
				}
				
				if(!conceptName.equalsIgnoreCase(parentName))
					node.addProperty(conceptName);				
			}
		} catch(SQLException ex) {
			ex.printStackTrace();
		} finally {
			// do not close the stmt
			// reuse it below
			closeStreams(null, rs);
		}
			
		try {
			// get the edges next
			//SELECT er.sourceconceptid, er.targetconceptid FROM ENGINERELATION er, engine e where e.id = er.engine and e.enginename = 'Mv1'
			String edgeQuery = "SELECT er.sourceconceptid, er.targetconceptid FROM ENGINERELATION er, engine e where e.id = er.engine and "
					+ "e.enginename = '" + engineName + "'";

			if(stmt == null) {
				stmt = conn.createStatement();
			}
			rs = stmt.executeQuery(edgeQuery);

			Hashtable <String, Hashtable> edgeHash = new Hashtable<String, Hashtable>();
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
			finalHash.put("nodes", nodeHash.values().toArray());
			finalHash.put("edges", edgeHash.values().toArray());
			
		} catch(SQLException ex) {
			ex.printStackTrace();
		} finally {
			closeStreams(stmt, rs);
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
				if (nodeHash.containsKey(engineName + "." + parentName))
					node = nodeHash.get(engineName + "." + parentName);
				else {
					node = new MetamodelVertex(engineName + "." + parentName);
					nodeHash.put(engineName + "." + conceptualName, node);
				}

				if (!conceptName.equalsIgnoreCase(parentName)) {
					// might be this or might not be
					// node.addProperty(engineName+ "." +conceptName);
					node.addProperty(conceptName);
				}
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
				newEdge.put("source", engineName + "." + sourceName);
				newEdge.put("target", engineName + "." + targetName);

				// if(nodeHash.containsKey(toId))

				boolean foundNode = true;
				if (!nodeHash.containsKey(engineName + "." + sourceName)) {
					foundNode = false;
					System.out.println("Unable to find node " + sourceName);
				}
				if (!nodeHash.containsKey(engineName + "." + targetName)) {
					foundNode = false;
					System.out.println("Unable to find node " + targetName);
				}

				if (foundNode) {
					edgeHash.put(engineName + "." + sourceName + "-" + engineName + "." + targetName, newEdge);
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
	 * Get the properties for a given concept across all the databases
	 * @param conceptName
	 * @param engineName		optional filter for the properties
	 * @return
	 */
	public static Map<String, HashMap> getConceptProperties(List<String> conceptLogicalNames, String engineFilter) {
		// get the bindings based on the input list
		String conceptString = makeListToString(conceptLogicalNames);
		String engineString = " and e.enginename= '" + engineFilter +"' ";
		if(engineFilter == null || engineFilter.isEmpty()) {
			engineString = "";
		}
		
		String propQuery = "select distinct e.enginename, c.conceptualname, ec.physicalname, ec.parentphysicalid, ec.physicalnameid, ec.property "
				+ "from engineconcept ec, concept c, engine e where ec.parentphysicalid in "
				+ "(select physicalnameid from engineconcept ec where localconceptid in (select localconceptid from concept where conceptualname in" +  conceptString.toString() + ") )" 
				+ engineString
				+ " and ec.engine=e.id and c.localconceptid=ec.localconceptid order by ec.property";
	
		Map<String, HashMap> returnHash = new HashMap<String, HashMap>();;
		Map<String, Map<String, ArrayList<String>>> queryData = new HashMap<String, Map<String, ArrayList<String>>>();

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
					Map<String, ArrayList<String>> engineMap = null;
					if(queryData.containsKey(engineName)) {
						engineMap = queryData.get(engineName);
					} else {
						engineMap = new HashMap<String, ArrayList<String>>();
						// add to query data map
						queryData.put(engineName, engineMap);
					}
					ArrayList<String> propList = null;
					if(engineMap.containsKey(parentName)) {
						propList = engineMap.get(parentName);
					} else {
						propList = new ArrayList<>();
					}
					propList.add(propName);
					// add to the engine map
					engineMap.put(parentName, propList);
				}
			}
		} catch (SQLException e) {
			e.printStackTrace();
		} finally {
			closeStreams(stmt, rs);
		}
		
		
		for(String engineName : queryData.keySet()) {
			returnHash.put(engineName, (HashMap) queryData.get(engineName));
		}

		return returnHash;
	}
	
	/**
	 * Get the properties for a given concept across all the databases
	 * @param conceptName
	 * @param engineName		optional filter for the properties
	 * @return
	 */
	public static Map<String, Object[]> getConceptPropertiesRDBMS(List<String> conceptLogicalNames, String engineFilter) {
		// get the bindings based on the input list
		String conceptString = makeListToString(conceptLogicalNames);
		String engineString = " and e.enginename= '" + engineFilter +"' ";
		if(engineFilter == null || engineFilter.isEmpty()) {
			engineString = "";
		}
		
		String propQuery = "select distinct e.enginename, c.conceptualname, ec.physicalname, ec.parentphysicalid, ec.physicalnameid, ec.property "
				+ "from engineconcept ec, concept c, engine e where ec.parentphysicalid in "
				+ "(select physicalnameid from engineconcept ec where localconceptid in (select localconceptid from concept where conceptualname in" +  conceptString.toString() + ") )" 
				+ engineString
				+ " and ec.engine=e.id and c.localconceptid=ec.localconceptid order by ec.property";
	
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
	
	private static String makeListToString(List <String> conceptLogicalNames)
	{
		StringBuilder conceptString = new StringBuilder("(");
		for(int logIndex = 0;logIndex < conceptLogicalNames.size();logIndex++)
		{
			if(logIndex > 0) {
				conceptString.append(",");
			}
			conceptString.append("'" + conceptLogicalNames.get(logIndex) + "'");
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
	public static Map getConnectedConceptsRDBMS(List<String> conceptLogicalNames) {
		// get the bindings based on the input list
		String conceptString = makeListToString(conceptLogicalNames);
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

		String conceptMasterQuery = "select e.enginename, ec.engine, c.conceptualname, ec.physicalnameid, ec.physicalname from concept c, engineconcept ec, engine e where "
				+ "c.logicalname in " + conceptString
				+ "and c.localconceptid=ec.localconceptid and e.id=ec.engine";
		try {
			stmt = conn.createStatement();
			rs = stmt.executeQuery(conceptMasterQuery);
			while(rs.next()) {
				String engineName = rs.getString(1);
				String logicalName = rs.getString(3);
				String physicalNameId = rs.getString(4);
				String equivalentConcept = rs.getString(5);

				// put the id for future reference
				// no reason why we cannot cache but.. 
				idToName.put(physicalNameId, logicalName);

				Map <String, Object> conceptSpecific = null;
				if(retMap.containsKey(engineName)) {
					conceptSpecific = retMap.get(engineName);
				} else {
					conceptSpecific = new TreeMap<String, Object>();
				}
				retMap.put(engineName, conceptSpecific);

				Hashtable <String, String> stream = new Hashtable<String, String>();
				stream.put("equivalentConcept", equivalentConcept);

				conceptSpecific.put(logicalName, stream);
				retMap.put(engineName, conceptSpecific);
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
		String downstreamQuery = "select distinct  e.enginename, er.sourceconceptid, 'upstream' as upstream,  "
				+ "er.relationname,  c.conceptualname , er.engine, er.targetconceptid, ec.physicalname from "
				+ "enginerelation er, engineconcept ec, concept c, engine e "
				+ "where er.sourceconceptid in (select physicalnameid from engineconcept where localconceptid in "
				+ "(select localconceptid from concept where logicalname in " + conceptString + ")) "
				+ "and ec.physicalnameid=er.targetconceptid and c.localconceptid=ec.localconceptid and e.id=er.engine;";

		try {
			if(stmt == null) {
				stmt = conn.createStatement();
			}
			rs = stmt.executeQuery(downstreamQuery);
			while(rs.next())
			{
				String engineName = rs.getString(1);
				String coreConceptId = rs.getString(2);
				String relationName = rs.getString(4);
				String streamConceptName = rs.getString(5);
				String streamPhysicalName = rs.getString(8);

				// this is the main concept
				String coreConceptName = idToName.get(coreConceptId);

				Map <String, Map> engineSpecific = retMap.get(engineName);
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
				retMap.put(engineName, engineSpecific);
			}
		} catch(SQLException e) {
			e.printStackTrace();
		} finally {
			closeStreams(null, rs);
		}

		// now time to run the upstream and downstream queries
		String upstreamQuery = "select distinct  e.enginename, er.targetconceptid, 'downstream' as downstream,  "
				+ "er.relationname,  c.conceptualname , er.engine, er.sourceconceptid, ec.physicalname from "
				+ "enginerelation er, engineconcept ec, concept c, engine e "
				+ "where er.targetconceptid in (select physicalnameid from engineconcept where localconceptid in "
				+ "(select localconceptid from concept where logicalname in " + conceptString + ")) "
				+ "and ec.physicalnameid=er.sourceconceptid and c.localconceptid=ec.localconceptid and e.id=er.engine;";
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
	
	public static Map<String, Set<String>> getAllConceptsFromEnginesRDBMS() {
		/*
		 * Grab the local master engine and query for the concepts
		 * We do not want to load the engine until the user actually wants to use it
		 */
		RDBMSNativeEngine engine = (RDBMSNativeEngine) Utility.getEngine(Constants.LOCAL_MASTER_DB_NAME);
		Connection conn = engine.makeConnection();

		// select distinct c.logicalname, ec.physicalname from concept c, engineconcept ec, engine e where ec.localconceptid=c.localconceptid and e.id=ec.engine and e.enginename = 'actor';
		// I am getting the logical and the physical
		Map<String, Set<String>> returnHash = new TreeMap<String, Set<String>>();
		Statement stmt = null;
		ResultSet rs = null;
		try
		{
			String conceptQuery = "select distinct e.enginename from engine e"; //, c.logicalname, ec.physicalname from concept c, engineconcept ec, engine e where ec.localconceptid=c.localconceptid and e.id=ec.engine";
			stmt = conn.createStatement();
			rs = stmt.executeQuery(conceptQuery);
			while(rs.next()) {
				String engineName = rs.getString(1);
				Set <String> conceptSet = new TreeSet<String>();
				conceptSet.add("");
				returnHash.put(engineName, conceptSet);
			}
		} catch(SQLException ex) {
			ex.printStackTrace();
		} finally {
			closeStreams(stmt, rs);
		}
		return returnHash;
	}

	public static List<String> getAllEnginesRDBMS() {
		/*
		 * Grab the local master engine and query for the concepts
		 * We do not want to load the engine until the user actually wants to use it
		 */
		RDBMSNativeEngine engine = (RDBMSNativeEngine) Utility.getEngine(Constants.LOCAL_MASTER_DB_NAME);
		Connection conn = engine.makeConnection();
		Statement stmt = null;
		ResultSet rs = null;
		
		List <String> retList = new ArrayList<String>();
		if(conn != null) {
			// select distinct c.logicalname, ec.physicalname from concept c, engineconcept ec, engine e where ec.localconceptid=c.localconceptid and e.id=ec.engine and e.enginename = 'actor';
			// I am getting the logical and the physical
			try {
				String conceptQuery = "select distinct e.enginename from engine e order by enginename"; //, c.logicalname, ec.physicalname from concept c, engineconcept ec, engine e where ec.localconceptid=c.localconceptid and e.id=ec.engine";
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
	 * Get the list of concepts for a given engine
	 * @param engineName
	 * @return
	 */
	public static Set<String> getConceptsWithinEngineRDBMS(String engineName) {
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
			String query = "select distinct c.logicalname, ec.physicalname from concept c, engineconcept ec, engine e "
						+ "where ec.localconceptid=c.localconceptid and e.id=ec.engine and ec.property=false and "
						+ "e.enginename = '" + engineName + "'";
			
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
	
	public static String getBasicDataType(String engineName, String conceptualName, String parentConceptualName) {
		String query = null;
		if(parentConceptualName == null || parentConceptualName.isEmpty()) {
			query = "select distinct ec.property_type "
					+ "from engineconcept ec "
					+ "inner join engine e on e.id = ec.engine "
					+ "inner join concept c on ec.localconceptid = c.localconceptid "
					+ "where e.enginename = '" + engineName + "' and c.conceptualname = '" + conceptualName + "'";
		} else {
			query = "select distinct ec.property_type "
					+ "from engine e "
					+ "inner join engineconcept ec on e.id = ec.engine "
					+ "inner join concept c on ec.localconceptid = c.localconceptid "
					+ "where e.enginename = '" + engineName + "' and "
					+ "c.conceptualname = '" + conceptualName + "' and "
					+ "ec.parentphysicalid in "
					+ "(select distinct ec.physicalnameid "
					+ "from engineconcept ec "
					+ "inner join engine e on ec.engine = e.id "
					+ "inner join concept c on c.localconceptid = ec.localconceptid "
					+ "where e.enginename = '" + engineName + "' and "
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
	
	/**
	 * Adds logical name to concept from engine
	 * 
	 * @param engineName
	 * @param concept
	 * @param logicalName
	 * @return
	 */
	public static boolean addLogicalName(String engineName, String concept, String logicalName) {
		RDBMSNativeEngine engine = (RDBMSNativeEngine) Utility.getEngine(Constants.LOCAL_MASTER_DB_NAME);
		Connection masterConn = engine.makeConnection();
		Statement stmt = null;
		ResultSet rs = null;
		int size = 0;
		try {
			String duplicateQueryCheck = "select localconceptid, conceptualname, logicalname, "
					+ "domainname, globalid from concept "
					+ "where localconceptid in (select localconceptid from engineconcept "
					+ "where engine in (select id from engine where enginename = \'" + engineName + "\')) "
					+ "and conceptualname = \'" + concept + "\' and logicalname = \'" + logicalName + "\';";
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
						+ "where engine in (select id from engine where enginename = \'" + engineName + "\')) "
						+ "and conceptualname = \'" + concept + "\';";
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
						String insertString = "insert into concept " + "values(\'" + localConceptID + "\', \'"
								+ conceptualName + "\', \'" + logicalName + "\', \'" + domainName + "\', \'"
								+ globalID.toString() + "\');";
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
					+ "where engine in (select id from engine where enginename = \'" + engineName + "\')) "
					+ "and conceptualname = \'" + concept + "\' and logicalname = \'" + logicalName + "\';";
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
	 * @param engineName
	 * @param concept
	 * @return logicalNames
	 */
	public static List<String> getLogicalNames(String engineName, String concept) {
		List<String> logicalNames = new ArrayList<String>();
		
		RDBMSNativeEngine engine = (RDBMSNativeEngine) Utility.getEngine(Constants.LOCAL_MASTER_DB_NAME);
		Connection masterConn = engine.makeConnection();
		Statement stmt = null;
		ResultSet rs = null;

		try {
			String query = "select logicalname from concept "
					+ "where localconceptid in (select localconceptid from engineconcept "
					+ "where engine in (select id from engine where enginename = \'" + engineName + "\')) "
					+ "and conceptualname = \'" + concept + "\';";
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
	
	
	/**
	 * 
	 * @param targetDB
	 * @param logical
	 * @return
	 */
	public static List<String> getConceptualNameFromLogical(String targetDB, String logical) {
		List<String> conceptulNames = new Vector<String>();
		RDBMSNativeEngine engine = (RDBMSNativeEngine) Utility.getEngine(Constants.LOCAL_MASTER_DB_NAME);
		Connection masterConn = engine.makeConnection();
		Statement stmt = null;
		ResultSet rs = null;
		try {
			String query = "select distinct c.localconceptid, c.conceptualname, c.logicalname, e.enginename "
					+ "from concept c, engineconcept ec, engine e " + "where c.logicalname in ('" + logical
					+ "') and e.enginename = '" + targetDB
					+ "' and e.id = ec.engine and ec.localconceptid = c.localconceptid;";
			stmt = masterConn.createStatement();
			rs = stmt.executeQuery(query);
			while (rs.next()) {
				conceptulNames.add(rs.getString(2));
			}
		} catch (SQLException e) {
			e.printStackTrace();
		} finally {
			closeStreams(stmt, rs);
		}
		return conceptulNames;
	}
	
	/**
	 * Get the properties for a given concept
	 * @param conceptName
	 * @param engineName		filter for the properties
	 * @return
	 */
	public static Map<String, List<String>> getSpecificConceptPropertiesRDBMS(String conceptString, String engineName) {
		// get the bindings based on the input list
		String engineString = " and e.enginename= '" + engineName +"' ";
		if(engineName == null || engineName.isEmpty()) {
			throw new IllegalArgumentException("Must define engineName");
		}
		
		String propQuery = "select distinct ec.physicalname, ec.property "
				+ "from engineconcept ec, concept c, engine e "
				+ "where ec.parentphysicalid in (select physicalnameid from engineconcept ec "
				+ "where localconceptid in (select localconceptid from concept where conceptualname in ('" +  conceptString + "')) )" 
				+ engineString
				+ " and ec.engine=e.id and c.localconceptid=ec.localconceptid order by ec.property";
		
		Map<String, List<String>> returnHash = new HashMap<String, List<String>>();

		RDBMSNativeEngine engine = (RDBMSNativeEngine) Utility.getEngine(Constants.LOCAL_MASTER_DB_NAME);
		Connection conn = engine.makeConnection();
		Statement stmt = null;
		ResultSet rs = null;
		List<String> properties = new ArrayList<String>();
		
		try {
			stmt = conn.createStatement();
			rs = stmt.executeQuery(propQuery);
			while(rs.next()) {
				String propName = rs.getString(1);
				properties.add(propName);
			}
		} catch (SQLException e) {
			e.printStackTrace();
		} finally {
			closeStreams(stmt, rs);
		}
		
		returnHash.put(engineName, properties);
		return returnHash;
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
	 * @param engineName
	 * @param concept
	 * @return
	 */
	public static String getLocalConceptID(String engineName, String concept) {
		String localConceptID = null;
		RDBMSNativeEngine engine = (RDBMSNativeEngine) Utility.getEngine(Constants.LOCAL_MASTER_DB_NAME);
		Connection conn = engine.makeConnection();
		Statement stmt = null;
		ResultSet rs = null;
		try {
			String query = "select localconceptid from concept "
					+ "where localconceptid in (select localconceptid from engineconcept "
					+ "where engine in (select id from engine where enginename = \'" + engineName + "\')) "
					+ "and conceptualname = \'" + concept + "\';";
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
			throw new IllegalArgumentException("Unable to get concept ID for " + engineName + " " + concept);
		}
		return localConceptID;
	}
	
	/**
	 * Get concept metadata value for a key
	 * 
	 * @param engineName
	 * @param concept
	 * @param key
	 * @return
	 */
	public static String getMetadataValue(String engineName, String concept, String key) {
		String value = null;
		RDBMSNativeEngine engine = (RDBMSNativeEngine) Utility.getEngine(Constants.LOCAL_MASTER_DB_NAME);
		Connection conn = engine.makeConnection();
		Statement stmt = null;
		ResultSet rs = null;
		try {
			String query = "select " + Constants.VALUE + " from " + Constants.CONCEPT_METADATA_TABLE
					+ " where localconceptid in (select localconceptid from concept "
					+ "where localconceptid in (select localconceptid from engineconcept "
					+ "where engine in (select id from engine where enginename = \'" + engineName + "\')) "
					+ "and conceptualname = \'" + concept + "\') and " + Constants.KEY + " = \'" + key + "\';";
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
					+ Constants.LOCAL_CONCEPT_ID + " = \'" + localConceptID + "\' and " + Constants.KEY + " = \'" + key
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
		String localConceptID = MasterDatabaseUtility.getLocalConceptID(engineName, concept);
		RDBMSNativeEngine engine = (RDBMSNativeEngine) Utility.getEngine(Constants.LOCAL_MASTER_DB_NAME);
		Connection conn = engine.makeConnection();
		Statement stmt = null;
		int count = 0;
		try {
			String deleteQuery = "DELETE FROM " + Constants.CONCEPT_METADATA_TABLE + " WHERE "
					+ Constants.LOCAL_CONCEPT_ID + " = \'" + localConceptID + "\' and " + Constants.KEY + " = \'" + key
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
	 * Get the engine id from the engine alias
	 * @param alias
	 * @return
	 */
	public static String getEngineIdFromAlias(String alias) {
		RDBMSNativeEngine engine = (RDBMSNativeEngine) Utility.getEngine(Constants.LOCAL_MASTER_DB_NAME);
		if(engine == null) {
			return null;
		}
		
		Connection conn = engine.makeConnection();
		ResultSet rs = null;
		Statement stmt = null;

		String id = null;
		try {
			String sql = "select e.id from engine e where e.enginename='" + alias + "'";
			stmt = conn.createStatement();
			rs = stmt.executeQuery(sql);
			while(rs.next()) {
				id = rs.getString(1);
			}
		} catch(Exception ex) {
			ex.printStackTrace();
		} finally {
			closeStreams(stmt, rs);
		}
		
		return id;
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
	
	/**
	 * Get the properties for a given concept
	 * 
	 * @param conceptName
	 * @param engineName
	 *            filter for the properties
	 * @return
	 */
	public static List<String> getAllConceptProperties(String engineName) {
		// get the bindings based on the input list
		if (engineName == null || engineName.isEmpty()) {
			throw new IllegalArgumentException("Must define engineName");
		}
		String propQuery = "select distinct ec.physicalname, ec.property "
				+ "from engineconcept ec, concept c, engine e " + " WHERE e.enginename= '" + engineName + "' "
				+ " and ec.engine=e.id and c.localconceptid=ec.localconceptid order by ec.property";
		RDBMSNativeEngine engine = (RDBMSNativeEngine) Utility.getEngine(Constants.LOCAL_MASTER_DB_NAME);
		Connection conn = engine.makeConnection();
		Statement stmt = null;
		ResultSet rs = null;
		List<String> properties = new ArrayList<String>();
		try {
			stmt = conn.createStatement();
			rs = stmt.executeQuery(propQuery);
			while (rs.next()) {
				String propName = rs.getString(1);
				properties.add(propName);
			}
		} catch (SQLException e) {
			e.printStackTrace();
		} finally {
			closeStreams(stmt, rs);
		}
		return properties;
	}

	/**
	 * Maps concepts from sourceDB to concepts in targetDB linked by logical
	 * names
	 * 
	 * @param sourceDB
	 * @param concepts
	 *            optional: specify concepts or get all concepts
	 * @param targetDB
	 * @return map with sourceDB concept - target DB concept
	 */
	public static Map<String, Object> getConceptMapping(String sourceDB, List<String> concepts, String targetDB) {
		Map<String, Object> map = new HashMap<String, Object>();
		List<String> conceptualName = null;
		// map specified concepts
		if (!(concepts == null) && !concepts.isEmpty()) {
			conceptualName = concepts;
		} else {
			// get all concepts
			conceptualName = getAllConceptProperties(sourceDB);
		}
		for (String concept : conceptualName) {
			List<String> propeties = new Vector<>();
			propeties.add(concept);
			List<String> logicalNames = getAllLogicalNamesFromConceptualRDBMS(propeties, sourceDB);
			for (String logicalName : logicalNames) {
				List<String> conceptuals = getConceptualNameFromLogical(targetDB, logicalName);
				if (conceptuals != null && !conceptuals.isEmpty()) {
					map.put(concept, conceptuals);
				}
			}
		}
		return map;
	}
	
	/**
	 * Get map of physical to conceptual name for an engine
	 * 
	 * @param physicalNames
	 * @param engineFilter
	 * @return
	 */
	public static Map<String, String> getConceptualFromPhysical(List<String> physicalNames, String engineFilter) {
		RDBMSNativeEngine engine = (RDBMSNativeEngine) Utility.getEngine(Constants.LOCAL_MASTER_DB_NAME);
		Connection conn = engine.makeConnection();
		// select logicalname from concept where conceptualname='MovieBudget'
		// and conceptualname != logicalname
		// select distinct c.conceptualname, ec.physicalname from concept c,
		// engineconcept ec, engine e where ec.localconceptid=c.localconceptid
		// and ec.physicalname in ('Title', 'Actor');
		Map<String, String> physicalConceptMap = new HashMap<>();
		String physicalList = makeListToString(physicalNames);
		ResultSet rs = null;
		Statement stmt = null;
		try {
			String query = "select distinct ec.physicalname, c.conceptualname "
					+ "from engineconcept ec, concept c, engine e where ec.localconceptid=c.localconceptid "
					+ " and e.enginename = '" + engineFilter + "' " + "and ec.engine=e.id " + "and ec.physicalname in "
					+ physicalList + ";";
			stmt = conn.createStatement();
			rs = stmt.executeQuery(query);
			while (rs.next()) {
				String physical = rs.getString(1);
				String concept = rs.getString(2);
				physicalConceptMap.put(physical, concept);
			}
		} catch (Exception ex) {
			ex.printStackTrace();
		} finally {
			closeStreams(stmt, rs);
		}
		return physicalConceptMap;
	}
	
	public static void main(String[] args) {
		TestUtilityMethods.loadDIHelper("C:\\workspace\\Semoss_Dev\\RDF_Map.prop");
		String engineProp = "C:\\workspace\\Semoss_Dev\\db\\LocalMasterDatabase.smss";
		IEngine coreEngine = new RDBMSNativeEngine();
		coreEngine.setEngineName(Constants.LOCAL_MASTER_DB_NAME);
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