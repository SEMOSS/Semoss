package prerna.nameserver.utility;

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

import prerna.engine.impl.rdbms.RDBMSNativeEngine;
import prerna.util.Constants;
import prerna.util.Utility;

/**
 * @author pkapaleeswaran
 *
 */
public class MasterDatabaseUtility {

	// -----------------------------------------   RDBMS CALLS ---------------------------------------
	
	/**
	 * Return all the logical names for a given conceptual name
	 * @param conceptualName
	 * @return
	 */
	public static List<String> getAllLogicalNamesFromConceptualRDBMS(List<String> conceptualName) {
		RDBMSNativeEngine engine = (RDBMSNativeEngine) Utility.getEngine(Constants.LOCAL_MASTER_DB_NAME);
		Connection conn = engine.makeConnection();
		
		//select logicalname from concept where conceptualname='MovieBudget' and conceptualname != logicalname
		//select distinct c.conceptualname, ec.physicalname from concept c, engineconcept ec, engine e where ec.localconceptid=c.localconceptid and ec.physicalname in ('Title', 'Actor');
		
		String conceptList = makeListToString(conceptualName);
		List <String> logicalNames = new ArrayList<String>();
		ResultSet rs = null;
		Statement stmt = null;
		try {
			String logicalQuery = "select distinct c.logicalname, ec.physicalname from "
								+ "concept c, engineconcept ec, engine e where ec.localconceptid=c.localconceptid and "
								+ "c.conceptualname in " + conceptList + " order by c.logicalname";
			stmt = conn.createStatement();
			rs = stmt.executeQuery(logicalQuery);
			while(rs.next()) {
				String logicalName = rs.getString(1);
				logicalNames.add(logicalName);
			}
		} catch(Exception ex) {
			ex.printStackTrace();
		} finally {
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
		return logicalNames;
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
			try {
				rs.close();
			} catch (SQLException e) {
				e.printStackTrace();
			}
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
				String fromId = rs.getString(1);
				String toId = rs.getString(2);
				
				Hashtable newEdge = new Hashtable();
				// need to check to see if the idHash has it else put it in
				String sourceName = idHash.get(toId);
				String targetName = idHash.get(fromId);
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
					edgeHash.put(fromId + "-" + toId, newEdge);
				}
			}
			finalHash.put("nodes", nodeHash.values().toArray());
			finalHash.put("edges", edgeHash.values().toArray());
			
		} catch(SQLException ex) {
			ex.printStackTrace();
		} finally {
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
		
		return finalHash;
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

		String conceptMasterQuery = "select e.enginename, ec.engine, c.logicalname, ec.physicalnameid, ec.physicalname from concept c, engineconcept ec, engine e where "
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
			try {
				if(rs != null) {
					rs.close();						
				}
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}


		//select distinct  e.enginename, 'Title', 'downstream' as downstream,  er.relationname,  c.logicalname , er.engine, er.targetconceptid, ec.physicalname from enginerelation er, engineconcept ec, concept c, engine e where er.sourceconceptid in (select physicalnameid from engineconcept where localconceptid in (select localconceptid from concept where logicalname in ('Title'))) 
		//and ec.physicalnameid=er.targetconceptid and c.localconceptid=ec.localconceptid and e.id=er.engine;

		// now time to run the upstream and downstream queries
		String downstreamQuery = "select distinct  e.enginename, er.sourceconceptid, 'downstream' as downstream,  "
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

				if(conceptSpecific.containsKey("downstream"))
					downstreams = (Set<String>)conceptSpecific.get("downstream");
				downstreams.add(streamConceptName);
				if(conceptSpecific.containsKey("physical"))
					physicalNames = (Set<String>)conceptSpecific.get("physical");
				physicalNames.add(streamPhysicalName);
				conceptSpecific.put("downstream", downstreams);
				conceptSpecific.put("physical", physicalNames);
				engineSpecific.put(coreConceptName, conceptSpecific);
				retMap.put(engineName, engineSpecific);
			}
		} catch(SQLException e) {
			e.printStackTrace();
		} finally {
			try {
				if(rs != null) {
					rs.close();
				}
			} catch(SQLException e) {
				e.printStackTrace();
			}
		}

		// now time to run the upstream and downstream queries
		String upstreamQuery = "select distinct  e.enginename, er.targetconceptid, 'upstream' as upstream,  "
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

				if(conceptSpecific.containsKey("upstream"))
					upstreams = (Set<String>)conceptSpecific.get("upstream");
				upstreams.add(streamConceptName);
				if(conceptSpecific.containsKey("physical"))
					physicalNames = (Set<String>)conceptSpecific.get("physical");
				physicalNames.add(streamPhysicalName);
				conceptSpecific.put("upstream", upstreams);
				conceptSpecific.put("physical", physicalNames);
				engineSpecific.put(coreConceptName, conceptSpecific);
				retMap.put(engineName, engineSpecific);
			}
		} catch(SQLException ex) {
			ex.printStackTrace();
		} finally {
			try {
				rs.close();
			} catch (SQLException e) {
				e.printStackTrace();
			}
			try {
				stmt.close();
			} catch (SQLException e) {
				e.printStackTrace();
			}
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
				String conceptQuery = "select distinct e.enginename from engine e"; //, c.logicalname, ec.physicalname from concept c, engineconcept ec, engine e where ec.localconceptid=c.localconceptid and e.id=ec.engine";
				stmt = conn.createStatement();
				rs = stmt.executeQuery(conceptQuery);
				while(rs.next()) {
					String engineName = rs.getString(1);
					retList.add(engineName);
				}		
			} catch(SQLException ex) {
				ex.printStackTrace();
			} finally {
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
		
		return null;
	}
	
	/**
	 * Adds logical name to concept from engine
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
					+ "and conceptualname = \'" + concept + "\' and logicalname = \'"+logicalName+"\';";
			stmt = masterConn.createStatement();
			rs = stmt.executeQuery(duplicateQueryCheck);
			if (rs != null) {
				rs.beforeFirst();
				rs.last();
				size = rs.getRow();
			}
		} catch(SQLException e) {
			e.printStackTrace();
		} finally {
			try {
				if(rs != null) {
					rs.close();
				}
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
		try {
			if (size == 0) {
				String sourceLogicalInfo = "select localconceptid, conceptualname, logicalname, "
						+ "domainname, globalid from concept "
						+ "where localconceptid in (select localconceptid from engineconcept "
						+ "where engine in (select id from engine where enginename = \'" + engineName + "\')) "
						+ "and conceptualname = \'" + concept + "\';";
				if(stmt == null) {
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
			}
			else {
				return true;
			}
		} catch (SQLException e) {
			e.printStackTrace();
		} finally {
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
		return false;
	}
	
	/**
	 * Removes logical name for a concept from an engine
	 * @param engineName
	 * @param concept
	 * @param logicalName
	 * @return success
	 */
	public static boolean removeLogicalName(String engineName, String concept, String logicalName) {
		RDBMSNativeEngine engine = (RDBMSNativeEngine) Utility.getEngine(Constants.LOCAL_MASTER_DB_NAME);
		Connection masterConn = engine.makeConnection();
		ResultSet rs = null;
		
		try {
			String deleteQuery = "delete from concept "
					+ "where localconceptid in (select localconceptid from engineconcept "
					+ "where engine in (select id from engine where enginename = \'" + engineName + "\')) "
					+ "and conceptualname = \'" + concept + "\' and logicalname = \'"+logicalName+"\';";
			int updateCount = masterConn.createStatement().executeUpdate(deleteQuery);
			if(updateCount == 1) {
				return true;
			}
		} catch(SQLException e) {
			e.printStackTrace();
		} finally {
			try {
				if(rs != null) {
					rs.close();
				}
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
		return false;
	}
	
	/**
	 * Returns Xray config files
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
			//Don't print stack trace... xrayConfigList table is missing if no config file exists
		} finally {
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
		return configMap;

	}
	
	/**
	 * Gets the xray config file
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
		return configFile;
	}
	
	
//	private static enum DIRECTION_KEYS {
//	UPSTREAM ("upstream"), 
//	DOWNSTREAM ("downstream");
//	
//	private final String name;
//	
//	private DIRECTION_KEYS(String name) {
//		this.name = name;
//	}
//	
//	public String toString() {
//		return this.name;
//	}
//};
//
///**
// * Get the list of engines available within SEMOSS
// * @return
// */
//public static List<String> getDatabaseList() {
//	/*
//	 * On start up, we store a string to hold all the engines in DIHelper
//	 * Grab the string from DIHelper and split based on ";" since that is the 
//	 * delimiter used to separate the engine names
//	 * 
//	 * Sometimes, if it starts with a ";" or ends with a ";" the methods split will
//	 * add an empty cell.  Remove start and ending ";" to avoid this
//	 */
//	String allEngines = DIHelper.getInstance().getLocalProp(Constants.ENGINES) + "";
//	if(allEngines.startsWith(";")) {
//		allEngines = allEngines.substring(1);
//	}
//	if(allEngines.endsWith(";")) {
//		allEngines = allEngines.substring(0, allEngines.length()-1);
//	}
//	String[] engines = allEngines.split(";");
//	return Arrays.asList(engines);
//}
//
//public static Map<String, Set<String>> getAllConceptsFromEngines() {
//	/*
//	 * Grab the local master engine and query for the concepts
//	 * We do not want to load the engine until the user actually wants to use it
//	 */
//	IEngine engine = (IEngine) DIHelper.getInstance().getLocalProp(Constants.LOCAL_MASTER_DB_NAME);
//
//	/*
//	 * The return structure should be the following:
//	 * 
//	 * { engineName1 -> [conceptualConceptName1, conceptualConceptName2, ...],
//	 * 	 engineName2 -> [conceptualConceptName1, conceptualConceptName2, ...],
//	 * 	 ...
//	 * }
//	 */
//	Map<String, Set<String>> returnHash = new TreeMap<String, Set<String>>();
//
//	String query = "SELECT DISTINCT ?engine ?conceptConceptual WHERE { "
//			+ "{?conceptComposite <" + RDFS.subClassOf + "> <http://semoss.org/ontologies/Concept>}"
//			+ "{?conceptComposite <http://semoss.org/ontologies/Relation/presentin> ?engine}"
//			+ "{?conceptComposite <http://semoss.org/ontologies/Relation/conceptual> ?conceptConceptual}"
//			+ "} ORDER BY ?engine";
//
//	// iterate through the results and append onto the list
//	IRawSelectWrapper wrapper = WrapperManager.getInstance().getRawWrapper(engine, query);
//	while(wrapper.hasNext()) {
//		IHeadersDataRow ss = wrapper.next();
//		Object[] values = ss.getValues();
//		
//		// grab values
//		String engineName = values[0] + "";
//		String conceptConceptual = values[1] + "";
//		
//		Set<String> conceptSet = null;
//		if(returnHash.containsKey(engineName)) {
//			conceptSet = returnHash.get(engineName);
//		} else {
//			conceptSet = new TreeSet<String>();
//			// add the concept set to the return map
//			returnHash.put(engineName, conceptSet);
//		}
//		
//		conceptSet.add(conceptConceptual);
//	}
//	
//	return returnHash;
//}
//
///**
// * Return all the logical names for a given conceptual name
// * @param conceptualName
// * @return
// */
//public static Set<String> getAllLogicalNamesFromConceptual(String conceptualName, String parentConceptualName) {
//	IEngine engine = (IEngine) DIHelper.getInstance().getLocalProp(Constants.LOCAL_MASTER_DB_NAME);
//
//	String conceptualUri = null;;
//	if(parentConceptualName != null && !parentConceptualName.trim().isEmpty()) {
//		conceptualUri = "http://semoss.org/ontologies/Relation/Contains/" + conceptualName + "/" + parentConceptualName;
//	} else {
//		conceptualUri = "http://semoss.org/ontologies/Concept/" + conceptualName;
//	}
//	
//	String query = "SELECT DISTINCT ?logical WHERE { "
//			+ "{?composite <http://semoss.org/ontologies/Relation/logical> ?logical}"
//			+ "{?composite <http://semoss.org/ontologies/Relation/conceptual> <" + conceptualUri + ">}"
//			+ "} ORDER BY ?logical";
//	
//	Set<String> logicalNames = new TreeSet<String>();
//
//	// iterate through the results and append onto the list
//	IRawSelectWrapper wrapper = WrapperManager.getInstance().getRawWrapper(engine, query);
//	while(wrapper.hasNext()) {
//		IHeadersDataRow ss = wrapper.next();
//		Object[] values = ss.getValues();
//		// this will be the logical names
//		logicalNames.add(values[0] + "");
//	}
//	
//	return logicalNames;
//}
//
///**
// * Return all the logical names for a given conceptual name
// * @param conceptualName
// * @return
// */
//public static Set<String> getAllLogicalNamesFromConceptual(List<String> conceptualName, List<String> parentConceptualName) {
//	IEngine engine = (IEngine) DIHelper.getInstance().getLocalProp(Constants.LOCAL_MASTER_DB_NAME);
//
//	// create list of bindings
//	List<String> conceptualUriSuffix = new Vector<String>();
//	int size = conceptualName.size();
//	for(int i = 0; i < size; i++) {
//		String uriSuffix = conceptualName.get(i);
//		if(parentConceptualName != null && parentConceptualName.get(i) != null && !parentConceptualName.get(i).trim().isEmpty()) {
//			uriSuffix += "/" + parentConceptualName.get(i);
//		}
//		// add to list
//		conceptualUriSuffix.add(uriSuffix);
//	}
//	
//	StringBuilder bindings = createUriBindings("conceptualUri", conceptualUriSuffix);
//	
//	String query = "SELECT DISTINCT ?logical WHERE { "
//			+ "{?composite <http://semoss.org/ontologies/Relation/logical> ?logical}"
//			+ "{?composite <http://semoss.org/ontologies/Relation/conceptual> ?conceptualUri}"
//			+ "} ORDER BY ?logical " + bindings.toString();
//	
//	Set<String> logicalNames = new TreeSet<String>();
//
//	// iterate through the results and append onto the list
//	IRawSelectWrapper wrapper = WrapperManager.getInstance().getRawWrapper(engine, query);
//	while(wrapper.hasNext()) {
//		IHeadersDataRow ss = wrapper.next();
//		String uri = ss.getRawValues()[0].toString();
//		if(uri.contains("http://semoss.org/ontologies/Relation/Contains/")) {
//			uri = uri.replace("http://semoss.org/ontologies/Relation/Contains/", "");
//			if(uri.contains("/")) {
//				uri = uri.substring(0, uri.indexOf("/"));
//			}
//		} else {
//			uri = ss.getValues()[0].toString();
//		}
//		// this will be the logical names
//		logicalNames.add(uri + "");
//	}
//	
//	return logicalNames;
//}
//
//
///**
// * Get the list of concepts for a given engine
// * @param engineName
// * @return
// */
//public static Set<String> getConceptsWithinEngine(String engineName) {
//	/*
//	 * Grab the local master engine and query for the concepts
//	 * We do not want to load the engine until the user actually wants to use it
//	 * 
//	 */
//	IEngine engine = (IEngine) DIHelper.getInstance().getLocalProp(Constants.LOCAL_MASTER_DB_NAME);
//
//	Set<String> conceptsList = new TreeSet<String>();
//
//	String query = "SELECT DISTINCT ?conceptConceptual WHERE { "
//			+ "{?conceptComposite <http://semoss.org/ontologies/Relation/presentin> <http://semoss.org/ontologies/meta/engine/" + engineName + ">}"
//			+ "{?conceptComposite <http://semoss.org/ontologies/Relation/conceptual> ?conceptConceptual}"
//			+ "} ORDER BY ?conceptConceptual";
//
//	// iterate through the results and append onto the list
//	IRawSelectWrapper wrapper = WrapperManager.getInstance().getRawWrapper(engine, query);
//	while(wrapper.hasNext()) {
//		IHeadersDataRow ss = wrapper.next();
//		Object[] values = ss.getValues();
//		// this will be the logical names
//		conceptsList.add(values[0] + "");
//	}
//	
//	return conceptsList;
//}
//
//
//
///**
// * Get the properties for a given concept across all the databases
// * @param conceptName
// * @param engineName		optional filter for the properties
// * @return
// */
//public static Map<String, Object[]> getConceptProperties(List<String> conceptLogicalNames, String engineFilter) {
//	// get the bindings based on the input list
//	StringBuilder logicalNameBindings = createUriBindings("conceptLogical", conceptLogicalNames);
//
//	String propQuery = "SELECT DISTINCT ?engine ?conceptConceptual ?propConceptual WHERE {";
//	if(engineFilter != null && !engineFilter.isEmpty()) {
//		propQuery += "BIND(<http://semoss.org/ontologies/meta/engine/" + engineFilter + "> AS ?engine) ";
//	}
//	propQuery += "{?conceptComposite <" + RDFS.subClassOf + "> <http://semoss.org/ontologies/Concept>}"
//			+ "{?conceptComposite <" + RDF.TYPE + "> ?concept}"
//			+ "{?conceptComposite <http://semoss.org/ontologies/Relation/logical> ?conceptLogical}"
//			+ "{?conceptComposite <http://semoss.org/ontologies/Relation/conceptual> ?conceptConceptual}"
//
//				+ "{?conceptComposite <" + OWL.DATATYPEPROPERTY + "> ?propComposite}"
//				+ "{?propComposite <" + RDF.TYPE + "> ?conceptProp}"
//				+ "{?propComposite <http://semoss.org/ontologies/Relation/presentin> ?engine}"
//				+ "{?propComposite <http://semoss.org/ontologies/Relation/conceptual> ?propConceptual}"
//
//				+ "FILTER(?concept != <http://semoss.org/ontologies/Concept> "
//				+ " && ?concept != <" + RDFS.Class + "> "
//				+ " && ?concept != <" + RDFS.Resource + "> "
//				+ " && ?conceptProp != <http://www.w3.org/2000/01/rdf-schema#Resource>"
//				+ ")"
//				+ "}"
//				+ logicalNameBindings.toString();
//
//	Map<String, Map<String, MetamodelVertex>> queryData = new TreeMap<String, Map<String, MetamodelVertex>>();
//
//	IEngine engine = (IEngine) DIHelper.getInstance().getLocalProp(Constants.LOCAL_MASTER_DB_NAME);
//	IRawSelectWrapper wrapper = WrapperManager.getInstance().getRawWrapper(engine, propQuery);
//	while(wrapper.hasNext())
//	{
//		IHeadersDataRow stmt = wrapper.next();
//		Object[] values = stmt.getValues();
//		// we need the raw values for properties since we need to get the "class name"
//		// as they are all defined (regardless of db type) as propName/parentName
//		Object[] rawValues = stmt.getRawValues();
//		
//		// grab values
//		String engineName = values[0] + "";
//		String conceptConceptual = values[1] + "";
//		String propConceptual = Utility.getClassName(rawValues[2] + "");
//		
//		Map<String, MetamodelVertex> engineMap = null;
//		if(queryData.containsKey(engineName)) {
//			engineMap = queryData.get(engineName);
//		} else {
//			engineMap = new TreeMap<String, MetamodelVertex>();
//			// add to query data map
//			queryData.put(engineName, engineMap);
//		}
//		
//		// get or create the vertex
//		MetamodelVertex vert = null;
//		if(engineMap.containsKey(conceptConceptual)) {
//			vert = engineMap.get(conceptConceptual);
//		} else {
//			vert = new MetamodelVertex(conceptConceptual);
//			// add to the engine map
//			engineMap.put(conceptConceptual, vert);
//		}
//		
//		// add the property conceptual name
//		vert.addProperty(propConceptual);
//	}
//	
//	Map<String, Object[]> returnHash = new TreeMap<String, Object[]>();
//	for(String engineName : queryData.keySet()) {
//		returnHash.put(engineName, queryData.get(engineName).values().toArray());
//	}
//	
//	return returnHash;
//}
//
//
//
//
///**
// * Get the list of  connected concepts for a given concept
// * 
// * Direction upstream/downstream is always in reference to the node being searched
// * For example, if the relationship in the direction Title -> Genre
// * The result would be { upstream -> [Genre] } because Title is upstream of Genre
// * 
// * @param conceptType
// * @return
// */
//public static Map getConnectedConcepts(List<String> conceptLogicalNames) {
//	// get the bindings based on the input list
//	StringBuilder logicalNameBindings = createUriBindings("toLogical", conceptLogicalNames);
//	
//	IEngine masterDB = (IEngine) DIHelper.getInstance().getLocalProp(Constants.LOCAL_MASTER_DB_NAME);
//
//	// this is the final return object
//	Map<String, Map> retMap = new TreeMap<String, Map>();
//	
//	String upstreamQuery = "SELECT DISTINCT ?someEngine ?fromConceptual ?toConceptual WHERE {"
//			+ "{?fromConceptComposite <" + RDFS.subClassOf + "> <http://semoss.org/ontologies/Concept>}"
//			+ "{?toConceptComposite <" + RDFS.subClassOf + "> <http://semoss.org/ontologies/Concept>}"
//			+ "{?fromConcept <" + RDFS.subClassOf + "> <http://semoss.org/ontologies/Concept>}"
//			+ "{?toConcept <" + RDFS.subClassOf + "> <http://semoss.org/ontologies/Concept>}"
//
//			+ "{?fromConceptComposite <http://semoss.org/ontologies/Relation/presentin> ?someEngine}"
//			+ "{?toConceptComposite <http://semoss.org/ontologies/Relation/presentin> ?someEngine}"
//			
//			+ "{?toConceptComposite <" + RDF.TYPE + "> ?toConcept}"
//			+ "{?toConceptComposite <http://semoss.org/ontologies/Relation/logical> ?toLogical}"
//			+ "{?toConceptComposite <http://semoss.org/ontologies/Relation/conceptual> ?toConceptual}"
//
//			+ "{?fromConceptComposite <" + RDF.TYPE + "> ?fromConcept}"
//			+ "{?fromConceptComposite <http://semoss.org/ontologies/Relation/conceptual> ?fromConceptual}"
//			
//			+ "{?someRel <" + RDFS.subPropertyOf + "> <http://semoss.org/ontologies/Relation>}"
//			+ "{?toConceptComposite ?someRel ?fromConceptComposite}"
//			
//			+ "FILTER(?fromConcept != <http://semoss.org/ontologies/Concept> "
//			+ "&& ?toConcept != <http://semoss.org/ontologies/Concept>"
//			+ "&& ?someRel != <http://semoss.org/ontologies/Relation>"
//			+ "&& ?toConcept != ?someEngine)"
//			+ "}"
//			+ logicalNameBindings;
//	
//	retMap = assimilateNodes(upstreamQuery, masterDB, retMap, DIRECTION_KEYS.UPSTREAM);
//	
//	// this query is identical except the from and to in the relationship triple are switched
//	String downstreamQuery = "SELECT DISTINCT ?someEngine ?fromConceptual ?toConceptual WHERE {"
//			+ "{?fromConceptComposite <" + RDFS.subClassOf + "> <http://semoss.org/ontologies/Concept>}"
//			+ "{?toConceptComposite <" + RDFS.subClassOf + "> <http://semoss.org/ontologies/Concept>}"
//			+ "{?fromConcept <" + RDFS.subClassOf + "> <http://semoss.org/ontologies/Concept>}"
//			+ "{?toConcept <" + RDFS.subClassOf + "> <http://semoss.org/ontologies/Concept>}"
//
//			+ "{?fromConceptComposite <http://semoss.org/ontologies/Relation/presentin> ?someEngine}"
//			+ "{?toConceptComposite <http://semoss.org/ontologies/Relation/presentin> ?someEngine}"
//			
//			+ "{?toConceptComposite <" + RDF.TYPE + "> ?toConcept}"
//			+ "{?toConceptComposite <http://semoss.org/ontologies/Relation/logical> ?toLogical}"
//			+ "{?toConceptComposite <http://semoss.org/ontologies/Relation/conceptual> ?toConceptual}"
//
//			+ "{?fromConceptComposite <" + RDF.TYPE + "> ?fromConcept}"
//			+ "{?fromConceptComposite <http://semoss.org/ontologies/Relation/conceptual> ?fromConceptual}"
//			
//			+ "{?someRel <" + RDFS.subPropertyOf + "> <http://semoss.org/ontologies/Relation>}"
//			+ "{?fromConceptComposite ?someRel ?toConceptComposite}"
//			
//			+ "FILTER(?fromConcept != <http://semoss.org/ontologies/Concept> "
//			+ "&& ?toConcept != <http://semoss.org/ontologies/Concept>"
//			+ "&& ?someRel != <http://semoss.org/ontologies/Relation>"
//			+ "&& ?toConcept != ?someEngine)"
//			+ "}"
//			+ logicalNameBindings;
//	
//	retMap = assimilateNodes(downstreamQuery, masterDB, retMap, DIRECTION_KEYS.DOWNSTREAM);		
//	return retMap;
//}
//
//private static Map assimilateNodes(String query, IEngine engine, Map<String, Map> enginesHash, DIRECTION_KEYS direction)
//{
//	IRawSelectWrapper wrapper = WrapperManager.getInstance().getRawWrapper(engine, query);
//	while(wrapper.hasNext())
//	{
//		// given the structure of the query
//		// the first output is the engine
//		// the second output is the node to traverse to
//		// stream key tells us the direction
//		// equivalent concept tells us the start concept since this is across databases
//		
//		IHeadersDataRow stmt = wrapper.next();
//		Object[] values = stmt.getValues();
//				
//		String engineName = values[0] + "";
//		String traverseConceptual = values[1] + "";
//		String equivConceptual = values[2] + "";
//
//		// grab existing hash for the engine if it exists
//		Map<String, Object> thisEngineHash = null;
//		if(enginesHash.containsKey(engineName)) {
//			thisEngineHash = enginesHash.get(engineName);
//		} else {
//			thisEngineHash = new TreeMap<String, Object>();
//			// add engine to overall map
//			enginesHash.put(engineName,  thisEngineHash);
//		}
//		
//		// within each engine
//		// we need a separate map for each equivalent concept name
//		// that gets mapped since we are searching using logical names
//		// which doesn't guarantee uniqueness of the conceptual name within an engine
//		
//		Map<String, Object> equivConceptHash = null;
//		if(thisEngineHash.containsKey(equivConceptual)) {
//			equivConceptHash = (Map<String, Object>) thisEngineHash.get(equivConceptual);
//		} else {
//			equivConceptHash = new Hashtable<String, Object>();
//			// add equiv concept to this engine hash
//			thisEngineHash.put(equivConceptual, equivConceptHash);
//		}
//		
//		// now for this equivalent conceptual
//		// need to add the traverse in the appropriate upstream/downstream direction
//		Set<String> directionList = null;
//		if(equivConceptHash.containsKey(direction.toString())) {
//			directionList = (Set<String>) equivConceptHash.get(direction.toString());
//		} else {
//			directionList = new TreeSet<String>();
//			// add direction to equivalent conceptual map
//			equivConceptHash.put(direction.toString(), directionList);
//		}
//		
//		// add the conceptual name to the list for the FE to show
//		directionList.add(traverseConceptual); 
//	}
//	
//	return enginesHash;
//}
//
//private static StringBuilder createUriBindings(String varName, List<String> nameBindings) {
//	StringBuilder logicalNameBindings = new StringBuilder(" BINDINGS ?" + varName + " {");
//	int size = nameBindings.size();
//	for(int i = 0; i < size; i++) {
//		String binding = nameBindings.get(i);
//		// we need to distinguish between a node and a property
//		if(binding.contains("/")) {
//			logicalNameBindings.append("(<http://semoss.org/ontologies/Relation/Contains/").append(binding).append(">)");
//		} else {
//			logicalNameBindings.append("(<http://semoss.org/ontologies/Concept/").append(binding).append(">)");
//		}
//	}
//	logicalNameBindings.append("}");
//	return logicalNameBindings;
//}
//
//
//public static Map<String, Object[]> getMetamodel(String engineName)
//{
//	// this needs to be moved to the name server
//	// and this needs to be based on local master database
//	// need this to be a simple OWL data
//	// I dont know if it is worth it to load the engine at this point ?
//	// or should I just load it ?
//	// need to get local master and pump out the metamodel
//	
//	// final map to return
//	// TODO: outline the structure
//	Map<String, Map> edgeAndVertex = new Hashtable<String, Map>();
//	
//	IEngine engine = (IEngine)DIHelper.getInstance().getLocalProp(Constants.LOCAL_MASTER_DB_NAME);
//	
//	// query to get all the concepts, their properties, and their keys
//	String vertexQuery = "SELECT DISTINCT ?conceptConceptual (COALESCE(?propConceptual, ?noprop) as ?propConceptual) (COALESCE(?keyConceptual, ?nokey) as ?keyConceptual) "
//			+ " WHERE "
//			+ "{BIND(<http://semoss.org/ontologies/Relation/contains/noprop/noprop> AS ?noprop)"
//			
//			+ "BIND(<http://semoss.org/ontologies/Relation/contains/nokey/nokey> AS ?nokey)"
//			
//			+ "{?conceptComposite <" + RDFS.subClassOf + "> <http://semoss.org/ontologies/Concept>}"
//			+ "{?conceptComposite <http://semoss.org/ontologies/Relation/presentin> <http://semoss.org/ontologies/meta/engine/" + engineName + ">}"
//			
//			+ "{?conceptComposite <" + RDF.TYPE + "> ?concept}"
//			+ "{?conceptComposite <http://semoss.org/ontologies/Relation/conceptual> ?conceptConceptual}"
//			
//			+ "OPTIONAL{"
//			+ "{?conceptComposite <" + OWL.DATATYPEPROPERTY + "> ?propComposite}"
//			+ "{?propComposite <" + RDF.TYPE + "> ?prop}"
//			+ "{?propComposite <http://semoss.org/ontologies/Relation/conceptual> ?propConceptual}"
//			+ "}"
//			
//			+ "OPTIONAL{"
//			+ "{?conceptComposite <" + Constants.META_KEY + "> ?keyComposite}"
//			+ "{?keyComposite <http://semoss.org/ontologies/Relation/conceptual> ?keyConceptual}"				
//			+ "}"
//			
//			+ "FILTER(?concept != <http://semoss.org/ontologies/Concept> "
//			+ " && ?concept != <" + RDFS.Class + "> "
//			+ " && ?concept != <" + RDFS.Resource + "> "
//			+ ")"
//			+ "}";
//	
//	// get all the vertices and their properties
//	makeVertices(engine, vertexQuery, edgeAndVertex);
//	
//	// query to get all the edges between nodes
//	String edgeQuery = "SELECT DISTINCT ?fromConceptual ?toConceptual ?someRel WHERE {"
//			+ "{?conceptComposite <" + RDFS.subClassOf + "> <http://semoss.org/ontologies/Concept>}"
//			+ "{?toConceptComposite <" + RDFS.subClassOf + "> <http://semoss.org/ontologies/Concept>}"
//			
//			+ "{?conceptComposite <http://semoss.org/ontologies/Relation/presentin> <http://semoss.org/ontologies/meta/engine/" + engineName + ">}"
//			+ "{?toConceptComposite <http://semoss.org/ontologies/Relation/presentin> <http://semoss.org/ontologies/meta/engine/" + engineName + ">}"
//			
//			+ "{?conceptComposite <" + RDF.TYPE + "> ?fromConcept}"
//			+ "{?toConceptComposite <"+ RDF.TYPE + "> ?toConcept}"
//			+ "{?conceptComposite ?someRel ?toConceptComposite}"
//
//			+ "{?conceptComposite <http://semoss.org/ontologies/Relation/conceptual> ?fromConceptual}"
//			+ "{?toConceptComposite <http://semoss.org/ontologies/Relation/conceptual> ?toConceptual}"			
//			
//			+ "FILTER(?fromConcept != <http://semoss.org/ontologies/Concept> "
//			+ "&& ?toConcept != <http://semoss.org/ontologies/Concept>"
//			+ "&& ?fromConcept != ?toConcept"
//			+ "&& ?fromConcept != <" + RDFS.Class + "> "
//			+ "&& ?toConcept != <" + RDFS.Class + "> "
//			+ "&& ?fromConcept != <" + RDFS.Resource + "> "
//			+ "&& ?toConcept != <" + RDFS.Resource + "> "
//			+ "&& ?someRel != <http://semoss.org/ontologies/Relation>"
//			+ "&& ?someRel != <" + RDFS.subClassOf + ">)}";
//
//	// make the edges
//	makeEdges(engine, edgeQuery, edgeAndVertex);
//	
//	Map<String, Object[]> finalHash = new Hashtable<String, Object[]>();
//	finalHash.put("nodes", edgeAndVertex.get("nodes").values().toArray());
//	finalHash.put("edges", edgeAndVertex.get("edges").values().toArray());
//
//	// return the map
//	return finalHash;
//}
//
//
//
//public static Map<String, Object[]> getMetamodelSecure(String engineName, HashMap<String, ArrayList<String>> metamodelFilter)
//{
//	// this needs to be moved to the name server
//	// and this needs to be based on local master database
//	// need this to be a simple OWL data
//	// I dont know if it is worth it to load the engine at this point ?
//	// or should I just load it ?
//	// need to get local master and pump out the metamodel
//	
//	// final map to return
//	// TODO: outline the structure
//	Map<String, Map> edgeAndVertex = new Hashtable<String, Map>();
//	
//	IEngine engine = (IEngine)DIHelper.getInstance().getLocalProp(Constants.LOCAL_MASTER_DB_NAME);
//	
//	// query to get all the concepts and their properties
//	String vertexQuery = "SELECT DISTINCT ?conceptConceptual (COALESCE(?propConceptual, ?noprop) as ?propConceptual) ?propLogical"
//			+ " WHERE "
//			+ "{BIND(<http://semoss.org/ontologies/Relation/contains/noprop/noprop> AS ?noprop)"
//			
//			+ "{?conceptComposite <" + RDFS.subClassOf + "> <http://semoss.org/ontologies/Concept>}"
//			+ "{?conceptComposite <http://semoss.org/ontologies/Relation/presentin> <http://semoss.org/ontologies/meta/engine/" + engineName + ">}"
//			
//			+ "{?conceptComposite <" + RDF.TYPE + "> ?concept}"
//			+ "{?conceptComposite <http://semoss.org/ontologies/Relation/conceptual> ?conceptConceptual}"
//			
//			+ "OPTIONAL{"
//			+ "{?conceptComposite <" + OWL.DATATYPEPROPERTY + "> ?propComposite}"
//			+ "{?propComposite <" + RDF.TYPE + "> ?prop}"
//			+ "{?propComposite <http://semoss.org/ontologies/Relation/conceptual> ?propConceptual}"
//			+ "{?propComposite <http://semoss.org/ontologies/Relation/logical> ?propLogical}"
//			+ "}"
//			
//			+ "FILTER(?concept != <http://semoss.org/ontologies/Concept> "
//			+ " && ?concept != <" + RDFS.Class + "> "
//			+ " && ?concept != <" + RDFS.Resource + "> "
//			+ ")"
//			+ "}";
//
//	// get all the vertices and their properties
//	makeVerticesSecure(engine, vertexQuery, edgeAndVertex, metamodelFilter);
//	
//	// query to get all the edges between nodes
//	String edgeQuery = "SELECT DISTINCT ?fromConceptual ?toConceptual ?someRel WHERE {"
//			+ "{?conceptComposite <" + RDFS.subClassOf + "> <http://semoss.org/ontologies/Concept>}"
//			+ "{?toConceptComposite <" + RDFS.subClassOf + "> <http://semoss.org/ontologies/Concept>}"
//			
//			+ "{?conceptComposite <http://semoss.org/ontologies/Relation/presentin> <http://semoss.org/ontologies/meta/engine/" + engineName + ">}"
//			+ "{?toConceptComposite <http://semoss.org/ontologies/Relation/presentin> <http://semoss.org/ontologies/meta/engine/" + engineName + ">}"
//			
//			+ "{?conceptComposite <" + RDF.TYPE + "> ?fromConcept}"
//			+ "{?toConceptComposite <"+ RDF.TYPE + "> ?toConcept}"
//			+ "{?conceptComposite ?someRel ?toConceptComposite}"
//
//			+ "{?conceptComposite <http://semoss.org/ontologies/Relation/conceptual> ?fromConceptual}"
//			+ "{?toConceptComposite <http://semoss.org/ontologies/Relation/conceptual> ?toConceptual}"			
//			
//			+ "FILTER(?fromConcept != <http://semoss.org/ontologies/Concept> "
//			+ "&& ?toConcept != <http://semoss.org/ontologies/Concept>"
//			+ "&& ?fromConcept != ?toConcept"
//			+ "&& ?fromConcept != <" + RDFS.Class + "> "
//			+ "&& ?toConcept != <" + RDFS.Class + "> "
//			+ "&& ?fromConcept != <" + RDFS.Resource + "> "
//			+ "&& ?toConcept != <" + RDFS.Resource + "> "
//			+ "&& ?someRel != <http://semoss.org/ontologies/Relation>"
//			+ "&& ?someRel != <" + RDFS.subClassOf + ">)}";
//
//	// make the edges
//	makeEdgesSecure(engine, edgeQuery, edgeAndVertex, metamodelFilter.keySet());
//	
//	Map<String, Object[]> finalHash = new Hashtable<String, Object[]>();
//	finalHash.put("nodes", edgeAndVertex.get("nodes").values().toArray());
//	finalHash.put("edges", edgeAndVertex.get("edges").values().toArray());
//
//	// return the map
//	return finalHash;
//}
//
//private static void makeVertices(IEngine engine, String query, Map<String, Map> edgesAndVertices)
//{		
//	Map<String, MetamodelVertex> nodes = new TreeMap<String, MetamodelVertex>();
//	if(edgesAndVertices.containsKey("nodes")) {
//		nodes = (Map) edgesAndVertices.get("nodes");
//	}
//	
//	IRawSelectWrapper wrapper = WrapperManager.getInstance().getRawWrapper(engine, query);
//	while(wrapper.hasNext())
//	{
//		// based on query being passed in
//		// only 3 values in array
//		// first output is the concept conceptual name
//		// second output is the property conceptual name
//		// third output is the key conceptual name
//		IHeadersDataRow stmt = wrapper.next();
//		Object[] values = stmt.getValues();
//		// we need the raw values for properties since we need to get the "class name"
//		// as they are all defined (regardless of db type) as propName/parentName
//		Object[] rawValues = stmt.getRawValues();
//		
//		String conceptualConceptName = values[0] + "";
//		String concpetualPropertyName = Utility.getClassName(rawValues[1] + "");
//		String conceptualKeyName = Utility.getClassName(rawValues[2] + "");
//		
//		// get or create the metamodel vertex
//		MetamodelVertex node = null;
//		if(nodes.containsKey(conceptualConceptName)) {
//			node = nodes.get(conceptualConceptName);
//		} else {
//			node = new MetamodelVertex(conceptualConceptName);
//			// add to the map of nodes
//			nodes.put(conceptualConceptName, node);
//		}
//		
//		// add the property 
//		node.addProperty(concpetualPropertyName);
//		
//		// add the key
//		node.addKey(conceptualKeyName);
//	}
//	
//	// add all the nodes back into the main map
//	edgesAndVertices.put("nodes", nodes);
//}
//
//private static void makeVerticesSecure(IEngine engine, String query, Map<String, Map> edgesAndVertices, HashMap<String, ArrayList<String>> metamodelFilter)
//{
//	Set<String> conceptFilter = new HashSet<String>();
//	if(metamodelFilter!= null && !metamodelFilter.isEmpty()) {
//		conceptFilter = metamodelFilter.keySet();
//	}
//	
//	Map<String, MetamodelVertex> nodes = new TreeMap<String, MetamodelVertex>();
//	if(edgesAndVertices.containsKey("nodes")) {
//		nodes = (Map) edgesAndVertices.get("nodes");
//	}
//	
//	IRawSelectWrapper wrapper = WrapperManager.getInstance().getRawWrapper(engine, query);
//	while(wrapper.hasNext())
//	{
//		// based on query being passed in
//		// only 2 values in array
//		// first output is the concept conceptual name
//		// second output is the property conceptual name
//		IHeadersDataRow stmt = wrapper.next();
//		Object[] values = stmt.getValues();
//		// we need the raw values for properties since we need to get the "class name"
//		// as they are all defined (regardless of db type) as propName/parentNamee
//		Object[] rawValues = stmt.getRawValues();
//		
//		String conceptualConceptName = values[0] + "";
//		String concpetualPropertyName = Utility.getClassName(rawValues[1] + "");
//		
//		// Check the concepts and props against the metamodel filter that is set
//		// TODO: Better way to represent/check if it's actual node value or a prop rather than checking equality to "noprop"
//		if(conceptFilter != null && !conceptFilter.isEmpty()) {
//			if(!conceptFilter.contains(conceptualConceptName)) {
//				continue;
//			} else if(!concpetualPropertyName.equals("noprop") && !metamodelFilter.get(conceptualConceptName).contains(concpetualPropertyName)) {
//				continue;
//			}
//		}
//		
//		// get or create the metamodel vertex
//		MetamodelVertex node = null;
//		if(nodes.containsKey(conceptualConceptName)) {
//			node = nodes.get(conceptualConceptName);
//		} else {
//			node = new MetamodelVertex(conceptualConceptName);
//			// add to the map of nodes
//			nodes.put(conceptualConceptName, node);
//		}
//		
//		// add the property 
//		node.addProperty(concpetualPropertyName);
//	}
//	
//	// add all the nodes back into the main map
//	edgesAndVertices.put("nodes", nodes);
//}
//
//private static void makeEdges(IEngine engine, String query, Map<String, Map> edgesAndVertices)
//{	
//	Map<String, Map<String, String>> edges = new Hashtable<String, Map<String, String>>();
//	if(edgesAndVertices.containsKey("edges")) {
//		edges = (Map<String, Map<String, String>>) edgesAndVertices.get("edges");
//	}
//	
//	IRawSelectWrapper wrapper = WrapperManager.getInstance().getRawWrapper(engine, query);
//	while(wrapper.hasNext())
//	{
//		// based on query being passed in
//		// only 3 values in array
//		// first output is the from conceptual name
//		// second output is the to conceptual name
//		// third output is the relationship name
//		IHeadersDataRow stmt = wrapper.next();
//		Object[] values = stmt.getValues();
//		
//		String fromConceptualName = values[0] + "";
//		String toConceptualName = values[1] + "";			
//		String relName = values[2] + "";
//		
//		Map<String, String> edge = new Hashtable<String, String>();
//		edge.put("source", fromConceptualName);
//		edge.put("target", toConceptualName);
//
//		// add to edges map
//		edges.put(relName, edge);
//	}
//	// add to overall map
//	edgesAndVertices.put("edges", edges);
//}
//
//private static void makeEdgesSecure(IEngine engine, String query, Map<String, Map> edgesAndVertices, Set<String> conceptFilter)
//{	
//	Map<String, Map<String, String>> edges = new Hashtable<String, Map<String, String>>();
//	if(edgesAndVertices.containsKey("edges")) {
//		edges = (Map<String, Map<String, String>>) edgesAndVertices.get("edges");
//	}
//	
//	IRawSelectWrapper wrapper = WrapperManager.getInstance().getRawWrapper(engine, query);
//	while(wrapper.hasNext())
//	{
//		// based on query being passed in
//		// only 3 values in array
//		// first output is the from conceptual name
//		// second output is the to conceptual name
//		// third output is the relationship name
//		IHeadersDataRow stmt = wrapper.next();
//		Object[] values = stmt.getValues();
//		
//		String fromConceptualName = values[0] + "";
//		String toConceptualName = values[1] + "";
//		
//		// Check against the concept filter for both from and to concepts
//		if(conceptFilter != null && !conceptFilter.isEmpty() && (!conceptFilter.contains(fromConceptualName) || !conceptFilter.contains(toConceptualName))) {
//			continue;
//		}
//		
//		String relName = values[2] + "";
//		
//		Map<String, String> edge = new Hashtable<String, String>();
//		edge.put("source", fromConceptualName);
//		edge.put("target", toConceptualName);
//
//		// add to edges map
//		edges.put(relName, edge);
//	}
//	// add to overall map
//	edgesAndVertices.put("edges", edges);
//}*/
//
	
}