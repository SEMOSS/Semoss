package prerna.poi.main;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Vector;

import prerna.auth.utils.SecurityUpdateUtils;
import prerna.ds.util.RdbmsQueryBuilder;
import prerna.engine.api.IEngine;
import prerna.engine.api.ISelectWrapper;
import prerna.engine.impl.InsightAdministrator;
import prerna.engine.impl.MetaHelper;
import prerna.engine.impl.rdbms.RDBMSNativeEngine;
import prerna.engine.impl.rdbms.RdbmsConnectionHelper;
import prerna.engine.impl.rdf.RDFFileSesameEngine;
import prerna.query.querystruct.AbstractQueryStruct;
import prerna.rdf.engine.wrappers.WrapperManager;
import prerna.sablecc2.reactor.app.upload.rdbms.external.CustomTableAndViewIterator;
import prerna.util.OWLER;
import prerna.util.Utility;

public class RDBMSEngineCreationHelper {

	private RDBMSEngineCreationHelper() {
		
	}
	
	/**
	 * Create existing metamodel from owl to create insights
	 * @param owl
	 */
	public static void insertAllTablesAsInsights(IEngine rdbmsEngine, OWLER owl) {
		Map<String, Map<String, String>> existingMetaModel = getExistingRDBMSStructure(owl);
		insertNewTablesAsInsights(rdbmsEngine, existingMetaModel, existingMetaModel.keySet());
	}
	
	public static void insertNewTablesAsInsights(IEngine rdbmsEngine, OWLER owl, Set<String> newTables) {
		Map<String, Map<String, String>> existingMetaModel = getExistingRDBMSStructure(owl, newTables);
		// use the keyset from the OWL to help with the upload
		insertNewTablesAsInsights(rdbmsEngine, existingMetaModel, existingMetaModel.keySet());
	}
	
	public static void insertNewTablesAsInsights(IEngine rdbmsEngine,  Map<String, Map<String, String>> existingMetaModel, Set<String> newTables) {
		String engineName = rdbmsEngine.getEngineId();
		InsightAdministrator admin = new InsightAdministrator(rdbmsEngine.getInsightDatabase());
		
		//determine the # where the new questions should start
		String insightName = ""; 
		String[] recipeArray = null;
		String layout = ""; 

		try {
			for(String newTable : newTables) {
				String cleanTableName = cleanTableName(newTable);
				insightName = "Show first 500 records from " + cleanTableName;
				layout = "Grid";
				recipeArray = new String[5];
				recipeArray[0] = "AddPanel(0);";
				recipeArray[1] = "Panel(0)|SetPanelView(\"visualization\");";
				recipeArray[2] = "CreateFrame(grid).as([FRAME]);";
				StringBuilder importPixel = new StringBuilder("Database(\"" + engineName + "\") | Select(");
				Map<String, String> colToTypes = existingMetaModel.get(newTable);
				// inconsistent case for rdbms existingMetamodel passed in from reactors and building it from DatabaseMetaData 
				if(colToTypes == null) {
				   colToTypes = existingMetaModel.get(newTable.toUpperCase());
				}

				Set<String> columnNames = colToTypes.keySet();
				int size = columnNames.size();
				int counter = 0;
				for(String col : columnNames) {
					if(col.equals(AbstractQueryStruct.PRIM_KEY_PLACEHOLDER)) {
						importPixel.append(newTable);
					} else {
						importPixel.append(newTable).append("__").append(col);
					}
					if(counter + 1 != size) {
						importPixel.append(", ");
					}
					counter++;
				}
				importPixel.append(") | Limit(500) | Import();"); 
				recipeArray[3] = importPixel.toString();
				
				StringBuilder viewPixel = new StringBuilder("Frame() | Select(");
				counter = 0;
				for(String col : columnNames) {
					if(col.equals(AbstractQueryStruct.PRIM_KEY_PLACEHOLDER)) {
						viewPixel.append(newTable);
					} else {
						viewPixel.append(col);
					}
					if(counter + 1 != size) {
						viewPixel.append(", ");
					}
					counter++;
				}
				viewPixel.append(") | Format ( type = [ 'table' ] ) | TaskOptions({\"0\":{\"layout\":\"Grid\",\"alignment\":{\"label\":[");
				counter = 0;
				for(String col : columnNames) {
					viewPixel.append("\"");
					if(col.equals(AbstractQueryStruct.PRIM_KEY_PLACEHOLDER)) {
						viewPixel.append(newTable);
					} else {
						viewPixel.append(col);
					}
					viewPixel.append("\"");
					if(counter + 1 != size) {
						viewPixel.append(", ");
					}
					counter++;
				}
				viewPixel.append("]}}}) | Collect(500);"); 
				recipeArray[4] = viewPixel.toString();
				String id = admin.addInsight(insightName, layout, recipeArray);
				SecurityUpdateUtils.addInsight(rdbmsEngine.getEngineId(), id, insightName, false, layout);
			}
		} catch(RuntimeException e) {
			System.out.println("caught exception while adding question.................");
			e.printStackTrace();
		}
	}
	
	public static Map<String, Map<String, String>> getExistingRDBMSStructure(OWLER owl) {
		RDFFileSesameEngine rfse = new RDFFileSesameEngine();
		rfse.openFile(owl.getOwlPath(), null, null);
		// we create the meta helper to facilitate querying the engine OWL
		MetaHelper helper = new MetaHelper(rfse, null, null);
		Vector<String> conceptsList = helper.getConcepts(false);
		Map<String, Map<String, String>> existingMetaModel = new HashMap<>();
		for(String conceptPhysicalUri: conceptsList) {
			// so grab the conceptual name
			String conceptConceptualUri = helper.getConceptualUriFromPhysicalUri(conceptPhysicalUri);
			String conceptualName = Utility.getInstanceName(conceptConceptualUri);
			String conceptType = helper.getDataTypes(conceptPhysicalUri);
			String type = "STRING";
			if(conceptType != null && conceptType.toString().contains(":")) {
				type = conceptType.split(":")[1];
			}
			Map<String, String> propMap = new HashMap<>();
			// add the concept prim key
			propMap.put(AbstractQueryStruct.PRIM_KEY_PLACEHOLDER, type);
			
			// add the properties
			List<String> properties = helper.getProperties4Concept(conceptPhysicalUri, false);
			for(String propPhysicalUri : properties) {
				// so grab the conceptual name
				String propertyConceptualUri = helper.getConceptualUriFromPhysicalUri(propPhysicalUri);
				// property conceptual uris are always /Column/Table
				String propertyConceptualName = Utility.getClassName(propertyConceptualUri);
				String propertyType = helper.getDataTypes(propPhysicalUri);
				String propType = "STRING";
				if(propertyType != null && propertyType.toString().contains(":")) {
					propType = propertyType.split(":")[1];
				}
				
				propMap.put(propertyConceptualName, propType);
			}
			
			
			existingMetaModel.put(conceptualName, propMap);
		}
		
		return existingMetaModel;
	}
	
	public static Map<String, Map<String, String>> getExistingRDBMSStructure(OWLER owl, Set<String> tablesToRetrieve) {
		RDFFileSesameEngine rfse = new RDFFileSesameEngine();
		rfse.openFile(owl.getOwlPath(), null, null);
		// we create the meta helper to facilitate querying the engine OWL
		MetaHelper helper = new MetaHelper(rfse, null, null);
		Vector<String> conceptsList = helper.getConcepts(false);
		Map<String, Map<String, String>> existingMetaModel = new HashMap<>();
		for(String conceptPhysicalUri: conceptsList) {
			// so grab the conceptual name
			String conceptConceptualUri = helper.getConceptualUriFromPhysicalUri(conceptPhysicalUri);
			String conceptualName = Utility.getInstanceName(conceptConceptualUri);
			
			if(!tablesToRetrieve.contains(conceptualName.toUpperCase())) {
				continue;
			}
			
			String conceptType = helper.getDataTypes(conceptPhysicalUri);
			String type = "STRING";
			if(conceptType != null && conceptType.toString().contains(":")) {
				type = conceptType.split(":")[1];
			}
			Map<String, String> propMap = new HashMap<>();
			// add the concept prim key
			propMap.put(AbstractQueryStruct.PRIM_KEY_PLACEHOLDER, type);
			
			// add the properties
			List<String> properties = helper.getProperties4Concept(conceptPhysicalUri, false);
			for(String propPhysicalUri : properties) {
				// so grab the conceptual name
				String propertyConceptualUri = helper.getConceptualUriFromPhysicalUri(propPhysicalUri);
				// property conceptual uris are always /Column/Table
				String propertyConceptualName = Utility.getClassName(propertyConceptualUri);
				String propertyType = helper.getDataTypes(propPhysicalUri);
				String propType = "STRING";
				if(propertyType != null && propertyType.toString().contains(":")) {
					propType = propertyType.split(":")[1];
				}
				
				propMap.put(propertyConceptualName, propType);
			}
			
			
			existingMetaModel.put(conceptualName, propMap);
		}
		
		return existingMetaModel;
	}
	
	public static Map<String, Map<String, String>> getExistingRDBMSStructure(IEngine rdbmsEngine) {
		return getExistingRDBMSStructure(rdbmsEngine, null);
	}
	
	public static Map<String, Map<String, String>> getExistingRDBMSStructure(IEngine rdbmsEngine, Set<String> tablesToRetrieve) {
		// get the metadata from the connection
		RDBMSNativeEngine rdbms = (RDBMSNativeEngine) rdbmsEngine;
		Connection con = rdbms.makeConnection();
		DatabaseMetaData meta = rdbms.getConnectionMetadata();
		
		String catalogFilter = null;
		try {
			catalogFilter = con.getCatalog();
		} catch (SQLException e1) {
			e1.printStackTrace();
		}
		String schemaFilter = RdbmsConnectionHelper.getSchema(meta, con, rdbms.getConnectionUrl());
		
		// table that will store 
		// table_name -> {
		// 					colname1 -> coltype,
		//					colname2 -> coltype,
		//				}
		Map<String, Map<String, String>> tableColumnMap = new Hashtable<String, Map<String, String>>();

		// get all the tables
		CustomTableAndViewIterator tableViewIterator = new CustomTableAndViewIterator(meta, catalogFilter, schemaFilter, tablesToRetrieve); 

		try {
			while (tableViewIterator.hasNext()) {
				String[] nextRow = tableViewIterator.next();
				String tableOrView = nextRow[0];

				// keep a map of the columns
				Map<String, String> colDetails = new HashMap<String, String>();
				// iterate through the columns
				
				ResultSet keys = null;
				try {
					keys = meta.getColumns(catalogFilter, schemaFilter, tableOrView, null);
					while(keys.next()) {
						colDetails.put(keys.getString("column_name"), keys.getString("type_name"));
					}
				} catch(SQLException e) {
					e.printStackTrace();
				} finally {
					try {
						if(keys != null) {
							keys.close();
						}
					} catch (SQLException e1) {
						e1.printStackTrace();
					}
				}
				tableColumnMap.put(tableOrView, colDetails);
			}
		} finally {
			try {
				tableViewIterator.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}

		return tableColumnMap;
	}

	/**
	 * Remove all non alpha-numeric underscores from form name
	 * @param s
	 * @return
	 */
	public static String cleanTableName(String s) {
		s = s.trim();
		s = s.replaceAll(" ", "_");
		s = s.replaceAll("[^a-zA-Z0-9\\_]", ""); // matches anything that is not alphanumeric or underscore
		while(s.contains("__")){
			s = s.replace("__", "_");
		}
		// can't start with a digit in rdbms
		// have it start with an underscore and it will work
		if(Character.isDigit(s.charAt(0))) {
			s = "_" + s;
		}
		
		return s;
	}
	
	public static boolean conceptExists(IEngine engine, String tableName, String colName, Object instanceValue) {
		String query = "SELECT DISTINCT " + colName + " FROM " + tableName + " WHERE " + colName + "='" + RdbmsQueryBuilder.escapeForSQLStatement(instanceValue + "") + "'";
		ISelectWrapper wrapper = WrapperManager.getInstance().getSWrapper(engine, query);
		String[] names = wrapper.getVariables();
		while(wrapper.hasNext()) {
			return true;
		}
		return false;
	}
}
