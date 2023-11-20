package prerna.poi.main;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.Map;
import java.util.Set;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.engine.api.IDatabaseEngine;
import prerna.engine.api.IRDBMSEngine;
import prerna.engine.api.ISelectWrapper;
import prerna.engine.impl.rdbms.RdbmsConnectionHelper;
import prerna.rdf.engine.wrappers.WrapperManager;
import prerna.reactor.database.upload.rdbms.external.CustomTableAndViewIterator;
import prerna.util.Constants;
import prerna.util.sql.AbstractSqlQueryUtil;
import prerna.util.sql.RdbmsTypeEnum;

public class RDBMSEngineCreationHelper {

	private static final Logger classLogger = LogManager.getLogger(RDBMSEngineCreationHelper.class);
	
	private RDBMSEngineCreationHelper() {
		
	}
	
//	/**
//	 * Create existing metamodel from owl to create insights
//	 * @param owl
//	 */
//	public static void insertAllTablesAsInsights(IProject project, IDatabaseEngine rdbmsEngine, Owler owl) {
//		Map<String, Map<String, String>> existingMetaModel = getExistingRDBMSStructure(rdbmsEngine.getEngineId(), owl);
//		insertNewTablesAsInsights(project, rdbmsEngine, existingMetaModel.keySet());
//	}
//	
//	public static void insertNewTablesAsInsights(IProject project, IDatabaseEngine rdbmsEngine, Owler owl, Set<String> newTables) {
//		Map<String, Map<String, String>> existingMetaModel = getExistingRDBMSStructure(rdbmsEngine.getEngineId(), owl, newTables);
//		// use the keyset from the OWL to help with the upload
//		insertNewTablesAsInsights(project, rdbmsEngine, existingMetaModel.keySet());
//	}
//	
//	public static void insertNewTablesAsInsights(IProject project, IDatabaseEngine rdbmsEngine, Set<String> newTables) {
//		String appId = rdbmsEngine.getEngineId();
//		InsightAdministrator admin = new InsightAdministrator(project.getInsightDatabase());
//		
//		//determine the # where the new questions should start
//		String insightName = ""; 
//		List<String> recipeArray = null;
//		String layout = ""; 
//
//		try {
//			for(String newTable : newTables) {
//				String cleanTableName = cleanTableName(newTable);
//				insightName = "Show first 500 records from " + cleanTableName;
//				layout = "Grid";
//				recipeArray = new Vector<String>(5);
//				recipeArray.add("AddPanel(0);");
//				recipeArray.add("Panel(0)|SetPanelView(\"visualization\");");
//				recipeArray.add("CreateFrame(grid).as([FRAME]);");
//				recipeArray.add("Database(\"" + appId + "\") | SelectTable(" + newTable + ") | Limit(500) | Import();");
//				recipeArray.add("Frame() | QueryAll() | AutoTaskOptions(panel=[\"0\"], layout=[\"GRID\"]) | Collect(500);");
//
//				boolean global = true;
//				boolean cacheable = Utility.getApplicationCacheInsight();
//				int cacheMinutes = Utility.getApplicationCacheInsightMinutes();
//				boolean cacheEncrypt = Utility.getApplicationCacheEncrypt();
//				String cacheCron = Utility.getApplicationCacheCron();
//				LocalDateTime cachedOn = null;
//				
//				// insight metadata
//				List<String> tags = new Vector<String>();
//				tags.add("default");
//				tags.add("preview");
//				String description = "Preview of the table " + newTable + " and all of its columns";
//				String schemaName = SecurityInsightUtils.makeInsightSchemaNameUnique(project.getProjectId(), insightName);
//				
//				String insightId = admin.addInsight(insightName, layout, recipeArray, global, 
//						cacheable, cacheMinutes, cacheCron, cachedOn, cacheEncrypt, schemaName);
//				admin.updateInsightTags(insightId, tags);
//				admin.updateInsightDescription(insightId, description);
//				
//				// write recipe to file
//				try {
//					MosfetSyncHelper.makeMosfitFile(project.getProjectId(), project.getProjectName(), 
//							insightId, insightName, layout, recipeArray, global, 
//							cacheable, cacheMinutes, cacheCron, cachedOn, cacheEncrypt, 
//							description, tags, schemaName);
//					// add the insight to git
//					String gitFolder = AssetUtility.getProjectVersionFolder(project.getProjectName(), project.getProjectId());
//					List<String> files = new Vector<>();
//					files.add(insightId + "/" + MosfetFile.RECIPE_FILE);
//					GitRepoUtils.addSpecificFiles(gitFolder, files);				
//					GitRepoUtils.commitAddedFiles(gitFolder, GitUtils.getDateMessage("Saved "+ insightName +" insight on : "));		
//				} catch (Exception e) {
//					e.printStackTrace();
//				}
//				
//				// insight security
//				SecurityInsightUtils.addInsight(project.getProjectId(), insightId, insightName, 
//						global, layout, cacheable, cacheMinutes, cacheCron, cachedOn, cacheEncrypt, recipeArray, schemaName);
//				SecurityInsightUtils.updateInsightTags(project.getProjectId(), insightId, tags);
//				SecurityInsightUtils.updateInsightDescription(project.getProjectId(), insightId, description);
//			}
//		} catch(RuntimeException e) {
//			System.out.println("Caught exception while adding question.................");
//			e.printStackTrace();
//		}
//	}
//	
//	/**
//	 * Get the existing RDBMS structure
//	 * All keys in the map are pixel values
//	 * @param owler
//	 * @return
//	 */
//	public static Map<String, Map<String, String>> getExistingRDBMSStructure(String engineId, Owler owler) {
//		RDFFileSesameEngine rfse = owler.getOwlEngine();
//		// we create the meta helper to facilitate querying the engine OWL
//		MetaHelper helper = new MetaHelper(rfse, null, null);
//		List<String> conceptsList = helper.getPhysicalConcepts();
//		Map<String, Map<String, String>> existingMetaModel = new HashMap<>();
//		for(String conceptPhysicalUri: conceptsList) {
//			// the concept names are not important
//			// i will grab the pixel name
//			// but there is no data that exists 
//			// so grab the conceptual name
//			String pixelTable = helper.getPixelSelectorFromPhysicalUri(conceptPhysicalUri);
//
//			// now we need to go and add all the props
//			Map<String, String> propMap = new HashMap<>();
//			// add the properties
//			List<String> propertyUris = helper.getPropertyUris4PhysicalUri(conceptPhysicalUri);
//			for(String propPhysicalUri : propertyUris) {
//				// so grab the conceptual name
//				String propertyPixelUri = helper.getPropertyPixelUriFromPhysicalUri(conceptPhysicalUri, propPhysicalUri);
//				// property conceptual uris are always /Column/Table
//				String propertyPixelName = Utility.getClassName(propertyPixelUri);
//				String propertyType = helper.getDataTypes(propPhysicalUri);
//				String propType = "STRING";
//				if(propertyType != null && propertyType.toString().contains(":")) {
//					propType = propertyType.split(":")[1];
//				}
//				propMap.put(propertyPixelName, propType);
//			}
//			existingMetaModel.put(pixelTable, propMap);
//		}
//		return existingMetaModel;
//	}
//	
//	/**
//	 * Get the existing RDBMS structure
//	 * All keys in the map are pixel values
//	 * @param owl
//	 * @param tablesToRetrieve
//	 * @return
//	 */
//	public static Map<String, Map<String, String>> getExistingRDBMSStructure(String engineId, Owler owler, Set<String> tablesToRetrieve) {
//		RDFFileSesameEngine rfse = owler.getOwlEngine();
//		// we create the meta helper to facilitate querying the engine OWL
//		MetaHelper helper = new MetaHelper(rfse, null, null);
//		List<String> conceptsList = helper.getPhysicalConcepts();
//		Map<String, Map<String, String>> existingMetaModel = new HashMap<>();
//		for(String conceptPhysicalUri: conceptsList) {
//			// the concept names are not important
//			// i will grab the pixel name
//			// but there is no data that exists 
//			// so grab the conceptual name
//			String pixelTable = helper.getPixelSelectorFromPhysicalUri(conceptPhysicalUri);
//
//			if(!tablesToRetrieve.contains(pixelTable.toUpperCase())) {
//				continue;
//			}
//			
//			// now we need to go and add all the props
//			Map<String, String> propMap = new HashMap<>();
//			// add the properties
//			List<String> propertyUris = helper.getPropertyUris4PhysicalUri(conceptPhysicalUri);
//			for(String propPhysicalUri : propertyUris) {
//				// so grab the conceptual name
//				String propertyPixelUri = helper.getPropertyPixelUriFromPhysicalUri(conceptPhysicalUri, propPhysicalUri);
//				// property conceptual uris are always /Column/Table
//				String propertyPixelName = Utility.getClassName(propertyPixelUri);
//				String propertyType = helper.getDataTypes(propPhysicalUri);
//				String propType = "STRING";
//				if(propertyType != null && propertyType.toString().contains(":")) {
//					propType = propertyType.split(":")[1];
//				}
//				propMap.put(propertyPixelName, propType);
//			}
//			existingMetaModel.put(pixelTable, propMap);
//		}
//		return existingMetaModel;
//	}
//	
	public static Map<String, Map<String, String>> getExistingRDBMSStructure(IDatabaseEngine rdbmsEngine) {
		return getExistingRDBMSStructure(rdbmsEngine, null);
	}
	
	public static Map<String, Map<String, String>> getExistingRDBMSStructure(IDatabaseEngine rdbmsEngine, Set<String> tablesToRetrieve) {
		// get the metadata from the connection
		IRDBMSEngine rdbms = null;
		if(rdbmsEngine instanceof IRDBMSEngine) {
			rdbms = (IRDBMSEngine) rdbmsEngine;
		} else {
			throw new IllegalArgumentException("Engine must be a valid JDBC engine");
		}
		AbstractSqlQueryUtil queryUtil = rdbms.getQueryUtil();
		RdbmsTypeEnum driverEnum = rdbms.getDbType();

		// table that will store 
		// table_name -> {
		// 					colname1 -> coltype,
		//					colname2 -> coltype,
		//				}
		Map<String, Map<String, String>> tableColumnMap = new Hashtable<String, Map<String, String>>();

		Connection con = null;
		try {
			try {
				con = rdbms.makeConnection();
			} catch (SQLException e) {
				classLogger.error(Constants.STACKTRACE, e);
				throw new IllegalArgumentException(e.getMessage());
			}
			DatabaseMetaData meta = rdbms.getConnectionMetadata();
			
			String catalogFilter = queryUtil.getDatabaseMetadataCatalogFilter();
			if(catalogFilter == null) {
				try {
					catalogFilter = con.getCatalog();
				} catch (SQLException e) {
					classLogger.error(Constants.STACKTRACE, e);
				}
			}
	
			String schemaFilter = queryUtil.getDatabaseMetadataSchemaFilter();
			if(schemaFilter == null) {
				schemaFilter = (String) rdbms.getSchema();
			}
	
			String[] columnKeys = RdbmsConnectionHelper.getColumnKeys(driverEnum);
			final String COLUMN_NAME_STR = columnKeys[0];
			final String COLUMN_TYPE_STR = columnKeys[1];
	
			CustomTableAndViewIterator tableViewIterator = new CustomTableAndViewIterator(con, meta, catalogFilter, schemaFilter, driverEnum, tablesToRetrieve); 
			try {
				while (tableViewIterator.hasNext()) {
					String[] nextRow = tableViewIterator.next();
					String tableOrView = nextRow[0];
	
					// keep a map of the columns
					Map<String, String> colDetails = new HashMap<String, String>();
					// iterate through the columns
					
					ResultSet columnsRs = null;
					try {
						columnsRs = RdbmsConnectionHelper.getColumns(meta, tableOrView, catalogFilter, schemaFilter, driverEnum);
						while(columnsRs.next()) {
							colDetails.put(columnsRs.getString(COLUMN_NAME_STR), columnsRs.getString(COLUMN_TYPE_STR));
						}
					} catch(SQLException e) {
						classLogger.error(Constants.STACKTRACE, e);
					} finally {
						try {
							if(columnsRs != null) {
								columnsRs.close();
							}
						} catch (SQLException e1) {
							classLogger.error(Constants.STACKTRACE, e1);
						}
					}
					tableColumnMap.put(tableOrView, colDetails);
				}
			} finally {
				if(tableViewIterator != null) {
					tableViewIterator.close();
				}
			}
		} finally {
			if(con != null && rdbms.isConnectionPooling()) {
				try {
					con.close();
				} catch (SQLException e) {
					classLogger.error(Constants.STACKTRACE, e);
				}
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
		HeadersException check = HeadersException.getInstance();
		if(check.isIllegalHeader(s)) {
			s = check.appendNumOntoHeader(s);
		}

		return s;
	}
	
	public static boolean conceptExists(IDatabaseEngine engine, String tableName, String colName, Object instanceValue) {
		String query = "SELECT DISTINCT " + colName + " FROM " + tableName + " WHERE " + colName + "='" + AbstractSqlQueryUtil.escapeForSQLStatement(instanceValue + "") + "'";
		ISelectWrapper wrapper = WrapperManager.getInstance().getSWrapper(engine, query);
		String[] names = wrapper.getVariables();
		while(wrapper.hasNext()) {
			return true;
		}
		return false;
	}
}
