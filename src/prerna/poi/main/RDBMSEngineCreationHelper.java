package prerna.poi.main;

import java.io.File;
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

import prerna.auth.utils.SecurityInsightUtils;
import prerna.ds.util.RdbmsQueryBuilder;
import prerna.engine.api.IEngine;
import prerna.engine.api.ISelectWrapper;
import prerna.engine.api.impl.util.Owler;
import prerna.engine.impl.InsightAdministrator;
import prerna.engine.impl.MetaHelper;
import prerna.engine.impl.rdbms.RDBMSNativeEngine;
import prerna.engine.impl.rdbms.RdbmsConnectionHelper;
import prerna.engine.impl.rdf.RDFFileSesameEngine;
import prerna.rdf.engine.wrappers.WrapperManager;
import prerna.sablecc2.reactor.app.upload.rdbms.external.CustomTableAndViewIterator;
import prerna.util.MosfetSyncHelper;
import prerna.util.Utility;
import prerna.util.sql.RdbmsTypeEnum;

public class RDBMSEngineCreationHelper {

	private RDBMSEngineCreationHelper() {
		
	}
	
	/**
	 * Create existing metamodel from owl to create insights
	 * @param owl
	 */
	public static void insertAllTablesAsInsights(IEngine rdbmsEngine, Owler owl) {
		Map<String, Map<String, String>> existingMetaModel = getExistingRDBMSStructure(owl);
		insertNewTablesAsInsights(rdbmsEngine, existingMetaModel.keySet());
	}
	
	public static void insertNewTablesAsInsights(IEngine rdbmsEngine, Owler owl, Set<String> newTables) {
		Map<String, Map<String, String>> existingMetaModel = getExistingRDBMSStructure(owl, newTables);
		// use the keyset from the OWL to help with the upload
		insertNewTablesAsInsights(rdbmsEngine, existingMetaModel.keySet());
	}
	
	public static void insertNewTablesAsInsights(IEngine rdbmsEngine, Set<String> newTables) {
		String appId = rdbmsEngine.getEngineId();
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
				recipeArray[3] = "Database(\"" + appId + "\") | SelectTable(" + newTable + ") | Limit(500) | Import();"; 
				recipeArray[4] = "Frame() | QueryAll() | AutoTaskOptions(panel=[\"0\"], layout=[\"GRID\"]) | Collect(500);";

				String insightId = admin.addInsight(insightName, layout, recipeArray);
				//write recipe to file
				File retFile = MosfetSyncHelper.makeMosfitFile(appId, rdbmsEngine.getEngineName(), insightId, insightName, layout, recipeArray, false);
				// add the git here
				String recipePath = retFile.getParent();
				// make a version folder if one doesn't exist
				//GitRepoUtils.init(recipePath);
				SecurityInsightUtils.addInsight(rdbmsEngine.getEngineId(), insightId, insightName, false, layout);
				
				List<String> tags = new Vector<String>();
				tags.add("default");
				tags.add("preview");
				SecurityInsightUtils.updateInsightTags(appId, insightId, tags);
				SecurityInsightUtils.updateInsightDescription(appId, insightId, "Preview of the table " + newTable + " and all of its columns");
			}
		} catch(RuntimeException e) {
			System.out.println("Caught exception while adding question.................");
			e.printStackTrace();
		}
	}
	
	/**
	 * Get the existing RDBMS structure
	 * All keys in the map are pixel values
	 * @param owl
	 * @return
	 */
	public static Map<String, Map<String, String>> getExistingRDBMSStructure(Owler owl) {
		RDFFileSesameEngine rfse = new RDFFileSesameEngine();
		rfse.openFile(owl.getOwlPath(), null, null);
		// we create the meta helper to facilitate querying the engine OWL
		MetaHelper helper = new MetaHelper(rfse, null, null);
		List<String> conceptsList = helper.getPhysicalConcepts();
		Map<String, Map<String, String>> existingMetaModel = new HashMap<>();
		for(String conceptPhysicalUri: conceptsList) {
			// the concept names are not important
			// i will grab the pixel name
			// but there is no data that exists 
			// so grab the conceptual name
			String pixelTable = helper.getPixelSelectorFromPhysicalUri(conceptPhysicalUri);

			// now we need to go and add all the props
			Map<String, String> propMap = new HashMap<>();
			// add the properties
			List<String> propertyUris = helper.getPropertyUris4PhysicalUri(conceptPhysicalUri);
			for(String propPhysicalUri : propertyUris) {
				// so grab the conceptual name
				String propertyPixelUri = helper.getPropertyPixelUriFromPhysicalUri(conceptPhysicalUri, propPhysicalUri);
				// property conceptual uris are always /Column/Table
				String propertyPixelName = Utility.getClassName(propertyPixelUri);
				String propertyType = helper.getDataTypes(propPhysicalUri);
				String propType = "STRING";
				if(propertyType != null && propertyType.toString().contains(":")) {
					propType = propertyType.split(":")[1];
				}
				propMap.put(propertyPixelName, propType);
			}
			existingMetaModel.put(pixelTable, propMap);
		}
		return existingMetaModel;
	}
	
	/**
	 * Get the existing RDBMS structure
	 * All keys in the map are pixel values
	 * @param owl
	 * @param tablesToRetrieve
	 * @return
	 */
	public static Map<String, Map<String, String>> getExistingRDBMSStructure(Owler owl, Set<String> tablesToRetrieve) {
		RDFFileSesameEngine rfse = new RDFFileSesameEngine();
		rfse.openFile(owl.getOwlPath(), null, null);
		// we create the meta helper to facilitate querying the engine OWL
		MetaHelper helper = new MetaHelper(rfse, null, null);
		List<String> conceptsList = helper.getPhysicalConcepts();
		Map<String, Map<String, String>> existingMetaModel = new HashMap<>();
		for(String conceptPhysicalUri: conceptsList) {
			// the concept names are not important
			// i will grab the pixel name
			// but there is no data that exists 
			// so grab the conceptual name
			String pixelTable = helper.getPixelSelectorFromPhysicalUri(conceptPhysicalUri);

			if(!tablesToRetrieve.contains(pixelTable.toUpperCase())) {
				continue;
			}
			
			// now we need to go and add all the props
			Map<String, String> propMap = new HashMap<>();
			// add the properties
			List<String> propertyUris = helper.getPropertyUris4PhysicalUri(conceptPhysicalUri);
			for(String propPhysicalUri : propertyUris) {
				// so grab the conceptual name
				String propertyPixelUri = helper.getPropertyPixelUriFromPhysicalUri(conceptPhysicalUri, propPhysicalUri);
				// property conceptual uris are always /Column/Table
				String propertyPixelName = Utility.getClassName(propertyPixelUri);
				String propertyType = helper.getDataTypes(propPhysicalUri);
				String propType = "STRING";
				if(propertyType != null && propertyType.toString().contains(":")) {
					propType = propertyType.split(":")[1];
				}
				propMap.put(propertyPixelName, propType);
			}
			existingMetaModel.put(pixelTable, propMap);
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
		String schemaFilter = RdbmsConnectionHelper.getSchema(meta, con, rdbms.getConnectionUrl(), rdbms.getDbType());
		
		// table that will store 
		// table_name -> {
		// 					colname1 -> coltype,
		//					colname2 -> coltype,
		//				}
		Map<String, Map<String, String>> tableColumnMap = new Hashtable<String, Map<String, String>>();

		// get all the tables
		RdbmsTypeEnum driver;
		if (rdbmsEngine instanceof RDBMSNativeEngine) {
			driver = ((RDBMSNativeEngine) rdbmsEngine).getDbType();
		} else {
			driver = RdbmsTypeEnum.getEnumFromString(rdbmsEngine.getProperty("RDBMS_TYPE"));
		}
		CustomTableAndViewIterator tableViewIterator = new CustomTableAndViewIterator(con, meta, catalogFilter, schemaFilter, driver, tablesToRetrieve); 

		String[] columnKeys = RdbmsConnectionHelper.getColumnKeys(driver);
		final String COLUMN_NAME_STR = columnKeys[0];
		final String COLUMN_TYPE_STR = columnKeys[1];
		
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
						colDetails.put(keys.getString(COLUMN_NAME_STR), keys.getString(COLUMN_TYPE_STR));
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
		HeadersException check = HeadersException.getInstance();
		if(check.isIllegalHeader(s)) {
			s = check.appendNumOntoHeader(s);
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
