//package prerna.solr;
//
//import java.io.File;
//import java.io.IOException;
//import java.nio.file.Files;
//import java.nio.file.attribute.BasicFileAttributes;
//import java.nio.file.attribute.FileTime;
//import java.security.KeyManagementException;
//import java.security.KeyStoreException;
//import java.security.NoSuchAlgorithmException;
//import java.text.DateFormat;
//import java.util.ArrayList;
//import java.util.Collection;
//import java.util.Collections;
//import java.util.Date;
//import java.util.HashMap;
//import java.util.HashSet;
//import java.util.List;
//import java.util.Map;
//import java.util.Properties;
//import java.util.Set;
//
//import org.apache.log4j.Logger;
//import org.apache.solr.client.solrj.SolrServerException;
//import org.apache.solr.client.solrj.impl.HttpSolrServer;
//import org.apache.solr.common.SolrDocument;
//import org.apache.solr.common.SolrInputDocument;
//
//import com.fasterxml.jackson.databind.ObjectMapper;
//
//import prerna.engine.api.IRawSelectWrapper;
//import prerna.engine.api.ISelectStatement;
//import prerna.engine.api.ISelectWrapper;
//import prerna.engine.impl.SmssUtilities;
//import prerna.engine.impl.rdbms.RDBMSNativeEngine;
//import prerna.rdf.engine.wrappers.WrapperManager;
//import prerna.test.TestUtilityMethods;
//import prerna.util.Constants;
//import prerna.util.DIHelper;
//import prerna.util.Utility;
//
//public final class SolrUtility {
//
//	private static final Logger LOGGER = Logger.getLogger(SolrUtility.class);
//
//	private SolrUtility() {
//
//	}
//	
//	/**
//	 * Used to create a generic SolrInputDocument
//	 * This is usually called when you want to store a list of solr documents you want to index
//	 * 		It is significantly more efficient to add the full list of documents to index at the same time
//	 * 		instead of indexing each document separately 
//	 * This can be used when creating a document for the insight and instance core
//	 * @param uniqueID				The unique id for the solr document
//	 * @param fieldData				The field data for the document.. must match the existing schema values
//	 * 									all the fields are defined above as constants
//	 * @return
//	 */
//	public static SolrInputDocument createDocument(String idFieldName, String uniqueID, Map<String, Object> fieldData) {
//		SolrInputDocument doc = new SolrInputDocument();
//		// set document ID to uniqueID
//		doc.setField(idFieldName, uniqueID);
//		// add field names and data to new Document
//		for (String fieldname : fieldData.keySet()) {
//			doc.setField(fieldname, fieldData.get(fieldname));
//		}
//		
//		return doc;
//	}
//	
//	/**
//	 * Add a series of documents into the given core
//	 * @param server
//	 * @param docs
//	 * @throws SolrServerException
//	 * @throws IOException
//	 */
//	public static void addSolrInputDocuments(HttpSolrServer server, Collection<SolrInputDocument> docs) throws SolrServerException, IOException{
//		if(docs != null && !docs.isEmpty()) {
//			LOGGER.info("Adding " + docs.size() + " documents into insight server...");
//			server.add(docs);
//			server.commit();
//			LOGGER.info("Done adding documents in insight server.");
//		}
//	}
//	
//	/**
//	 * Uses the passed in params to add a new document into a given solr core
//	 * @param uniqueID					new id to be added
//	 * @param fieldData					fields to be added to the new Doc
//	 * @throws SolrServerException
//	 * @throws IOException
//	 */
//	public static void addSolrInputDocument(HttpSolrServer server, String idFieldName, String uniqueID, Map<String, Object> fieldData) throws SolrServerException, IOException {
//		// create new Document
//		SolrInputDocument doc = createDocument(idFieldName, uniqueID, fieldData);
//		LOGGER.info("Adding INSIGHTS with unique ID:  " + uniqueID);
//		server.add(doc);
//		server.commit();
//		LOGGER.info("UniqueID " + uniqueID + "'s INSIGHTS has been added");
//	}
//	
////	/**
////	 * Right now, this is just adding the app name
////	 * @param appName
////	 * @throws KeyStoreException 
////	 * @throws NoSuchAlgorithmException 
////	 * @throws KeyManagementException 
////	 */
////	public static void addAppToSolr(String appId) throws KeyManagementException, NoSuchAlgorithmException, KeyStoreException {
////		SolrIndexEngine solrE = SolrIndexEngine.getInstance();
////		if(!solrE.containsApp(appId)) {
////			// get the db type
////			String smssFile = DIHelper.getInstance().getCoreProp().getProperty(appId + "_" + Constants.STORE);
////			Properties prop = Utility.loadProperties(smssFile);
////			
////			String appName = prop.getProperty(Constants.ENGINE_ALIAS);
////			if(appName == null) {
////				appName = appId;
////			}
////			
////			LOGGER.info("Need to add app into app core");
////			Map<String, Object> fieldData = new HashMap<String, Object>();
////			fieldData.put("id", appId);
////			fieldData.put("app_name", appName);
////			fieldData.put("app_creation_date", SolrIndexEngine.getDateFormat().format(new Date()));
////			fieldData.put("app_tags", Collections.nCopies(1, appName));
////			
////			String app_type = null;
////			String app_cost = null;
////			// the whole app cost stuff is completely made up...
////			// but it will look cool so we are doing it
////			String eType = prop.getProperty(Constants.ENGINE_TYPE);
////			if(eType.equals("prerna.engine.impl.rdbms.RDBMSNativeEngine")) {
////				String rdbmsType = prop.getProperty(Constants.RDBMS_TYPE);
////				if(rdbmsType == null) {
////					rdbmsType = "H2_DB";
////				}
////				rdbmsType = rdbmsType.toUpperCase();
////				app_type = rdbmsType;
////				if(rdbmsType.equals("TERADATA") || rdbmsType.equals("DB2")) {
////					app_cost = "$$";
////				} else {
////					app_cost = "";
////				}
////			} else if(eType.equals("prerna.engine.impl.rdbms.ImpalaEngine")) {
////				app_type = "IMPALA";
////				app_cost = "$$$";
////			} else if(eType.equals("prerna.engine.impl.rdf.BigDataEngine")) {
////				app_type = "RDF";
////				app_cost = "";
////			} else if(eType.equals("prerna.engine.impl.rdf.RDFFileSesameEngine")) {
////				app_type = "RDF";
////				app_cost = "";
////			} else if(eType.equals("prerna.ds.datastax.DataStaxGraphEngine")) {
////				app_type = "DATASTAX";
////				app_cost = "$$$";
////			} else if(eType.equals("prerna.engine.impl.solr.SolrEngine")) {
////				app_type = "SOLR";
////				app_cost = "$$";
////			} else if(eType.equals("prerna.engine.impl.tinker.TinkerEngine")) {
////				String tinkerDriver = prop.getProperty(Constants.TINKER_DRIVER);
////				if(tinkerDriver.equalsIgnoreCase("neo4j")) {
////					app_type = "NEO4J";
////					app_cost = "";
////				} else {
////					app_type = "TINKER";
////					app_cost = "";
////				}
////			} else if(eType.equals("prerna.engine.impl.json.JsonAPIEngine") || eType.equals("prerna.engine.impl.json.JsonAPIEngine2")) {
////				app_type = "JSON";
////				app_cost = "";
////			} else if(eType.equals("prerna.engine.impl.app.AppEngine")) {
////				app_type = "APP";
////				app_cost = "$";
////			}
////			
////			fieldData.put("app_type", app_type);
////			fieldData.put("app_cost", app_cost);
////			
////			try {
////				solrE.addApp(appId, fieldData);
////			} catch (SolrServerException | IOException e) {
////				e.printStackTrace();
////			}
////		} else {
////			LOGGER.info("App Exists in App Core!!");
////		}
////	}
//	
//	public static void addToSolrInsightCore(String appId) throws KeyManagementException, NoSuchAlgorithmException, KeyStoreException {
//		// get the engine name		
//		// generate the appropriate query to execute on the local master engine to get the time stamp
//		LOGGER.info("SOLR'ing on " + appId);
//
//		String smssFile = DIHelper.getInstance().getCoreProp().getProperty(appId + "_" + Constants.STORE);
//		Properties prop = Utility.loadProperties(smssFile);
//		String appName = prop.getProperty(Constants.ENGINE_ALIAS);
//		if(appName == null) {
//			appName = appId;
//		}
//		SolrIndexEngine solrE = SolrIndexEngine.getInstance();
//		String baseFolder = DIHelper.getInstance().getProperty("BaseFolder");
//		String engineDbTime = null;
//		String dbLocation = null;
//
//		// file is an export of the solr values exist
//		File solrDocFile = null;
//		// boolean is we should be updating the solr values
//		boolean forceUpdateSolr = false;
//
//		try {
//			// see if solr document exists
//			String solrDocLocation = prop.getProperty(Constants.SOLR_EXPORT);
//			if(solrDocLocation != null && !solrDocLocation.isEmpty()) {
//				solrDocLocation = baseFolder + "/" + solrDocLocation;
//				solrDocFile = new File(solrDocLocation);
//			}
//
//			// see if force update
//			String smssForceUpdateString = prop.getProperty(Constants.SOLR_RELOAD);
//			if (smssForceUpdateString != null && !smssForceUpdateString.isEmpty()) {
//				forceUpdateSolr = Boolean.parseBoolean(smssForceUpdateString);
//			}
//
//			File dbfile = SmssUtilities.getInsightsRdbmsFile(prop);
//			dbLocation = dbfile.getAbsolutePath();
//			BasicFileAttributes bfa = Files.readAttributes(dbfile.toPath(), BasicFileAttributes.class);
//			FileTime ft = bfa.lastModifiedTime();
//			DateFormat df = SolrIndexEngine.getDateFormat();
//			engineDbTime = df.format(ft.toMillis());
//		} catch(Exception ex) {
//			ex.printStackTrace();
//		}
//		
//		// make the engine compare if this is valid
//		if(forceUpdateSolr || !solrE.containsEngine(appId))
//		{
//			// get the solr index engine
//			// if the solr is active...
//			if (solrE.serverActive()) {
//				if(solrDocFile != null && solrDocFile.exists()) {
//					// delete any existing insights from this engine
//					solrE.deleteEngine(appId);
//
//					LOGGER.info("We have a solr document export to load");
//					try {
//						SolrImportUtility.processSolrTextDocument(solrDocFile.getAbsolutePath());
//					} catch (IOException e) {
//						e.printStackTrace();
//					}
//				} else {
//					// this has all the details
//					// the engine file is primarily the SMSS that is going to be utilized for the purposes of retrieving all the data
//					//jdbc:h2:@BaseFolder@/db/@ENGINE@/database;query_timeout=180000;early_filter=true;query_cache_size=24;cache_size=32768
//					String jdbcURL = "jdbc:h2:" + dbLocation.replace(".mv.db", "") + ";query_timeout=180000;early_filter=true;query_cache_size=24;cache_size=32768";
//					LOGGER.info("Connecting to URL.. " + jdbcURL);
//					String userName = "sa";
//					String password = "";
//					RDBMSNativeEngine rne = new RDBMSNativeEngine();
//					rne.makeConnection(jdbcURL, userName, password);
//
//					List<SolrInputDocument> docs = new ArrayList<SolrInputDocument>();
//					/*
//					 * The unique document is the engineName concatenated with the engine unique rdbms name (which is just a number)
//					 * 
//					 * Logic is as follows
//					 * 1) Delete all existing insights that are tagged by this engine 
//					 * 2) Execute a query to get all relevant information from the engine rdbms insights database
//					 * 3) For each insight, grab the relevant information and store into a solr document and add it to the docs list
//					 * 4) Index all the documents stored in docs list
//					 */
//
//					// 1) delete any existing insights from this engine
//					solrE.deleteEngine(appId);
//
//					// also going to get some default field values since they are not captured anywhere...
//
//					// get the current date which will be used to store in "created_on" and "modified_on" fields within schema
//					String currDate = engineDbTime;
//					// set all the users to be default...
//					String userID = "default";
//
//					// TO ACCOOMDATE FOR SCHEMA CHANGE!!!!
//					// TO ACCOOMDATE FOR SCHEMA CHANGE!!!!
//					// TO ACCOOMDATE FOR SCHEMA CHANGE!!!!
//					String q = "SELECT TYPE_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME='QUESTION_ID' and COLUMN_NAME='HIDDEN_INSIGHT'";
//					IRawSelectWrapper  wrap = WrapperManager.getInstance().getRawWrapper(rne, q);
//					if(!wrap.hasNext()) {
//						String update = "ALTER TABLE QUESTION_ID ADD HIDDEN_INSIGHT BOOLEAN DEFAULT FALSE;";
//						rne.insertData(update);
//						rne.commit();
//					}
//					wrap.cleanUp();
//					
//					// 2) execute the query and iterate through the insights
//					String query = "SELECT DISTINCT ID, QUESTION_NAME, QUESTION_LAYOUT, QUESTION_MAKEUP, QUESTION_PERSPECTIVE, HIDDEN_INSIGHT FROM QUESTION_ID";
//					ISelectWrapper wrapper = WrapperManager.getInstance().getSWrapper(rne, query);
//					while(wrapper.hasNext()){
//						ISelectStatement ss = wrapper.next();
//
//						// 3) start to get all the relevant metadata surrounding the insight
//						boolean hidden = (boolean) ss.getVar("HIDDEN_INSIGHT");
//						if(hidden) {
//							continue;
//						}
//						
//						// get the unique id of the insight within the engine
//						String id = ss.getVar("ID") + "";
//						// get the question name
//						String name = (String) ss.getVar("QUESTION_NAME");
//						// get the question layout
//						String layout = (String) ss.getVar("QUESTION_LAYOUT");
//						// is the insight hidden
//
//						// get the question perspective to use as a default tag
//						String perspective = (String) ss.getVar("QUESTION_PERSPECTIVE");
//						// sadly, at some point the perspective which we use as a tag has been added 
//						// using the following 3 ways...
//						// remove all 3 if found
//						String perspString1 ="-Perspective";
//						String perspString2 ="_Perspective";
//						String perspString3 ="Perspective";
//						if (perspective.contains(perspString1)) {
//							perspective = perspective.replace(perspString1, "").trim();
//						}
//						if(perspective.contains(perspString2)){
//							perspective = perspective.replace(perspString2, "").trim();
//						}
//						if(perspective.contains(perspString3)){
//							perspective = perspective.replace(perspString3, "").trim();
//						}
//
//						Set<String> engineSet = new HashSet<String>();
//						engineSet.add(appId);
//						/////// END CLOB PROCESSING TO GET LIST OF ENGINES ///////
//
//						// have all the relevant fields now, so store with appropriate schema name
//						// create solr document and add into docs list
//						Map<String, Object>  queryResults = new  HashMap<> ();
//						queryResults.put(SolrIndexEngine.STORAGE_NAME, name);
//						queryResults.put(SolrIndexEngine.CREATED_ON, currDate);
//						queryResults.put(SolrIndexEngine.MODIFIED_ON, currDate);
//						queryResults.put(SolrIndexEngine.USER_ID, userID);
//						queryResults.put(SolrIndexEngine.APP_ID, appId);
//						queryResults.put(SolrIndexEngine.APP_NAME, appName);
//						queryResults.put(SolrIndexEngine.APP_INSIGHT_ID, id);
//						queryResults.put(SolrIndexEngine.LAYOUT, layout);
//						queryResults.put(SolrIndexEngine.TAGS, perspective);
//						try {
//							docs.add(createDocument(SolrIndexEngine.ID, SolrIndexEngine.getSolrIdFromInsightEngineId(appId, id), queryResults));
//						} catch (Exception e) {
//							e.printStackTrace();
//						}
//					}
//
//					// 4) index all the documents at the same time for efficiency
//					try {
//						solrE.addInsights(docs);
//						rne.close();
//					} catch (SolrServerException | IOException e) {
//						e.printStackTrace();
//					}
//				}
//			}
//			LOGGER.info("Completed " + appId);
//
//			if(forceUpdateSolr) {
//				LOGGER.info(appId + " is changing solr boolean on smss");
//				Utility.changePropMapFileValue(smssFile, Constants.SOLR_RELOAD, "false");	
//			}
//		}
//		else
//		{
//			LOGGER.info("Exists !!");
//		}
//	}
//
//	/**
//	 * Used to update the data within a GIT synchronized file
//	 * @param engineId
//	 * @throws KeyManagementException
//	 * @throws NoSuchAlgorithmException
//	 * @throws KeyStoreException
//	 */
//	public static void addMosfetFileToSolrInsightCore(String fileLocation) throws KeyManagementException, NoSuchAlgorithmException, KeyStoreException {
//		// get the solr index engine
//		SolrIndexEngine solrE = SolrIndexEngine.getInstance();
//		// if the solr is active...
//		if (solrE.serverActive()) {
//			final String ENGINE_KEY = "engine";
//			final String RDBMS_ID_KEY = "rdbmsId";
//			final String INSIGHT_NAME_KEY = "insightName";
//			final String LAYOUT_KEY = "layout";
//			
//			File mosfetFile = new File(fileLocation);
//			Map<String, Object> mapData = null;
//			try {
//				mapData = new ObjectMapper().readValue(mosfetFile, Map.class);
//			} catch (IOException e1) {
//				throw new IllegalArgumentException("MOSFET file is not in valid JSON format");
//			}
//			
//			String engineName = mapData.get(ENGINE_KEY).toString();
//			String id = mapData.get(RDBMS_ID_KEY).toString();
//			String name = mapData.get(INSIGHT_NAME_KEY).toString();
//			String layout = mapData.get(LAYOUT_KEY).toString();
//			
//			// get the current date which will be used to store in "created_on" and "modified_on" fields within schema
//			String currDate = SolrIndexEngine.getDateFormat().format(new Date(mosfetFile.lastModified()));
//			// set all the users to be default...
//			String userID = "default";
//
//			Set<String> engineSet = new HashSet<String>();
//			engineSet.add(engineName);
//
//			// have all the relevant fields now, so store with appropriate schema name
//			// create solr document and add into docs list
//			Map<String, Object>  queryResults = new  HashMap<> ();
//			queryResults.put(SolrIndexEngine.STORAGE_NAME, "MAHER MODIFIED " + name);
//			queryResults.put(SolrIndexEngine.CREATED_ON, currDate);
//			queryResults.put(SolrIndexEngine.MODIFIED_ON, currDate);
//			queryResults.put(SolrIndexEngine.USER_ID, userID);
//			queryResults.put(SolrIndexEngine.APP_ID, engineName);
//			queryResults.put(SolrIndexEngine.APP_INSIGHT_ID, id);
//			queryResults.put(SolrIndexEngine.LAYOUT, layout);
//
//			try {
//				solrE.addInsight(SolrIndexEngine.getSolrIdFromInsightEngineId(engineName, id), queryResults);
//			} catch (SolrServerException | IOException e) {
//				e.printStackTrace();
//			}
//			LOGGER.info("Completed adding " + fileLocation + " into solr");
//		}
//	}
//	
//	public static void addMosfetFileToSolrInsightCore(List<String> fileLocations) throws KeyManagementException, NoSuchAlgorithmException, KeyStoreException {
//		// get the solr index engine
//		SolrIndexEngine solrE = SolrIndexEngine.getInstance();
//		// if the solr is active...
//		if (solrE.serverActive()) {
//			final String ENGINE_KEY = "engine";
//			final String RDBMS_ID_KEY = "rdbmsId";
//			final String INSIGHT_NAME_KEY = "insightName";
//			final String LAYOUT_KEY = "layout";
//
//			// this has all the details
//			List<SolrInputDocument> docs = new ArrayList<SolrInputDocument>();
//			
//			for(String fileLocation : fileLocations) {
//				File mosfetFile = new File(fileLocation);
//				Map<String, Object> mapData = null;
//				try {
//					mapData = new ObjectMapper().readValue(mosfetFile, Map.class);
//				} catch (IOException e1) {
//					throw new IllegalArgumentException("MOSFET file is not in valid JSON format");
//				}
//				
//				String engineName = mapData.get(ENGINE_KEY).toString();
//				String id = mapData.get(RDBMS_ID_KEY).toString();
//				String name = mapData.get(INSIGHT_NAME_KEY).toString();
//				String layout = mapData.get(LAYOUT_KEY).toString();
//
//				// get the current date which will be used to store in "created_on" and "modified_on" fields within schema
//				String currDate = SolrIndexEngine.getDateFormat().format(new Date(mosfetFile.lastModified()));
//				// set all the users to be default...
//				String userID = "default";
//	
//				Set<String> engineSet = new HashSet<String>();
//				engineSet.add(engineName);
//	
//				// have all the relevant fields now, so store with appropriate schema name
//				// create solr document and add into docs list
//				Map<String, Object>  queryResults = new  HashMap<> ();
//				queryResults.put(SolrIndexEngine.STORAGE_NAME, name);
//				queryResults.put(SolrIndexEngine.CREATED_ON, currDate);
//				queryResults.put(SolrIndexEngine.MODIFIED_ON, currDate);
//				queryResults.put(SolrIndexEngine.USER_ID, userID);
//				queryResults.put(SolrIndexEngine.APP_ID, engineName);
//				queryResults.put(SolrIndexEngine.APP_INSIGHT_ID, id);
//				queryResults.put(SolrIndexEngine.LAYOUT, layout);
//
//				try {
//					docs.add(createDocument(SolrIndexEngine.ID, SolrIndexEngine.getSolrIdFromInsightEngineId(engineName, id), queryResults));
//				} catch (Exception e) {
//					e.printStackTrace();
//				}
//			}
//
//			// index all the documents at the same time for efficiency
//			try {
//				solrE.addInsights(docs);
//			} catch (SolrServerException | IOException e) {
//				e.printStackTrace();
//			}
//			LOGGER.info("Completed adding documents into solr");
//		}
//	}
//
//	/**
//	 * Delete all the insights surrounding a specified engine
//	 * @param engineName				
//	 */
//	public static void deleteFromSolr(String engineName) {
//		try {
//			SolrIndexEngine.getInstance().deleteEngine(engineName);
//		} catch (KeyManagementException e) {
//			e.printStackTrace();
//		} catch (NoSuchAlgorithmException e) {
//			e.printStackTrace();
//		} catch (KeyStoreException e) {
//			e.printStackTrace();
//		}
//	}
//
//
//	public static File getStockImage(String appId, String insightId) {
//		String imageDir = DIHelper.getInstance().getProperty(Constants.BASE_FOLDER) + "\\images\\stock\\";
//		try {
//			SolrDocument solrDoc = SolrIndexEngine.getInstance().getInsight(SolrIndexEngine.getSolrIdFromInsightEngineId(appId, insightId));
//			String layout = solrDoc.get(SolrIndexEngine.LAYOUT).toString().toLowerCase();
//			if(layout.equals("area")) {
//				return new File(imageDir + "area.png");
//			} else if(layout.equals("column")) {
//				return new File(imageDir + "bar.png");
//			} else if(layout.equals("boxwhisker")) {
//				return new File(imageDir + "boxwhisker.png");
//			} else if(layout.equals("bubble")) {
//				return new File(imageDir + "bubble.png");
//			} else if(layout.equals("choropleth")) {
//				return new File(imageDir + "choropleth.png");
//			} else if(layout.equals("cloud")) {
//				return new File(imageDir + "cloud.png");
//			} else if(layout.equals("cluster")) {
//				return new File(imageDir + "cluster.png");
//			} else if(layout.equals("dendrogram")) {
//				return new File(imageDir + "dendrogram-echarts.png");
//			} else if(layout.equals("funnel")) {
//				return new File(imageDir + "funnel.png");
//			} else if(layout.equals("gauge")) {
//				return new File(imageDir + "gauge.png");
//			} else if(layout.equals("graph")) {
//				return new File(imageDir + "graph.png");
//			} else if(layout.equals("grid")) {
//				return new File(imageDir + "grid.png");
//			} else if(layout.equals("heatmap")) {
//				return new File(imageDir + "heatmap.png");
//			} else if(layout.equals("infographic")) {
//				return new File(imageDir + "infographic.png");
//			} else if(layout.equals("line")) {
//				return new File(imageDir + "line.png");
//			} else if(layout.equals("map")) {
//				return new File(imageDir + "map.png");
//			} else if(layout.equals("pack")) {
//				return new File(imageDir + "pack.png");
//			} else if(layout.equals("parallelcoordinates")) {
//				return new File(imageDir + "parallel-coordinates.png");
//			} else if(layout.equals("pie")) {
//				return new File(imageDir + "pie.png");
//			} else if(layout.equals("polar")) {
//				return new File(imageDir + "polar-bar.png");
//			} else if(layout.equals("radar")) {
//				return new File(imageDir + "radar.png");
//			} else if(layout.equals("sankey")) {
//				return new File(imageDir + "sankey.png");
//			} else if(layout.equals("scatter")) {
//				return new File(imageDir + "scatter.png");
//			} else if(layout.equals("scatterplotmatrix")) {
//				return new File(imageDir + "scatter-matrix.png");
//			} else if(layout.equals("singleaxiscluster")) {
//				return new File(imageDir + "single-axis.png");
//			} else if(layout.equals("sunburst")) {
//				return new File(imageDir + "sunburst.png");
//			} else if(layout.equals("text-widget")) {
//				return new File(imageDir + "text-widget.png");
//			} else if(layout.equals("treemap")) {
//				return new File(imageDir + "treemap.png");
//			} else {
//				return new File(imageDir + "color-logo.png");
//			}
//		} catch (KeyManagementException | NoSuchAlgorithmException | KeyStoreException | SolrServerException | IOException e) {
//			e.printStackTrace();
//		}
//		return new File(imageDir + "color-logo.png");
//	}
//	
//	public static void main(String[] args) {
//		TestUtilityMethods.loadDIHelper();
//		try {
//			SolrUtility.addMosfetFileToSolrInsightCore("C:\\workspace\\Semoss_Dev\\db\\Movie_RDBMS\\57\\.mosfet");
//		} catch (KeyManagementException | NoSuchAlgorithmException | KeyStoreException e) {
//			e.printStackTrace();
//		}
//	}
//	
//	
//	
//	
////	/**
////	 * Add the engine instances into the solr index engine
////	 * @param engineToAdd					The IEngine to add into the solr index engine
////	 * @throws KeyManagementException
////	 * @throws NoSuchAlgorithmException
////	 * @throws KeyStoreException
////	 * @throws ParseException
////	 */
////	public static void addToSolrInstanceCore(IEngine engineToAdd) throws KeyManagementException, NoSuchAlgorithmException, KeyStoreException, ParseException{
////		// get the engine id
////		String engineId = engineToAdd.getEngineId();
////		// get the solr index engine
////		SolrIndexEngine solrE = SolrIndexEngine.getInstance();
////		// if the solr is active...
////		if (solrE.serverActive()) {
////
////			List<SolrInputDocument> docs = new ArrayList<SolrInputDocument>();
////			/*
////			 * The unique document is the engineName concatenated with the concept
////			 * BUT for properties it is the engineName concatenated with the concept concatenated with the property 
////			 * Note: we only add properties that are not numeric
////			 * 
////			 * Logic is as follows
////			 * 1) Get the list of all the concepts
////			 * 2) For each concept add the concept with all its instance values to the docs list
////			 * 3) If the concept has properties, perform steps 4 & 5
////			 * 		4) for each property, get the list of values
////			 * 		5) if property is categorical, add the property with all its instance values as a document to the docs list
////			 * 6) Index all the documents that are stored in docs
////			 * 
////			 * There is a very annoying caveat.. we have an annoying bifurcation based on the engine type
////			 * If it is a RDBMS, getting the properties is pretty easy based on the way the IEngine is set up and how RDBMS queries work
////			 * However, for RDF, getting the properties requires us to create a query and execute that query to get the list of values :/
////			 */
////
////			//TODO: WE NOW STORE THE DATA TYPES ON THE OWL!!! NO NEED TO PERFORM THE QUERY BEFORE CHECKING THE TYPE!!!!!
////			//TODO: will come back to this
////
////			// 1) grab all concepts that exist in the database
////			List<String> conceptList = engineToAdd.getConcepts(false);
////			for(String concept : conceptList) {
////				// we ignore the default concept node...
////				if(concept.equals("http://semoss.org/ontologies/Concept")) {
////					continue;
////				}
////
////				// 2) get all the instances for the concept
////				// fieldData will store the instance document information when we add the concept
////				List<Object> instances = engineToAdd.getEntityOfType(concept);
////				if(instances.isEmpty()) {
////					// sometimes this list is empty when users create databases with empty fields that are
////					// meant to filled in via forms 
////					continue;
////				}
////				// create the concept unique id which is the engineName concatenated with the concept
////				String newId = engineId + "_" + concept;
////
////				//use the method that you just made to save the concept
////				Map<String, Object> fieldData = new HashMap<String, Object>();
////				fieldData.put(SolrIndexEngine.APP_ID, engineId);
////				fieldData.put(SolrIndexEngine.VALUE, concept);
////				// sadly, the instances we get back are URIs.. need to extract the instance values from it
////				List<Object> instancesList = new ArrayList<Object>();
////				for(Object instance : instances) {
////					instancesList.add(Utility.getInstanceName(instance + ""));
////				}
////				fieldData.put(SolrIndexEngine.INSTANCES, instancesList);
////				// add to the docs list
////				docs.add(createDocument(SolrIndexEngine.ID, newId, fieldData));
////
////				// 3) now see if the concept has properties
////				List<String> propName = engineToAdd.getProperties4Concept(concept, false);
////				if(propName.isEmpty()) {
////					// if no properties, go onto the next concept
////					continue;
////				}
////
////				// we found properties, lets try to add those as well
////				// here we have the bifurcation based on the engine type
////				if(engineToAdd.getEngineType().equals(IEngine.ENGINE_TYPE.RDBMS)) {
////					NEXT_PROP : for(String prop : propName) {
////						Vector<Object> propertiesList = engineToAdd.getEntityOfType(prop);
////						boolean isNumeric = false;
////						Iterator<Object> it = propertiesList.iterator();
////						while(it.hasNext()) {
////							Object o = it.next();
////							if(o == null || o.toString().isEmpty()) {
////								it.remove();
////								continue;
////							}
////							if(o instanceof Number || Utility.isStringDate(o + "")) {
////								isNumeric = true;
////								continue NEXT_PROP;
////							}
////						}
////
////						// add if the property and its instances to the docs list
////						if(!isNumeric && !propertiesList.isEmpty()) {
////							String propId = newId + "_" + prop; // in case there are properties for the same engine, tie it to the concept
////							Map<String, Object> propFieldData = new HashMap<String, Object>();
////							propFieldData.put(SolrIndexEngine.APP_ID, engineId);
////							propFieldData.put(SolrIndexEngine.VALUE, prop);
////							propertiesList.add(Utility.getInstanceName(prop));
////							propFieldData.put(SolrIndexEngine.INSTANCES, propertiesList);
////							// add the property document to the docs
////							docs.add(createDocument(SolrIndexEngine.ID, propId, propFieldData));
////						}
////					}
////				} else {
////					for(String prop : propName) {
////						// there is no way to get the list of properties for a specific concept in RDF through the interface
////						// create a query using the concept and the property name
////						String propQuery = "SELECT DISTINCT ?property WHERE { {?x <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <" + concept + "> } { ?x <" + prop + "> ?property} }";
////						ISelectWrapper propWrapper = WrapperManager.getInstance().getSWrapper(engineToAdd, propQuery);
////						List<Object> propertiesList = new ArrayList<Object>();
////						while (propWrapper.hasNext()) {
////							ISelectStatement propSS = propWrapper.next();
////							Object property = propSS.getVar("property");
////							if(property instanceof String && !Utility.isStringDate((String)property)){
////								//replace property underscores with space
////								property = property.toString();
////								propertiesList.add(property);
////							}
////						}
////
////						// add if the property and its instances to the docs list
////						if(!propertiesList.isEmpty()) {
////							String propId = newId + "_" + prop; // in case there are properties for the same engine, tie it to the concept
////							Map<String, Object> propFieldData = new HashMap<String, Object>();
////							propFieldData.put(SolrIndexEngine.APP_ID, engineId);
////							propFieldData.put(SolrIndexEngine.VALUE, prop);
////							propertiesList.add(Utility.getInstanceName(prop));
////							propFieldData.put(SolrIndexEngine.INSTANCES, propertiesList);
////							// add the property document to the docs
////							docs.add(createDocument(SolrIndexEngine.ID, propId, propFieldData));
////						}
////					}
////				}
////			}
////
////			// 6) index all the documents at the same time for efficiency
////			try {
////				solrE.addInstances(docs);
////			} catch (SolrServerException | IOException e) {
////				e.printStackTrace();
////			}
////		}
////	}
//	
//}
