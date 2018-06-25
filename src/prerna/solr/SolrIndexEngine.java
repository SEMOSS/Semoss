package prerna.solr;

import java.io.IOException;
import java.security.KeyManagementException;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.Vector;

import org.apache.http.conn.ssl.SSLConnectionSocketFactory;
import org.apache.http.conn.ssl.TrustSelfSignedStrategy;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.ssl.SSLContextBuilder;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.HttpSolrServer;
import org.apache.solr.client.solrj.response.FacetField;
import org.apache.solr.client.solrj.response.FacetField.Count;
import org.apache.solr.client.solrj.response.Group;
import org.apache.solr.client.solrj.response.GroupCommand;
import org.apache.solr.client.solrj.response.GroupResponse;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.client.solrj.response.SpellCheckResponse;
import org.apache.solr.client.solrj.response.SpellCheckResponse.Collation;
import org.apache.solr.client.solrj.response.SpellCheckResponse.Correction;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.util.SimpleOrderedMap;

import prerna.util.Constants;
import prerna.util.DIHelper;
import prerna.util.Utility;

public class SolrIndexEngine {

	private static final Logger LOGGER = LogManager.getLogger(SolrIndexEngine.class.getName());

	// two cores for the solr index engine 
	public static enum SOLR_PATHS {
		SOLR_INSIGHTS_PATH //, SOLR_INSTANCES_PATH, SOLR_APP_PATH_NAME
	}
	
	// the name of insight core
	private static final String SOLR_INSIGHTS_PATH_NAME = "/insightCore";
	// the name of instances core
//	private static final String SOLR_INSTANCES_PATH_NAME = "/instancesCore";
	// the name of the app core
//	private static final String SOLR_APP_PATH_NAME = "/appCore";

	
	// the solr index engine singleton
	private static SolrIndexEngine singleton;
	// the url to connect to the solr index engine
	private static String url;
	// the insight solr server
	private HttpSolrServer insightServer;
//	// the instance solr server
//	private HttpSolrServer instanceServer;
//	// the app solr server
//	private HttpSolrServer appServer;

	// search return response
	private static final String QUERY_RESPONSE = "queryResponse";
	// the spellcheck return response
	private static final String SPELLCHECK_RESPONSE = "spellcheckResponse";
	// the total number of documents found -> used for limit/offset by FE
	public static final String NUM_FOUND = "numFound";

	// the default query to get all documents
	public static final String QUERY_ALL = "*:*";
	
	// Schema Field Names For Insight Core
	public static final String ID = "id";
	
	public static final String STORAGE_NAME = "name";
	public static final String INDEX_NAME = "index_name";
	
	public static final String CREATED_ON = "created_on";
	public static final String MODIFIED_ON = "modified_on";
	public static final String LAST_VIEWED_ON = "last_viewed_on";

	public static final String APP_ID = "app_id";
	public static final String APP_NAME = "app_name";
	public static final String APP_INSIGHT_ID = "app_insight_id";

	public static final String UP_VOTES = "up_votes";
	public static final String VIEW_COUNT = "view_count";
	
	public static final String TAGS = "tags";
	public static final String INDEXED_TAGS = "indexed_tags";

	public static final String DESCRIPTION = "description";
	public static final String INDEXED_DESCRIPTION = "indexed_description";
	
	public static final String USER_ID = "user_id";
	public static final String LAYOUT = "layout";

	// Schema Field Names For Instance Core
	public static final String VALUE = "value";
	public static final String INSTANCES = "instances";
	
	// ordering of results
	public static final String DESC = "desc";
	public static final String ASC = "asc";
	// the match score for the insight to the query terms
	public static final String SCORE = "score";
	
	/**
	 * Sets a constant url for Solr
	 * 
	 * @param url
	 *            - string of Solr's url
	 */
	public static void setUrl(String url) {
		SolrIndexEngine.url = url;
	}

	/**
	 * Creates one instance of the Solr engine.
	 * @return an instance of the SolrIndexEngine
	 */
	public static SolrIndexEngine getInstance()
			throws KeyManagementException, NoSuchAlgorithmException, KeyStoreException {
		if (singleton == null) {
			singleton = new SolrIndexEngine();
		}
		return singleton;
	}

	/**
	 * Singleton wrapper class around the HttpSolrServer object for
	 * adding/editing/querying documents in Solr
	 */
	private SolrIndexEngine() throws NoSuchAlgorithmException, KeyStoreException, KeyManagementException {
		SSLContextBuilder builder = new SSLContextBuilder();
		builder.loadTrustMaterial(null, new TrustSelfSignedStrategy());
		SSLConnectionSocketFactory sslsf = new SSLConnectionSocketFactory(builder.build());
		CloseableHttpClient httpclient = HttpClients.custom().setSSLSocketFactory(sslsf).build();
		
		// the URL for the solr index is defined in RDF_MAP
		// need to make sure this is accurate for code to work
		if (SolrIndexEngine.url == null) {
			SolrIndexEngine.url = DIHelper.getInstance().getProperty(Constants.SOLR_URL);
		}

		// create your insight and instance servers
		insightServer = new HttpSolrServer(SolrIndexEngine.url + SOLR_INSIGHTS_PATH_NAME, httpclient);
//		instanceServer = new HttpSolrServer(SolrIndexEngine.url + SOLR_INSTANCES_PATH_NAME, httpclient);
//		appServer = new HttpSolrServer(SolrIndexEngine.url + SOLR_APP_PATH_NAME, httpclient);
	}

	/**
	 * Used to determine if the server is active or not
	 * This call is basically made before any other method is called
	 * @return true if the server is active
	 */
	public boolean serverActive() {
		boolean isActive = true;
		try {
			insightServer.ping();
//			appServer.ping();
		} catch (Exception e) {
			isActive = false;
		}
		return isActive;
	}
	
	
	/////////////////// START ADDING INSIGHTS INTO SOLR ///////////////////
	
	/*
	 * There are two ways to add insights into solr.  
	 * 1) add one insight at a time 
	 * 		-> this is used when we are saving a specific insight
	 * 2) input a collection of insights to add
	 * 		-> this is currently being used on start up (used Utility.LoadWebEngine)
	 * 		-> we query the entire engines rdbms insights database and then add all the documents at the same time
	 * 		-> this is significantly faster than indexing each insight one at a time 
	 */
	

	
	/**
	 * Used to add a list of insights into the insight solr core
	 * @param docs						The list of solr documents to index
	 * @throws SolrServerException
	 * @throws IOException
	 */
	public void addInsights(Collection<SolrInputDocument> docs) throws SolrServerException, IOException {
		if (serverActive()) {
			LOGGER.info("Adding " + docs.size() + " documents into insight server...");
			SolrUtility.addSolrInputDocuments(insightServer, docs);
			LOGGER.info("Done adding documents in insight server.");
			buildSuggester();
		}
	}
	
	/**
	 * Uses the passed in params to add a new document into insight solr core
	 * @param uniqueID					new id to be added
	 * @param fieldData					fields to be added to the new Doc
	 * @throws SolrServerException
	 * @throws IOException
	 */
	public void addInsight(String uniqueID, Map<String, Object> fieldData) throws SolrServerException, IOException {
		if (serverActive()) {
			LOGGER.info("Adding insight with unique ID:  " + uniqueID);
			SolrUtility.addSolrInputDocument(insightServer, ID, uniqueID, fieldData);
			LOGGER.info("UniqueID " + uniqueID + "'s INSIGHTS has been added");
			buildSuggester();
		}
	}
	
	/////////////////// END ADDING INSIGHTS INTO SOLR ///////////////////

	/////////////////// START MODIFICATION TO INSIGHTS SOLR CORE ///////////////////

	/**
	 * Deletes the specified document based on its Unique ID
	 * @param uniqueID              ID to be deleted
	 */
	public void removeInsight(List<String> uniqueIds) throws SolrServerException, IOException {
		if (serverActive()) {
			// delete document based on ID
			LOGGER.info("Deleting documents with ids:  " + uniqueIds);
			insightServer.deleteById(uniqueIds);
			insightServer.commit();
			LOGGER.info("Documents with uniqueIDs: " + uniqueIds + " have been deleted");
			buildSuggester();
		}
	}
	
	/**
	 * Deletes the specified document based on its Unique ID
	 * @param uniqueID              ID to be deleted
	 */
	public void removeInsight(String uniqueId) throws SolrServerException, IOException {
		if (serverActive()) {
			// delete document based on ID
			LOGGER.info("Deleting documents with id:  " + uniqueId);
			insightServer.deleteById(uniqueId);
			insightServer.commit();
			LOGGER.info("Documents with uniqueId: " + uniqueId + " have been deleted");
			buildSuggester();
		}
	}
	
	/**
	 * Modifies the specified document based on its Unique ID
	 * @param uniqueID              ID to be modified
	 * @param fieldsToModify        specific fields to modify
	 */
	public synchronized Map<String, Object> modifyInsight(String uniqueID, Map<String, Object> fieldsToModify) throws SolrServerException, IOException {
		if (serverActive()) {
			/*
			 * solr doens't allow you to modify specific fields in a document that is already indexed
			 * therefore, to modify an existing insight we need to
			 * 1) query the solr insight core using the specific unique id for the document
			 * 2) once you have the solr document, get the map containing the field attributes
			 * 3) override any of the existing fields contained in the map that were received from the solr document
			 * 		with the new values in the fieldsToModify map that was passed into the method
			 * 4) add this new solr document back into insight core
			 * 
			 * note: if you add a solr document which has the same id as an existing document that was indexed
			 * 			solr automatically overrides that index with the new one
			 */
			
			
			// 1) query to get the existing insight
			
			// create a solr query builder and add a filter on the specific ID to get the correct insight
			SolrIndexEngineQueryBuilder queryBuilder = new SolrIndexEngineQueryBuilder();
			queryBuilder.setSearchString(QUERY_ALL);
			Map<String, List<String>> filterForId = new HashMap<String, List<String>>();
			List<String> idList = new ArrayList<String>();
			idList.add(uniqueID);
			filterForId.put(ID, idList);
			queryBuilder.setFilterOptions(filterForId);
			// execute the query
			QueryResponse res = getQueryResponse(queryBuilder.getSolrQuery(), SOLR_PATHS.SOLR_INSIGHTS_PATH);
			// the results object is defaulted to a list.. but with the ID bind (which is unique) there should
			// be exactly one solr document returned
			SolrDocumentList docList = res.getResults();
			if(docList.size() == 0) {
				LOGGER.error("COULD NOT FIND QUESITON WITH ID = " + uniqueID + " INSIDE SOLR TO MODIFY");
				return null;
			}
			SolrDocument origDoc = docList.get(0);
			
			// 2) create an iterator to go through the existing fields
			Iterator<Entry<String, Object>> iterator = origDoc.iterator();
			
			// 3) we need to create a new solr document to combine the existing values and override any of those values
			//		with those in the fieldsToModify set
			SolrInputDocument doc = new SolrInputDocument();
			
			// we also need to keep a list of fields that have been added
			// this is because the existing values in the iterator only returns those which are set
			// but there may be values which are not set that are defined in the fieldsToModify map
			// based on the looping, need to iterate through and make sure all are added
			Set<String> currFieldNames = new HashSet<String>();
			
			// loop through existing values
			while (iterator.hasNext()) {
				//get the next field value in the existing map
				Entry<String, Object> field = iterator.next();
				// fieldName will correspond to a defined field name in the schema
				String fieldName = field.getKey();
				// add to the list of field names that have been added
				currFieldNames.add(fieldName);
				// if modified field, grab new value
				if (fieldsToModify.containsKey(fieldName)) {
					doc.setField(fieldName, fieldsToModify.get(fieldName));
				} else {
					// if not modified field, use existing value
					doc.setField(fieldName, field.getValue());
					// also update the map to return what all the values are
					if(fieldName.equals(CREATED_ON) || fieldName.equals(MODIFIED_ON)) {
						// special case for dates since they must be in a specific format
						try {
							Date d = getSolrDateFormat().parse(field.getValue() + "");
							fieldsToModify.put(fieldName, getDateFormat().format(d));
						} catch (ParseException e) {
							e.printStackTrace();
						}
					} else {
						// if not a date, just add the value as is
						fieldsToModify.put(fieldName, field.getValue());
					}
				}
			}
			
			// again.. since the iterator only contains the values that are set
			// the above loop will not get any new fields defined in fieldsToModify map
			// so loop through the map and see if any fields are defined there that need to be set
			for (String newField : fieldsToModify.keySet()) {
				if (!currFieldNames.contains(newField)) {
					doc.setField(newField, fieldsToModify.get(newField));
				}
			}
			
			// when committing, automatically overrides existing field values with the new ones
			LOGGER.info("Modifying document:  " + uniqueID);
			insightServer.add(doc);
			insightServer.commit();
			LOGGER.info("UniqueID " + uniqueID + "'s doc has been modified");
			buildSuggester();
		}
		
		return fieldsToModify;
	}
	
	
//	/**
//	 * Modifies the specified document based on its Unique ID
//	 * @param uniqueID              ID to be modified
//	 * @param fieldsToModify        specific fields to modify
//	 */
//	public synchronized Map<String, Object> modifyApp(String uniqueID, Map<String, Object> fieldsToModify) throws SolrServerException, IOException {
//		if (serverActive()) {
//			/*
//			 * solr doens't allow you to modify specific fields in a document that is already indexed
//			 * therefore, to modify an existing insight we need to
//			 * 1) query the solr insight core using the specific unique id for the document
//			 * 2) once you have the solr document, get the map containing the field attributes
//			 * 3) override any of the existing fields contained in the map that were received from the solr document
//			 * 		with the new values in the fieldsToModify map that was passed into the method
//			 * 4) add this new solr document back into insight core
//			 * 
//			 * note: if you add a solr document which has the same id as an existing document that was indexed
//			 * 			solr automatically overrides that index with the new one
//			 */
//			
//			
//			// 1) query to get the existing insight
//			
//			// create a solr query builder and add a filter on the specific ID to get the correct insight
//			SolrIndexEngineQueryBuilder queryBuilder = new SolrIndexEngineQueryBuilder();
//			queryBuilder.setSearchString(QUERY_ALL);
//			Map<String, List<String>> filterForId = new HashMap<String, List<String>>();
//			List<String> idList = new ArrayList<String>();
//			idList.add(uniqueID);
//			filterForId.put(ID, idList);
//			queryBuilder.setFilterOptions(filterForId);
//			// execute the query
//			QueryResponse res = getQueryResponse(queryBuilder.getSolrQuery(), SOLR_PATHS.SOLR_APP_PATH_NAME);
//			// the results object is defaulted to a list.. but with the ID bind (which is unique) there should
//			// be exactly one solr document returned
//			SolrDocumentList docList = res.getResults();
//			if(docList.size() == 0) {
//				LOGGER.error("COULD NOT FIND APP WITH ID = " + uniqueID + " INSIDE SOLR TO MODIFY");
//				return null;
//			}
//			SolrDocument origDoc = docList.get(0);
//			
//			// 2) create an iterator to go through the existing fields
//			Iterator<Entry<String, Object>> iterator = origDoc.iterator();
//			
//			// 3) we need to create a new solr document to combine the existing values and override any of those values
//			//		with those in the fieldsToModify set
//			SolrInputDocument doc = new SolrInputDocument();
//			
//			// we also need to keep a list of fields that have been added
//			// this is because the existing values in the iterator only returns those which are set
//			// but there may be values which are not set that are defined in the fieldsToModify map
//			// based on the looping, need to iterate through and make sure all are added
//			Set<String> currFieldNames = new HashSet<String>();
//			
//			// loop through existing values
//			while (iterator.hasNext()) {
//				//get the next field value in the existing map
//				Entry<String, Object> field = iterator.next();
//				// fieldName will correspond to a defined field name in the schema
//				String fieldName = field.getKey();
//				// add to the list of field names that have been added
//				currFieldNames.add(fieldName);
//				// if modified field, grab new value
//				if (fieldsToModify.containsKey(fieldName)) {
//					doc.setField(fieldName, fieldsToModify.get(fieldName));
//				} else {
//					// if not modified field, use existing value
//					doc.setField(fieldName, field.getValue());
//					// also update the map to return what all the values are
//					if(fieldName.equals("app_creation_date")) {
//						// special case for dates since they must be in a specific format
//						try {
//							Date d = getSolrDateFormat().parse(field.getValue() + "");
//							fieldsToModify.put(fieldName, getDateFormat().format(d));
//						} catch (ParseException e) {
//							e.printStackTrace();
//						}
//					} else {
//						// if not a date, just add the value as is
//						fieldsToModify.put(fieldName, field.getValue());
//					}
//				}
//			}
//			
//			// again.. since the iterator only contains the values that are set
//			// the above loop will not get any new fields defined in fieldsToModify map
//			// so loop through the map and see if any fields are defined there that need to be set
//			for (String newField : fieldsToModify.keySet()) {
//				if (!currFieldNames.contains(newField)) {
//					doc.setField(newField, fieldsToModify.get(newField));
//				}
//			}
//			
//			// when committing, automatically overrides existing field values with the new ones
//			LOGGER.info("Modifying document:  " + uniqueID);
//			appServer.add(doc);
//			appServer.commit();
//			LOGGER.info("UniqueID " + uniqueID + "'s doc has been modified");
//		}
//		
//		return fieldsToModify;
//	}
	
	
	/**
	 * Modifies the view count and last viewed time of a document based on its Unique ID
	 * @param uniqueID              ID to be modified
	 */
	public synchronized void updateViewedInsight(String uniqueID) throws SolrServerException, IOException {
		if (serverActive()) {
			/*
			 * solr doens't allow you to modify specific fields in a document that is already indexed
			 * therefore, to modify an existing insight we need to
			 * 1) query the solr insight core using the specific unique id for the document
			 * 2) once you have the solr document, get the map containing the field attributes
			 * 3) override the view count field
			 * 4) add this new solr document back into insight core
			 * 
			 * note: if you add a solr document which has the same id as an existing document that was indexed
			 * 			solr automatically overrides that index with the new one
			 */
			
			// 1) query to get the existing insight
			
			// create a solr query builder and add a filter on the specific ID to get the correct insight
			SolrIndexEngineQueryBuilder queryBuilder = new SolrIndexEngineQueryBuilder();
			queryBuilder.setSearchString(QUERY_ALL);
			Map<String, List<String>> filterForId = new HashMap<String, List<String>>();
			List<String> idList = new ArrayList<String>();
			idList.add(uniqueID);
			filterForId.put(ID, idList);
			queryBuilder.setFilterOptions(filterForId);
			// execute the query
			QueryResponse res = getQueryResponse(queryBuilder.getSolrQuery(), SOLR_PATHS.SOLR_INSIGHTS_PATH);
			// the results object is defaulted to a list.. but with the ID bind (which is unique) there should
			// be exactly one solr document returned
			SolrDocumentList docList = res.getResults();
			if(docList.size() == 0) {
				LOGGER.error("COULD NOT FIND QUESITON WITH ID = " + uniqueID + " INSIDE SOLR TO MODIFY");
				return;
			}
			SolrDocument origDoc = docList.get(0);
			
			// 2) create an iterator to go through the existing fields
			Iterator<Entry<String, Object>> iterator = origDoc.iterator();
			
			// 3) we need to create a new solr document to combine the existing values and increae the view count
			SolrInputDocument doc = new SolrInputDocument();
			
			boolean hasViewCount = false;
			// loop through existing values
			while (iterator.hasNext()) {
				//get the next field value in the existing map
				Entry<String, Object> field = iterator.next();
				// fieldName will correspond to a defined field name in the schema
				String fieldName = field.getKey();
				// if field is view count, modify it by increasing the value by 1
				if (fieldName.equalsIgnoreCase(VIEW_COUNT)) {
					doc.setField(fieldName, ((Number) field.getValue()).longValue() + 1);
					hasViewCount = true;
				} else {
					// if not modified field, use existing value
					doc.setField(fieldName, field.getValue());
				}
			}
			
			// in case there is no view count yet
			if(!hasViewCount) {
				doc.setField(VIEW_COUNT, 1);
			}
			
			// get the current time in the right format
			String currTime = getDateFormat().format(new Date());
			doc.setField(LAST_VIEWED_ON, currTime);
			
			// when committing, automatically overrides existing field values with the new ones
			LOGGER.info("Modifying document view count and last view time:  " + uniqueID);
			insightServer.add(doc);
			insightServer.commit();
			LOGGER.info("UniqueID " + uniqueID + "'s doc has been modified");
		}
	}
	
	/////////////////// END MODIFICATION TO INSIGHTS SOLR CORE ///////////////////

	/////////////////// START ADDING APPS INTO SOLR ///////////////////

//	/**
//	 * Used to add a list of insights into the insight solr core
//	 * @param docs						The list of solr documents to index
//	 * @throws SolrServerException
//	 * @throws IOException
//	 */
//	public void addApps(Collection<SolrInputDocument> docs) throws SolrServerException, IOException {
//		if (serverActive()) {
//			LOGGER.info("Adding " + docs.size() + " documents into app server...");
//			SolrUtility.addSolrInputDocuments(appServer, docs);
//			LOGGER.info("Done adding documents in insight server.");
//		}
//	}
//	
//	/**
//	 * Uses the passed in params to add a new document into insight solr core
//	 * @param uniqueID					new id to be added
//	 * @param fieldData					fields to be added to the new Doc
//	 * @throws SolrServerException
//	 * @throws IOException
//	 */
//	public void addApp(String uniqueID, Map<String, Object> fieldData) throws SolrServerException, IOException {
//		if (serverActive()) {
//			LOGGER.info("Adding app with unique ID:  " + uniqueID);
//			SolrUtility.addSolrInputDocument(appServer, SolrIndexEngine.ID, uniqueID, fieldData);
//			LOGGER.info("UniqueID " + uniqueID + "'s app has been added");
//		}
//	}
//
//	/**
//	 * Used to verify if an app exists and has been added into solr
//	 * @param appName
//	 * @return
//	 */
//	public boolean containsApp(String appId) {
//		// check if db currently exists
//		LOGGER.info("checking if app " + appId + " needs to be added to solr");
//		SolrIndexEngineQueryBuilder builder = new SolrIndexEngineQueryBuilder();
//		Map<String, List<String>> filterData = new HashMap<String, List<String>>();
//		List<String> filterList = new Vector<String>();
//		filterList.add(appId);
//		filterData.put(SolrIndexEngine.ID, filterList);
//		builder.setFilterOptions(filterData );
//		builder.setDefaultSearchField(SolrIndexEngine.ID);
//		builder.setLimit(1);
//		
//		SolrDocumentList queryRet = null;
//		try {
//			QueryResponse res = getQueryResponse(builder.getSolrQuery(), SOLR_PATHS.SOLR_APP_PATH_NAME);
//			queryRet = res.getResults();
//		} catch (SolrServerException e) {
//			e.printStackTrace();
//		}
//		if (queryRet != null && queryRet.size() != 0) {
//			LOGGER.info("Engine " + appId + " already exists inside solr");
//		} else {
//			LOGGER.info("queryRet.size() = 0 ... so add engine");
//		}
//		return (queryRet.size() != 0);
//	}
	
	
	/////////////////// END ADDING APPS INTO SOLR ///////////////////

	
	/////////////////// START ADDING INSTANCES INTO SOLR ///////////////////

//	/*
//	 * There are two ways to add instances into solr.  
//	 * 1) add one insight at a time 
//	 * 		-> this used to be called back when we were indexing one document at a time
//	 * 2) input a collection of insights to add
//	 * 		-> this is currently being used on start up (used Utility.LoadWebEngine)
//	 * 		-> we query the entire engines rdbms insights database and then add all the documents at the same time
//	 * 		-> this is significantly faster than indexing each insight one at a time 
//	 */
//	
//	/**
//	 * Used to add a list of instances into the instance solr core
//	 * @param docs						The list of solr documents to index
//	 * @throws SolrServerException
//	 * @throws IOException
//	 */
//	public void addInstances(Collection<SolrInputDocument> docs) throws SolrServerException, IOException {
//		if (serverActive()) {
//			LOGGER.info("Adding " + docs.size() + " documents into instance server...");
//			SolrUtility.addSolrInputDocuments(instanceServer, docs);
//			LOGGER.info("Done adding documents in instance server.");
//		}
//	}
//	
//	/**
//	 * Adds a specific concept with all its instances into the instance solr core
//	 * @param uniqueID					new id to be added
//	 * @param fieldData					fields to be added to the new Doc
//	 * @throws SolrServerException
//	 * @throws IOException
//	 */
//	public void addInstance(String uniqueID, Map<String, Object> fieldData) throws SolrServerException, IOException {
//		if (serverActive()) {
//			LOGGER.info("Adding instance with unique ID:  " + uniqueID);
//			SolrUtility.addSolrInputDocument(instanceServer, ID, uniqueID, fieldData);
//			LOGGER.info("UniqueID " + uniqueID + "'s instance has been added");
//		}
//	}
	
	/////////////////// END ADDING INSTANCES INTO SOLR ///////////////////

	
	
	/////////////////// START VARIOUS WAYS TO QUERY SOLR ENGINE ///////////////////
	
	/**
	 * Provides the query response for a query on a specific path
	 * @param q							SolrQuery containing the query information
	 * @param path						The solr core to execute on
	 * @return							The QueryResponse for the query on the core
	 * @throws SolrServerException
	 */
	public QueryResponse getQueryResponse(SolrQuery q, SOLR_PATHS path) throws SolrServerException {
		/*
		 * This is the main method used to execute queries on the solr cores
		 * The SolrQuery is generated using a SolrIndexEngineQueryBuilder instance
		 * This returns a SolrQuery which is then run on the server
		 */
		
		QueryResponse res = null;
		if (serverActive()) {
			// determine based on the path which core to run the query on
			if (path == SOLR_PATHS.SOLR_INSIGHTS_PATH) {
				res = insightServer.query(q);
				LOGGER.info("Querying within the insighCore");
			}
//			else if (path == SOLR_PATHS.SOLR_APP_PATH_NAME) {
//				res = appServer.query(q);
//				LOGGER.info("Querying within the appCore");
//			} else if (path == SOLR_PATHS.SOLR_INSTANCES_PATH) {
//				res = instanceServer.query(q);
//				LOGGER.info("Querying within the instanceCore");
//			}
		}
		return res;
	}
	
	/**
	 * Refines the list of Documents in the search based on the query
	 * @param queryBuilder        		SolrIndexEngineQueryBuilder containing the information for the query
	 * @return SolrDocumentList			SolrDocumentList based on the results of the query
	 */
	public SolrDocumentList queryDocument(SolrIndexEngineQueryBuilder queryBuilder) throws SolrServerException, IOException {
		/*
		 * This is very similar to the executeSearchQuery method but differs in that it takes in a queryBuilder object
		 * This is used as a more generic way to execute a query on the solr insight core as the user has complete control over
		 * the queryBuilder that is being passed.  As opposed to the executeSearchQuery, it has specific inputs that restricts the
		 * query builder that is built.  It also differs in that this class only returns the solr document list and doesn't
		 * pass back any information regarding spell check
		 */
		SolrDocumentList results = null;
		if (serverActive()) {
			// get the solr query from the builder
			SolrQuery query = queryBuilder.getSolrQuery();
			// execute the query on the solr insight core
			QueryResponse res = getQueryResponse(query, SOLR_PATHS.SOLR_INSIGHTS_PATH);
//			// if the query return is null or has no results
//			if(res != null && res.getResults().size() == 0) {
//				/*
//				 * now we want to use the instance core
//				 * 1) we query the instance core to get an updated query search
//				 * 2) this new query search is then executed to get a new query response
//				 * 3) the results of this new query response are now returned
//				 */ 
//				
//				// 1) we query the instance core using the query previously executed
//				// look at method to see map structure
//				Map<String, Object> queryResults = executeInstanceCoreQuery(query.getQuery());
//				// 2) the query response within the returned map contains the new query to use to get results
//				String updatedQuerySearch = (String) queryResults.get(QUERY_RESPONSE);
//				// set the new search string and execute the query
//				query.setQuery(updatedQuerySearch);
//				// override the existing res object with the new one generated with the updated search
//				res = getQueryResponse(query, SOLR_PATHS.SOLR_INSIGHTS_PATH);
//			}
			// get the results from the query response
			// note the res is updated by reference if the original query returned no reuslts and had to be updated
			// using the instance core
			results = res.getResults();
		}
		LOGGER.info("Returning results of search");
		return results;
	}

	
	//////////////////////////// insight core general search methods /////////////////////////////////////////////
	/*
	 * There are the main methods used to query within the search bar.
	 * There are 3 operations available
	 * 1) search query
	 * 2) facet query
	 * 3) group by query
	 * 
	 * There are 2 methods for each operation.  One method is public and the other method is a private.
	 * The public method starts with the word "execute".  It takes in the possible inputs from the user
	 * and generates the appropriate queryBuilder object.  The private method then takes in the query builder
	 * generated from the public method and then executes it and returns the results to the user.
	 * 
	 * These methods are called in NameServer.java (Monolith package)
	 */
	
	/////////////////////////////////// operation 1 - search query /////////////////////////////////////////
	/**
	 * Executes a search query to get the insights based on the input values
	 * @param searchString						The search string for the query
	 * @param sortString						String either "asc" or "desc" to sort based on the insight name. if null does not sort
	 * @param offsetInt							The offset for the query
	 * @param limitInt							The limit for the query
	 * @param filterData						The filter values for the query.  The key in the map is the specific 
	 * 											schema key and the list corresponds to the values to filter on.
	 * 											Each entry in the list is a logical or with regards to the filter logic.
	 * 											Example: {layout : [bar, pie] } means filter to show insights where the 
	 * 											layout is either a bar OR pie chart 
	 * @return									The return map contains the following information
	 * 												1) the list of insights based on the provided limit/offset
	 * 												2) the total number of instances found <- so the FE knows how to execute for infinite scroll
	 * 												3) spell check corrections for any misspelled words
	 * @throws KeyManagementException
	 * @throws NoSuchAlgorithmException
	 * @throws KeyStoreException
	 * @throws SolrServerException
	 * @throws IOException
	 */
	public Map<String, Object> executeSearchQuery(String searchString, String sortField, String sortOrder, Integer offsetInt, Integer limitInt, Map<String, List<String>> filterData) 
			throws KeyManagementException, NoSuchAlgorithmException, KeyStoreException, SolrServerException, IOException 
	{
		/*
		 * General steps:
		 * 1) Create a query builder object
		 * 2) Add all the various inputs from the user 
		 * 3) Execute the query on the insight core
		 */
		
		// 1) create the query builder
		SolrIndexEngineQueryBuilder queryBuilder = new SolrIndexEngineQueryBuilder();
		
		
		// 2) now we start customizing the query based on the user inputs and default values defined

		// if the search string is not empty, add a search string
		// default in query builder is a select all
		if (searchString != null && !searchString.isEmpty()) {
			queryBuilder.setSearchString(searchString);
		}
		// if a order is set to sort based on the insight name
		if (sortField!= null && !sortField.isEmpty() && sortOrder != null && !sortOrder.isEmpty()) {
			queryBuilder.setSort(sortField, sortOrder.toLowerCase());
		}
		// always add sort by score desc
		queryBuilder.setSort(SCORE, DESC);
		
		// add the limit and offset
		// used for infinite scroll
		if (offsetInt != null) {
			queryBuilder.setOffset(offsetInt);
		}
		if (limitInt != null) {
			queryBuilder.setLimit(limitInt);
		}
		
		// created a heuristic that seems to work well with determining what insights better match the search terms 
		// this sets which fields will be weighted more regarding the match
		// however, only want to use this if the query is not a query_all (i.e. just return everything)
		// query_all is used if searchString is null/empty/or the solr query_all character
		if(searchString != null && !searchString.isEmpty() && !searchString.equals(QUERY_ALL)) {
			queryBuilder.setDefaultDisMaxWeighting();
		}

		// set the solr field values to return
		// these are the necessarily fields to view and run a returned insight
		List<String> retFields = new ArrayList<String>();
		retFields.add(ID);
		retFields.add(APP_ID);
		retFields.add(APP_NAME);
		retFields.add(APP_INSIGHT_ID);
		retFields.add(LAYOUT);
		retFields.add(STORAGE_NAME);
		retFields.add(CREATED_ON);
		retFields.add(MODIFIED_ON);
		retFields.add(LAST_VIEWED_ON);
		retFields.add(USER_ID);
		retFields.add(TAGS);
		retFields.add(SCORE);
		retFields.add(VIEW_COUNT);
		retFields.add(DESCRIPTION);
		queryBuilder.setReturnFields(retFields);

		// set the filter data
		if(filterData != null && !filterData.isEmpty()) {
			queryBuilder.setFilterOptions(filterData);
		}
		
		// also enable spell check to return to the user
		queryBuilder.setQueryType("/spell");
		queryBuilder.setSpellCheck(true);
		queryBuilder.setSpellCheckBuild(true);
		queryBuilder.setSpellCheckCollate(true);
		queryBuilder.setSpellCheckCollateExtendedResults(true);
		queryBuilder.setSpellCheckCount(4);

		// 3) execute the query
		return searchDocument(queryBuilder);
	}

	/**
	 * Returns results of executing a query on the insight core 
	 * @param queryBuilder							The queryBuilder containing the query options to be executed
	 * @return										The return object contains the following information
	 * 													1) the list of insights based on the provided limit/offset
	 * 													2) the total number of instances found <- so the FE knows how to execute for infinite scroll
	 * 													3) spell check corrections for any misspelled words
	 * @throws SolrServerException
	 * @throws IOException
	 */
	private Map<String, Object> searchDocument(SolrIndexEngineQueryBuilder queryBuilder) throws SolrServerException, IOException {
		// initialize the return map
		Map<String, Object> searchResultMap = new HashMap<String, Object>();
		if (serverActive()) {
			/*
			 * logic for execution
			 * 
			 * 1) get the SolrQuery object
			 * 2) execute the query on the insight core and get the results
			 * 3) get the spell check response
			 * the next steps only occur if the results returned from the insight core are empty
			 * 		4) execute the query on the instance core
			 *		5) the return object from the execute on the instance core gives you a new query
			 * 		to execute on the insight core
			 * 		6) use the new query string returned to execute that query to execute on the insight core
			 */
			
			// 1) get the query object
			SolrQuery query = queryBuilder.getSolrQuery();
			// 2) execute the query on the insight core and get the results
			QueryResponse res = getQueryResponse(query, SOLR_PATHS.SOLR_INSIGHTS_PATH);
			SolrDocumentList results = res.getResults();
			
			// 3) get the spell check response
			Map<String, List<String>> insightSpellCheck = getSpellCheckResponse(res);
			// also declare a object to store the spell check for the instance core
			Map<String, List<String>> instanceSpellCheck = null;

			// this code block is only entered if the results from executing on the insight core are empty
//			if(res != null && res.getResults().size() == 0) {
//				// we now need to query the instance core
//				// 4) use the query search string used and execute it on the instance core
//				Map<String, Object> queryResults = executeInstanceCoreQuery(query.get(CommonParams.Q));
//				// grab the spell check response from the instance core
//				instanceSpellCheck = (Map<String, List<String>>) queryResults.get(SPELLCHECK_RESPONSE);
//
//				// 5) the query response in the returned map from the execution on the instance core
//				// gives an updated query search string
//				String updatedQuerySearch = (String) queryResults.get(QUERY_RESPONSE);
//				
//				// note: we do not need the spell check params on this new search since it is a composite string
//				// with additions that we had added that the user did not include.. doesn't make sense to show them 
//				// to the user. we already have the spell check response from the insight and instance cores for what
//				// the user actually passed in
//				queryBuilder.removeSpellCheckParams();
//				// set the updatedQuerySearch within the builder
//				// and get a new query object with the updated information
//				queryBuilder.setSearchString(updatedQuerySearch);
//				query = queryBuilder.getSolrQuery();
//				// 6) query the insight core again with the updated query search string and get the results
//				res = getQueryResponse(query, SOLR_PATHS.SOLR_INSIGHTS_PATH);
//				results = res.getResults();
//			}
			
			/*
			 * Populate the return map with the results
			 * This map has the following structure
			 * 
			 * {
			 * 	queryResponse: SolrDocumentList <- this is in essence a list of maps where each entry is the necessary
			 * 										metadata to view and execute the insight 
			 * 	spellcheckResponse : 	{
			 * 								misspelledWord1: [possibleCorrectSpelling1, possibleCorrectSpelling2];
			 * 								misspelledWord2: [possibleCorrectSpelling3];
			 * 							}
			 * 	numFound: integer containing the total number of results found <- used to update limit/offset 
			 * 										for infinite scroll
			 * }
			 * However, the spellcheckResponse does not have to be present
			 */
			searchResultMap.put(QUERY_RESPONSE, results);
			searchResultMap.put(NUM_FOUND, results.getNumFound());
			// here we combine the spell check response from both the insight core and the instance core
			searchResultMap.put(SPELLCHECK_RESPONSE, mergeSpellCheckResponse(insightSpellCheck, instanceSpellCheck));
		}
		LOGGER.info("Done executing search query");
		return searchResultMap;
	}
	/////////////////////////////////// end operation 1 - search query /////////////////////////////////////////


	/////////////////////////////////// operation 2 - facet query /////////////////////////////////////////
	/**
	 * Get the facet results for a given query
	 * @param searchString							The string containing the search term
	 * @param facetList								The schema fields to facet and return
	 * @return										Map containing the return data. Each key in the main map
	 * 												contains a specific facet field that was passed in the 
	 * 												facet list.  That key corresponds to another map, which 
	 * 												contains the unique instance values present the core and the
	 * 												number of times that instance value appears 
	 * 												An example map is as follows:
	 * 												{
	 * 													layout: 
	 * 															{
	 * 																bar chart: 125,
	 * 																pie chart: 34
	 * 															}
	 * 													engines: 
	 * 															{
	 * 																movie_db : 130,
	 * 																actor_db : 12
	 * 															}
	 * 												}
	 * @throws KeyManagementException
	 * @throws NoSuchAlgorithmException
	 * @throws KeyStoreException
	 * @throws SolrServerException
	 */
	public Map<String, Map<String, Long>> executeQueryFacetResults(String searchString, List<String> facetList, SOLR_PATHS path)
					throws KeyManagementException, NoSuchAlgorithmException, KeyStoreException, SolrServerException 
	{
		/*
		 * General steps:
		 * 1) Create a query builder object
		 * 2) Add all the various inputs from the user 
		 * 3) Execute the query on the insight core
		 */
		
		// 1) create the query builder object
		SolrIndexEngineQueryBuilder queryBuilder = new SolrIndexEngineQueryBuilder();
		
		// 2) now customize the builder -- need to set the search string and the facet list
		
		// if the search string is not empty, add a search string
		// default in query builder is a select all
		if (searchString != null && !searchString.isEmpty()) {
			queryBuilder.setSearchString(searchString);
		}
		
		// since this is being used in conjunction with the search
		// we need to use the same scoring mechanism and logic as in executeSearchQuery
		if(searchString != null && !searchString.isEmpty() && !searchString.equals(QUERY_ALL)) {
			queryBuilder.setDefaultDisMaxWeighting();
		}
		
		// annoying... facet still requires a default field or it throws an error...
		queryBuilder.setDefaultSearchField(INDEX_NAME);
		
		// set the facet variables
		queryBuilder.setFacet(true);
		queryBuilder.setFacetField(facetList);
		queryBuilder.setFacetMinCount(1);
		queryBuilder.setFacetSortCount(true);

		// 3) execute the query and get the facet results
		return facetDocument(queryBuilder, path);
	}

	/**
	 * Returns facet results of executing a query on the insight core 
	 * @param queryBuilder							The queryBuilder containing the query options to be executed
	 * @return										Map containing the return data. Each key in the main map
	 * 												contains a specific facet field that was passed in the 
	 * 												facet list.  That key corresponds to another map, which 
	 * 												contains the unique instance values present the core and the
	 * 												number of times that instance value appears 
	 * @throws SolrServerException
	 */
	private Map<String, Map<String, Long>> facetDocument(SolrIndexEngineQueryBuilder queryBuilder, SOLR_PATHS path) throws SolrServerException {
		Map<String, Map<String, Long>> facetFieldMap = new LinkedHashMap<String, Map<String, Long>>();
		if (serverActive()) {
			/*
			 * logic for execution
			 * 
			 * 1) get the SolrQuery object
			 * 2) execute the query on the insight core and get the facet results
			 * steps 3-5 only occur if the facet results is empty from the insight core
			 * 		3) execute the query on the instance core
			 * 		4) the return object from the execute on the instance core gives you a new query
			 * 		to execute on the insight core
			 * 		5) use the new query string returned to execute that query to execute on the insight core
			 * 6) format the results to extract the relevant information
			 */
			
			// 1) get the query object
			SolrQuery query = queryBuilder.getSolrQuery();
			
			// 2) execute the query and get the facet results
			QueryResponse res = getQueryResponse(query, path);
			List<FacetField> facetFieldList = res.getFacetFields();
			
			// this code block is only entered if the results from executing on the insight core are empty
//			if (facetFieldList != null && facetFieldList.get(0).getValueCount() == 0) {
//				// we now need to query the instance core
//				// 3) use the query search string used and execute it on the instance core
//				Map<String, Object> queryResults = executeInstanceCoreQuery(query.get(CommonParams.Q));
//				// 4) the query response in the returned map from the execution on the instance core
//				// gives an updated query search string
//				String updatedQuerySearch = (String) queryResults.get(QUERY_RESPONSE);
//				// set the updatedQuerySearch within the builder
//				// and get a new query object with the updated information
//				queryBuilder.setSearchString(updatedQuerySearch);
//				query = queryBuilder.getSolrQuery();
//				// 5) query the insight core
//				res = getQueryResponse(query, path);
//				facetFieldList = res.getFacetFields();
//			}

			// 6) now iterate through the facet results and get the relevant information to send
			facetFieldMap = processFacetFieldMap(facetFieldList);
		}
		LOGGER.info("Done executing facet query");
		return facetFieldMap;
	}
	
	/**
	 * Process to extract the information from the facet field list
	 * @param facetFieldList
	 * @return
	 */
	public Map<String, Map<String, Long>> processFacetFieldMap(List<FacetField> facetFieldList) {
		/*
		 * example output that we want to produce
		 * 	{
		 * 		layout : 	{
		 * 						bar chart: 125,
		 * 						pie chart: 34
		 * 					}
		 * 
		 * 		engines : 	{
		 * 						movie_db : 130,
		 * 						actor_db : 12
		 * 					}
		 * 	}
		 */
		Map<String, Map<String, Long>> facetFieldMap = new LinkedHashMap<String, Map<String, Long>>();
		if (facetFieldList != null && facetFieldList.size() > 0) {
			// for each field -> corresponding to a schema entry in the facet list passed into the 
			// query builder before entering this method
			for (FacetField field : facetFieldList) {
				// the inner map will contain the instance level information
				LinkedHashMap<String, Long> innerMap = new LinkedHashMap<String, Long>();
				// the field name here is the schema name entry
				// in the above example, this corresponds to layout and engines
				String fieldName = field.getName();
				// here we get a list of unique instances associated with the field
				List<Count> facetInfo = field.getValues();
				if (facetInfo != null) {
					for (FacetField.Count facetInstance : facetInfo) {
						// facet name will correspond to the instance name
						// facet count will correspond to the number of times it appears
						// in the above example, this corresponds to each specific set
						// {bar chart : 125} and {pie chart : 34} when the fieldName is layout, and 
						// {movie_db : 130} and {actor_db : 12} when the fieldName is engines
						innerMap.put(facetInstance.getName(), facetInstance.getCount());
					}
				}
				// input into the return map
				facetFieldMap.put(fieldName, innerMap);
			}
		}
		return facetFieldMap;
	}

	/////////////////////////////////// end operation 2 - facet query /////////////////////////////////////////

	
	/////////////////////////////////// operation 3 - group by query /////////////////////////////////////////

	/**
	 * Executes a group by query to get the insights based on the input values
	 * @param searchString					The search string for the query
	 * @param groupOffset					The offset for the group by results
	 * @param groupLimit					The limit for the group by results
	 * @param groupByField					The field to group by
	 * @param groupSort						String either "asc" or "desc" to sort based on the insight name. if null does not sort
	 * @param filterData						The filter values for the query.  The key in the map is the specific 
	 * 											schema key and the list corresponds to the values to filter on.
	 * 											Each entry in the list is a logical or with regards to the filter logic.
	 * 											Example: {layout : [bar, pie] } means filter to show insights where the 
	 * 											layout is either a bar OR pie chart 
	 * @return								The return map contains the following information
	 * 											1) query response containing the group by data
	 * 											2) spell check corrections for any misspelled words
	 * 										The query response points to a map containing the group by field, which points to an 
	 * 										inner map containing each instance of the group by field as a key pointing to a list of 
	 * 										the solr documents corresponding to the group by
	 * 										Example return map for the query response portion is:
	 *										{
	 *											core_engine : 
	 *															{
	 * 																Actor_DB : SolrDocumentList with core_engine = Actor_DB,
	 * 																Movie_DB : SolrDocumentList with core_engine = Movie_DB,
	 * 																TAP_Core_Data : SolrDocumentList with core_engine = TAP_Core_Data
	 *															}
	 *										}										
	 * 										Note: the way this is set up, there can only be one field to group by, thus, in the
	 * 										above example, core_engine would be the only key present
	 * @throws KeyManagementException
	 * @throws NoSuchAlgorithmException
	 * @throws KeyStoreException
	 * @throws SolrServerException
	 * @throws IOException
	 */
	public Map<String, Object> executeQueryGroupBy(String searchString, Integer groupOffset, Integer groupLimit, 
			String groupByField, String groupSort, Map<String, List<String>> filterData)
			throws KeyManagementException, NoSuchAlgorithmException, KeyStoreException, SolrServerException, IOException 
	{
		/*
		 * General steps:
		 * 1) Create a query builder object
		 * 2) Add all the various inputs from the user 
		 * 3) Execute the query on the insight core
		 */
		
		// 1) create a query builder object
		SolrIndexEngineQueryBuilder queryBuilder = new SolrIndexEngineQueryBuilder();
		
		// 2) customize the query builder object based on input
		
		// if the search string is not empty, add a search string
		// default in query builder is a select all
		if (searchString != null && !searchString.isEmpty()) {
			queryBuilder.setSearchString(searchString);
		}

		// set the group by parameters
		queryBuilder.setGroupBy(true);
		// set the group limit/offset
		// this relates to the document within the groups themselves
		if (groupLimit != null) {
			queryBuilder.setGroupLimit(groupLimit);
		} else {
			// use default group limit of 200
			queryBuilder.setGroupLimit(200);
		}
		if (groupOffset != null) {
			queryBuilder.setGroupOffset(groupOffset);
		} else {
			// use default group offset to 0
			queryBuilder.setGroupOffset(0);
		}
		// add group sort if present
		if (groupSort != null && !groupSort.isEmpty()) {
			queryBuilder.setGroupSort(groupSort, "desc");
		}
		
		// set the limit for the number of groups to return
		//TODO: need to expose number of groups to return to UI
		queryBuilder.setLimit(50);

		// add the group by field
		// note, we only expose one group by field, but this in theory can do multi group bys
		List<String> groupList = new ArrayList<String>();
		groupList.add(groupByField);
		queryBuilder.setGroupFields(groupList);
		
		// created a heuristic that seems to work well with determining what insights better match the search terms 
		// this sets which fields will be weighted more regarding the match
		// however, only want to use this if the query is not a query_all (i.e. just return everything)
		// query_all is used if searchString is null/empty/or the solr query_all character
		if(searchString != null && !searchString.isEmpty() && !searchString.equals(QUERY_ALL)) {
			queryBuilder.setDefaultDisMaxWeighting();
		}
		
		// set the solr field values to return
		// these are the necessarily fields to view and run a returned insight
		List<String> retFields = new ArrayList<String>();
		retFields.add(ID);
		retFields.add(VIEW_COUNT);
		retFields.add(APP_ID);
		retFields.add(APP_INSIGHT_ID);
		retFields.add(LAYOUT);
		retFields.add(STORAGE_NAME);
		retFields.add(CREATED_ON);
		retFields.add(MODIFIED_ON);
		retFields.add(LAST_VIEWED_ON);
		retFields.add(DESCRIPTION);
		queryBuilder.setReturnFields(retFields);

		// set the filter data
		if(filterData != null && !filterData.isEmpty()) {
			queryBuilder.setFilterOptions(filterData);
		}
		
		// also enable spell check to return to the user
		queryBuilder.setQueryType("/spell");
		queryBuilder.setSpellCheck(true);
		queryBuilder.setSpellCheckBuild(true);
		queryBuilder.setSpellCheckCollate(true);
		queryBuilder.setSpellCheckCollateExtendedResults(true);
		queryBuilder.setSpellCheckCount(4);

		// 3) execute the query
		return groupDocument(queryBuilder);
	}

	/**
	 * Returns facet results of executing a query on the insight core 
	 * @param queryBuilder					The queryBuilder containing the query options to be executed
	 * @return								The return map contains the following information
	 * 											1) query response containing the group by data
	 * 											2) spell check corrections for any misspelled words							
	 * @throws SolrServerException
	 * @throws IOException
	 */
	private Map<String, Object> groupDocument(SolrIndexEngineQueryBuilder queryBuilder) throws SolrServerException, IOException {
		// initialize the return map
		Map<String, Object> groupByResponse = new HashMap<String, Object>();
		if (serverActive()) {
			
			// initialize the group field map
			Map<String, Map<String, SolrDocumentList>> groupFieldMap = new HashMap<String, Map<String, SolrDocumentList>>();

			/*
			 * logic for execution
			 * 
			 * 1) get the SolrQuery object
			 * 2) execute the query on the insight core and get the results
			 * 3) get the spell check response
			 * the next steps only occur if the results returned from the insight core are empty
			 * 		4) execute the query on the instance core
			 * 		5) the return object from the execute on the instance core gives you a new query
			 *		to execute on the insight core
			 * 		6) use the new query string returned to execute that query to execute on the insight core
			 * 7) format the group by return 
			 */
			
			// 1) get the solr query
			SolrQuery query = queryBuilder.getSolrQuery();
			// 2) execute the query on the insight core
			QueryResponse res = getQueryResponse(query, SOLR_PATHS.SOLR_INSIGHTS_PATH);
			// 3) get the insight spell check response
			Map<String, List<String>> insightSpellCheck = getSpellCheckResponse(res);
			// also declare a object to store the spell check for the instance core
			Map<String, List<String>> instanceSpellCheck = null;
			
			// get the group by response
			GroupResponse groupResponse = res.getGroupResponse();
			// this code block is only entered if the results from executing on the insight core are empty
//			if(groupResponse != null && groupResponse.getValues().get(0).getValues().size() == 0) {
//				// we now need to query the instance core
//				// 4) use the query search string used and execute it on the instance core
//				Map<String, Object> queryResults = executeInstanceCoreQuery(query.get(CommonParams.Q));
//				// grab the spell check response from the instance core
//				instanceSpellCheck = (Map<String, List<String>>) queryResults.get(SPELLCHECK_RESPONSE);
//				
//				// 5) the query response in the returned map from the execution on the instance core
//				// gives an updated query search string
//				String updatedQuerySearch = (String) queryResults.get(QUERY_RESPONSE);
//				
//				// note: we do not need the spell check params on this new search since it is a composite string
//				// with additions that we had added that the user did not include.. doesn't make sense to show them 
//				// to the user. we already have the spell check response from the insight and instance cores for what
//				// the user actually passed in
//				queryBuilder.removeSpellCheckParams();
//				// set the updatedQuerySearch within the builder
//				// and get a new query object with the updated information
//				queryBuilder.setSearchString(updatedQuerySearch);
//				query = queryBuilder.getSolrQuery();
//				// 6) query the insight core again with the updated query search string and get the results
//				res = getQueryResponse(query, SOLR_PATHS.SOLR_INSIGHTS_PATH);
//				groupResponse = res.getGroupResponse();
//			}

			// 7) format the group by response
			/*
			 * example output that we want to produce if we are grouping by engines:
			 *	{
			 * 		core_engine : 
			 * 				{
			 * 					Actor_DB : SolrDocumentList with core_engine = Actor_DB,
			 * 					Movie_DB : SolrDocumentList with core_engine = Movie_DB,
			 * 					TAP_Core_Data : SolrDocumentList with core_engine = TAP_Core_Data
			 * 				}
			 * 	}
			 */
			if (groupResponse != null) { 
				for (GroupCommand gc : groupResponse.getValues()) {
					Map<String, SolrDocumentList> innerMap = new HashMap<String, SolrDocumentList>();
					// groupBy is the schema level group by value
					// in the above example, it is the core_engine
					String groupBy = gc.getName();
					// get the group values
					List<Group> groups = gc.getValues();
					if (groups != null) {
						for (Group g : groups) {
							// group value is the specific instance within the group by field
							// the results is the solr document list corresponding to the specific instance value
							// in the above example, each iteration of this loop would produce one of the following:
							// { Actor_DB : SolrDocumentList with core_engine = Actor_DB } or
							// { Movie_DB : SolrDocumentList with core_engine = Movie_DB } or
							// { TAP_Core_Data : SolrDocumentList with core_engine = TAP_Core_Data }
							innerMap.put(g.getGroupValue(), g.getResult());
						}
					}
					groupFieldMap.put(groupBy, innerMap);
				}
			}
			
			// we put the formated group by map inside another map so we can also send spell check 
			groupByResponse.put(QUERY_RESPONSE, groupFieldMap);
			// we merge the spell check response between the insight and instance cores
			groupByResponse.put(SPELLCHECK_RESPONSE, mergeSpellCheckResponse(insightSpellCheck, instanceSpellCheck));
		}

		LOGGER.info("Done executing group by query");
		return groupByResponse;
	}
	/////////////////////////////////// end operation 3 - group by query /////////////////////////////////////////

	
//	//////////////////////////// instance core search methods /////////////////////////////////////////////
//	/*
//	 * Querying always occurs on the insight core before the instance core
//	 * When the query on the insight core returns no results, then that search string is executed on the insight core
//	 * 
//	 * The main assumption is that the query on the insight core returns no results when the search term is a set of instances
//	 * The instance core is then queried to provide the list of concepts associated with the search term
//	 * For example: the user passes in AHLTA
//	 * Output: System (matches the system AHLTA), SystemDCSite (matches AHLTA%xyz where xyz is a DCSite), etc.
//	 * 
//	 * The output is them used as a new search term in the insight core to get insights
//	 */
//	
//	/**
//	 * Execute a query on the instance core
//	 * @param querySearch						String containing the search term used by the user
//	 * @return									Map containing the query response and the spell check response 
//	 * 											for the query.  The query response is a string containing the new
//	 * 											search term to execute on the insight core.  The spell check response
//	 * 											is in case there is a spelling mistake with a user entered instance.
//	 * @throws SolrServerException
//	 */
//	public Map<String, Object> executeInstanceCoreQuery(String querySearch) throws SolrServerException {
//		Map<String, Object> queryResults = new HashMap<String, Object>();
//		if (serverActive()) {
//			// search for instances
//			SolrIndexEngineQueryBuilder queryBuilder = new SolrIndexEngineQueryBuilder();
//			queryBuilder.setSearchString(querySearch);
//			queryBuilder.setDefaultSearchField(INSTANCES);
//			queryBuilder.setQueryType("/spell");
//			queryBuilder.setSpellCheck(true);
//			queryBuilder.setSpellCheckBuild(true);
//			queryBuilder.setSpellCheckCollate(true);
//			queryBuilder.setSpellCheckCollateExtendedResults(true);
//			queryBuilder.setSpellCheckCount(4);
//			// no need to return the entire document info, just return the concept/property which is 
//			// stored under the "value" schema name
//			queryBuilder.addReturnFields(VALUE);
//			// get the solr query
//			SolrQuery query = queryBuilder.getSolrQuery();
//			// execute the query
//			QueryResponse res = getQueryResponse(query, SOLR_PATHS.SOLR_INSTANCES_PATH);
//			// the query response we want from the instance core is a new query to run on the insight
//			// core to get the instances that best matches the user input
//			String appendQuery = getUniqueConceptsForInstances(res);
//			//append the original search term with the new return
//			querySearch = querySearch + " " + appendQuery;
//			LOGGER.info("New search query will be: " + querySearch);
//			// set this as the query response
//			queryResults.put(QUERY_RESPONSE, querySearch);
//			
//			// set any spell check return
//			Map<String, List<String>> spellCheckResponse = getSpellCheckResponse(res);
//			if (spellCheckResponse != null && !spellCheckResponse.isEmpty()) {
//				queryResults.put(SPELLCHECK_RESPONSE, spellCheckResponse);
//			}
//		}		
//		/*
//		 * The return map containing the instance core results
//		 * This map has the following structure
//		 * 
//		 * {
//		 * 	queryResponse: "STRING CONTAINING NEW INSIGHT CORE SEARCH"
//		 * 	spellcheckResponse : 	{
//		 * 								misspelledWord1: [possibleCorrectSpelling1, possibleCorrectSpelling2];
//		 * 								misspelledWord2: [possibleCorrectSpelling3];
//		 * 							}
//		 * }
//		 * ***However, the spellcheckResponse does not have to be present
//		 */
//		return queryResults;
//	}

	/**
	 * This goes through the query return in instances core to get all the unique set of concepts
	 * @param queryResponse				The queryResponse object containing the unique set of 
	 * @return							Returns a string of the concepts, space delimited  
	 */
	private String getUniqueConceptsForInstances(QueryResponse queryResponse) {
		// create a set such that the search term is a unique set of concepts
		Set<String> valueSet = new HashSet<String>();
		// iterate through all the returns
		SolrDocumentList results = queryResponse.getResults();
		for (SolrDocument solrDoc : results) {
			// grab the concept/property from the solr doc
			String concept_or_property = (String) solrDoc.getFieldValue(VALUE);
			// add it to the set
			valueSet.add(Utility.getInstanceName(concept_or_property));
		}
		
		// iterate through the set and return a string containing all the values space delimited
		String queryAddition = "";
		for (String value : valueSet) {
			queryAddition += value + " ";
		}

		LOGGER.info("Based on the instance query add this to the next query: " + queryAddition);
		return queryAddition;
	}
	//////////////////////////// end instance core search methods /////////////////////////////////////////////

	
	//////////////////////////// spell check methods /////////////////////////////////////////////
	/*
	 * The following is used to get the spell check response from a query
	 * Each term within a query that is identified as being misspelled will receive a list of possible corrections
	 * 
	 * This is done at both the insight core and instance core to provide spell checking at both the 
	 * insight metadata level and the instance level if the user search is utilizing instances
	 */
	
	/**
	 * Format the spell check response contained within solr to the appropriate format for the FE
	 * @param res				QueryResponse object returned from executing a query on the 
	 * @return					Returns the results of the spell check in a more appropriate format
	 * 							Example input in search: moviee
	 * 							Example output format from spell check: { moviee -> [movies, movie, motive, mobile] }
	 */
	private static Map<String, List<String>> getSpellCheckResponse(QueryResponse res) {
		Map<String, List<String>> spellCheckRet = new HashMap<String, List<String>>();
		SpellCheckResponse scr = res.getSpellCheckResponse();
		// if there is a spell check response
		if (scr != null) {
			// grab the set of collations
			List<Collation> collations = scr.getCollatedResults();
			if (collations != null) {
				for (Collation c : collations) {
					// for each collation, grab the original input and the corrected values
					for (Correction correction : c.getMisspellingsAndCorrections()) {
						String orig = correction.getOriginal();
						String corr = correction.getCorrection();
						// the below is just to check if the incorrect word has already been seen
						// if so, add it to the existing list of values
						// else, set a new key-value pair in the map
						List<String> suggestions;
						if(spellCheckRet.containsKey(orig)) {
							suggestions = spellCheckRet.get(orig);
							suggestions.add(corr);
						} else {
							suggestions = new ArrayList<String>();
							suggestions.add(corr);
							spellCheckRet.put(orig, suggestions);
						}
					}
				}
			}
		}
		return spellCheckRet;
	}
	
	/**
	 * Merges the spell check results of execution on the insight core and on the instance core
	 * @param insightCoreSpelling			The spell check response on the insight core
	 * @param instanceCoreSpelling			The spell check response on the instance core
	 * @return								The combined spell check response from both cores
	 */
	private Map<String, List<String>> mergeSpellCheckResponse(Map<String, List<String>> insightCoreSpelling, Map<String, List<String>> instanceCoreSpelling) {
		/*
		 * This method just iterates through the two maps and combines the spelling corrections if the misspelled 
		 * word appears in both lists... i.e. if the two maps inputed contain the same key (key being the misspelled word)
		 * then the lists that they point to are combined into one list
		 */
		Map<String, List<String>> allResponse = new HashMap<String, List<String>>(); 
		//add insight suggestions
		if (insightCoreSpelling != null && !insightCoreSpelling.isEmpty()) {
			allResponse.putAll(insightCoreSpelling);
		}
		//add instance suggestions
		if (instanceCoreSpelling != null && !instanceCoreSpelling.isEmpty()) {
			for (String searchString : instanceCoreSpelling.keySet()) {
				// if the key (misspelled word) is already present due to it being in the other map
				if(allResponse.containsKey(searchString)) {
					// get all the current spelling corrections provided from the insight core which 
					// has been put into the new allResponse map
					List<String> currSpellingCorrections = allResponse.get(searchString);
					// get all the new spelling corrections provided from the instance core
					List<String> newSpellingCorrections = instanceCoreSpelling.get(searchString);
					// loop through all the instance spelling corrections
					for(String newSpelling : newSpellingCorrections) {
						// but while you loop, only add in the corrections not already there so the 
						// list of corrected values is unique
						if(!currSpellingCorrections.contains(newSpelling)) {
							currSpellingCorrections.add(newSpelling);
						}
					}
				} else {
					// the misspelled word hasn't been seen before
					// just add it and the list of corrections into the new map
					allResponse.put(searchString, instanceCoreSpelling.get(searchString));
				}
			}
		}
		return allResponse;
	}
	//////////////////////////// end spell check methods /////////////////////////////////////////////

	
	
	//////////////////////////// auto-complete methods /////////////////////////////////////////////
	/*
	 * Execute Auto Complete and getAutoSuggestResponse are both used to suggest insights to the user 
	 * based on the insight names that are stored within the solr core.
	 * 
	 * This uses the solr "insight_suggest" schema field.  This field is a copyfield from the insight name
	 * schema field (index_name), but uses different indexing/querying such that the entire question name
	 * is suggested based on the user input. It is currently set up to only show auto-complete suggestions 
	 * where the user input is a correct "prefix" for the insight name
	 * For example, if a user types in: "movie relationship" the suggestions might be the following:
	 * 			1) "movie relationship with actors"
	 * 			2) "movie relationship with studios"
	 * But it will not suggest the following
	 * 			1) "what are the movie relationships with actors"
	 * 			2) "what are the movie relationships with studios"
	 * 
	 * The auto-complete will only show results where the user input is a "prefix" on the insight name, but
	 * when a user runs the search routine, it will show all the insights above.
	 */

	/**
	 * Takes in a term (a sentence) and provides suggestions for insights the user might be interested in
	 * @param term							String containing the input to find suggestions
	 * @return								A List of suggestions based on the input term
	 * @throws SolrServerException
	 * @throws IOException
	 */
	public List<String> executeAutoCompleteQuery(String term) throws SolrServerException, IOException {
		List<String> insightLists = new ArrayList<String>();
		if(term != null && !term.isEmpty()) {
			// generate a solr query
			SolrIndexEngineQueryBuilder queryBuilder = new SolrIndexEngineQueryBuilder();
			// uses the suggest path and set the spellcheck query value to contain the term
			// the auto-complete suggestions exist within the spellcheckresponse on the QueryResponse
			// 		once the query is executed
			queryBuilder.setQueryType("/suggest");
			queryBuilder.setSearchString(term);
			// sort the return based on name
			queryBuilder.setSort(STORAGE_NAME, DESC);
			// get the solr query
			SolrQuery query = queryBuilder.getSolrQuery();
			// execute the query on the solr insight core
			QueryResponse resInsight = getQueryResponse(query, SOLR_PATHS.SOLR_INSIGHTS_PATH);
			// return the results
			insightLists.addAll(getAutoSuggestResponse(resInsight));
		}

		LOGGER.info("Suggestions include ::: " + insightLists);
		return insightLists;
	}

	/**
	 * Extracts the auto-complete suggestions from the query return
	 * @param res				The QueryResponse after the SolrQuery has been run on a specific core
	 * @return					List containing the suggested auto-complete sentences					
	 */
	private List<String> getAutoSuggestResponse(QueryResponse res) {
		List<String> autoSuggestRet = new ArrayList<String>();
		// the auto-complete suggestions are contained within the spell check response
		SimpleOrderedMap suggestResponse = (SimpleOrderedMap) ((Map<Object, Object>) res.getResponse().get("suggest")).get("mySuggester");
		if(suggestResponse != null && suggestResponse.size() == 1) {
			// get the suggestions
			SimpleOrderedMap terms = (SimpleOrderedMap) suggestResponse.getVal(0);
			if(terms != null) {
				List<SimpleOrderedMap> valueArr = (List<SimpleOrderedMap>) terms.getVal(1);
				if(valueArr != null) {
					for(int i = 0; i < valueArr.size(); i++) {
						SimpleOrderedMap inner = valueArr.get(i);
						autoSuggestRet.add(inner.get("term").toString());
					}
				}
			}
		}
		return autoSuggestRet;
	}
	//////////////////////////// end auto-complete methods /////////////////////////////////////////////

	
	//////////////////////////// getInsight methods /////////////////////////////////////////////
	/*
	 * The getInsight methods are just useful wrappers to get insight information
	 * This is used when the information isn't stored in the insight rdbms and instead
	 * we need to get the information from solr... this is the case for "drag and drop"
	 * insights where files are passed in
	 */
	
	/**
	 * Returns the solr metadata around one insight
	 * This calls the normal querying routines but with a filter on the solr id
	 * @param uniqueID						String containing the unique solr id for the insight
	 * @return								SolrDocument containing the insight metadata
	 * @throws SolrServerException
	 * @throws IOException
	 */
	public SolrDocument getInsight(String uniqueID) throws SolrServerException, IOException {
		SolrDocumentList results = null;
		if(serverActive()) {
			LOGGER.info("Getting documents with id:  " + uniqueID);
			// create a query builder
			SolrIndexEngineQueryBuilder queryBuilder = new SolrIndexEngineQueryBuilder();
			// use the generic return all query
			queryBuilder.setSearchString(QUERY_ALL);
			// create the required filter map object to contain the single uniqueID
			Map<String, List<String>> filterForId = new HashMap<String, List<String>>();
			List<String> uniqueIDs = new ArrayList<String>();
			uniqueIDs.add(uniqueID);
			filterForId.put(ID, uniqueIDs);
			// set the query builder to add the unique id as a filter
			queryBuilder.setFilterOptions(filterForId);
			// execute the query on the insight core
			QueryResponse res = getQueryResponse(queryBuilder.getSolrQuery(), SOLR_PATHS.SOLR_INSIGHTS_PATH);
			// get the output 
			results = res.getResults();
		}
		// return should only contain 1 value since each id is unique
		return results.get(0);
	}
	
	/**
	 * Returns the solr metadata around a list of insights
	 * @param uniqueIDs						List containing the unique solr ids for the insights
	 * @return								SolrDocumentList containing the insights metadata
	 * @throws SolrServerException
	 * @throws IOException
	 */
	public SolrDocumentList getInsight(List<String> uniqueIDs) throws SolrServerException, IOException {
		SolrDocumentList results = null;
		if(serverActive()) {
			LOGGER.info("Getting documents with ids:  " + uniqueIDs);
			// create a query builder
			SolrIndexEngineQueryBuilder queryBuilder = new SolrIndexEngineQueryBuilder();
			// use the generic return all query
			queryBuilder.setSearchString(QUERY_ALL);
			// create the required filter map object to contain the single uniqueID
			Map<String, List<String>> filterForId = new HashMap<String, List<String>>();
			filterForId.put(ID, uniqueIDs);
			// set the query builder to use the unqiue ids passed in as a filter
			queryBuilder.setFilterOptions(filterForId);
			// execute the query on the insight core
			QueryResponse res = getQueryResponse(queryBuilder.getSolrQuery(), SOLR_PATHS.SOLR_INSIGHTS_PATH);
			// get the output 
			results = res.getResults();
		}
		return results;
	}
	//////////////////////////// end getInsight methods /////////////////////////////////////////////

	
	
	
	/////////////////// END VARIOUS WAYS TO QUERY SOLR ENGINE ///////////////////

	
	/////////////////// START UTILITY METHODS ///////////////////

	/**
	 * Used to verify if specified engine is already contained within solr If
	 * the method returns a false then the engine needs to be added to solr
	 * @param engineId           name of the engine to verify existence
	 * @return true if the engine already exists
	 */
	public boolean containsEngine(String engineId) {
		// check if db currently exists
		LOGGER.info("checking if engine " + engineId + " needs to be added to solr");
		SolrIndexEngineQueryBuilder builder = new SolrIndexEngineQueryBuilder();
		Map<String, List<String>> filterData = new HashMap<String, List<String>>();
		List<String> filterList = new Vector<String>();
		filterList.add(engineId);
		filterData.put(SolrIndexEngine.APP_ID, filterList);
		builder.setFilterOptions(filterData );
		builder.setDefaultSearchField(SolrIndexEngine.APP_ID);
		builder.setLimit(1);
		
		SolrDocumentList queryRet = null;
		try {
			queryRet = queryDocument(builder);
		} catch (SolrServerException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		if(queryRet == null) {
			LOGGER.info("ERROR! Couldn't query solr and got queryReturn as null");
			LOGGER.info("ERROR! Couldn't query solr and got queryReturn as null");
			LOGGER.info("ERROR! Couldn't query solr and got queryReturn as null");
			LOGGER.info("ERROR! Couldn't query solr and got queryReturn as null");
			LOGGER.info("ERROR! Couldn't query solr and got queryReturn as null");
			return false;
		} else if(queryRet.size() == 0) {
			LOGGER.info("queryRet.size() = 0 ... so add engine");
			return false;
		} else {
			LOGGER.info("Engine " + engineId + " already exists inside solr");
			return true;
		}
	}

	/**
	 * Deletes all insights related to a specified engine
	 * @param engineId      engine name to delete
	 */
	public void deleteEngine(String appId) {
		if (serverActive()) {
			try {
				LOGGER.info("DELETING ENGINE FROM SOLR " + appId);
				String query = APP_ID + ":" + appId;
				LOGGER.info("deleted query is " + query);
				insightServer.deleteByQuery(query);
				insightServer.commit();
				query = ID + ":" + appId;
				LOGGER.info("deleted query is " + query);
//				appServer.deleteByQuery(query);
//				appServer.commit();
//				LOGGER.info("successfully removed " + appId + " from solr" + appId);
			} catch (SolrServerException e1) {
				e1.printStackTrace();
			} catch (IOException e1) {
				e1.printStackTrace();
			}
		}
	}
	
	/**
	 * Deletes all insights related to a specified engine
	 * @param engineName      engine name to delete
	 * @throws SolrServerException 
	 */
	public SolrDocumentList getEngineInsights(String engineName) throws SolrServerException {
		if (serverActive()) {
			LOGGER.info("Getting insights for core_engine = " + engineName);
			/*
			 * General steps:
			 * 1) Create a query builder object
			 * 2) Add all the various inputs from the user 
			 * 3) Execute the query on the insight core
			 */
			
			// 1) create the query builder
			SolrIndexEngineQueryBuilder queryBuilder = new SolrIndexEngineQueryBuilder();
			
			// set the solr field values to return
			// these are the necessarily fields to view and run a returned insight
			List<String> retFields = new ArrayList<String>();
			retFields.add(ID);
			retFields.add(APP_ID);
			retFields.add(APP_INSIGHT_ID);
			retFields.add(LAYOUT);
			retFields.add(STORAGE_NAME);
			retFields.add(CREATED_ON);
			retFields.add(MODIFIED_ON);
			retFields.add(LAST_VIEWED_ON);
			retFields.add(USER_ID);
			retFields.add(TAGS);
			retFields.add(VIEW_COUNT);
			retFields.add(DESCRIPTION);
			queryBuilder.setReturnFields(retFields);

			Map<String, List<String>> filterData = new HashMap<String, List<String>>();
			List<String> engineList = new Vector<String>();
			engineList.add(engineName);
			filterData.put(APP_ID, engineList);
			queryBuilder.setFilterOptions(filterData);
			
			QueryResponse res = getQueryResponse(queryBuilder.getSolrQuery(), SOLR_PATHS.SOLR_INSIGHTS_PATH);
			return res.getResults();
		}
		return null;
	}
	
	/**
	 * Deletes all insights related to a specified engine
	 * @param engineName      engine name to delete
	 * @throws SolrServerException 
	 */
	public long getNumEngineInsights(String engineName) throws SolrServerException {
		if (serverActive()) {
			LOGGER.info("Getting insights for core_engine = " + engineName);
			/*
			 * General steps:
			 * 1) Create a query builder object
			 * 2) Add all the various inputs from the user 
			 * 3) Execute the query on the insight core
			 */
			
			// 1) create the query builder
			SolrIndexEngineQueryBuilder queryBuilder = new SolrIndexEngineQueryBuilder();
			queryBuilder.setLimit(1);
			// set the solr field values to return
			// these are the necessarily fields to view and run a returned insight
			List<String> retFields = new ArrayList<String>();
			retFields.add(ID);
			queryBuilder.setReturnFields(retFields);

			Map<String, List<String>> filterData = new HashMap<String, List<String>>();
			List<String> engineList = new Vector<String>();
			engineList.add(engineName);
			filterData.put(APP_ID, engineList);
			queryBuilder.setFilterOptions(filterData);
			
			QueryResponse res = getQueryResponse(queryBuilder.getSolrQuery(), SOLR_PATHS.SOLR_INSIGHTS_PATH);
			return res.getResults().getNumFound();
		}
		return 0;
	}
	

	/**
	 * Deletes all insights within Solr
	 */
	public void deleteAllSolrData() throws IOException {
		if (serverActive()) {
			try {
				LOGGER.info("PREPARING TO DELETE ALL SOLR DATA");
				insightServer.deleteByQuery(QUERY_ALL);
//				instanceServer.deleteByQuery(QUERY_ALL);
//				appServer.deleteByQuery(QUERY_ALL);
				LOGGER.info("ALL SOLR DATA DELETED");
				insightServer.commit();
//				instanceServer.commit();
//				appServer.commit();
			} catch (SolrServerException ex) {
				throw new IOException("Failed to delete data in Solr. " + ex.getMessage(), ex);
			} catch (IOException ex) {
				throw new IOException("Failed to delete data in Solr. " + ex.getMessage(), ex);
			}
		}
	}
	
	/**
	 * Builds the suggester options
	 */
	public void buildSuggester() {
		if(serverActive()) {
			ModifiableSolrParams params = new ModifiableSolrParams();
			params.set("qt", "/suggest");
			params.set("spellchecker.build", "true");
			params.set("spellchecker.reload", "true");
			params.set("suggest.reloadAll", "true");
			try {
				insightServer.query(params);
			} catch (SolrServerException e) {
				e.printStackTrace();
			}
		}
	}
	
	/**
	 * Used to provide a uniform and Solr-friendly format for date/time
	 * @return Date format
	 */
	public static DateFormat getDateFormat() {
		return new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'");
	}
	
	/**
	 * While the solr document for passing in data is in yyyy-MM-dd'T'HH:mm:ss'Z' format, the date format 
	 * it returns those fields are in EEE MMM dd HH:mm:ss ZZZ yyyy format... not sure why it does this
	 * @return
	 */
	public static DateFormat getSolrDateFormat() {
		return new SimpleDateFormat("EEE MMM dd HH:mm:ss ZZZ yyyy");
	}
	
	/**
	 * Get the solr id based on the engine name and the unique engine id
	 * This does a concatenation between the engine name and the unique engine id
	 * @param engineId			The engine name
	 * @param engineIds				The list of the engine ids
	 * @return						The solr id for each entry in the engine id list
	 */
	public static List<String> getSolrIdFromInsightEngineId(String engineId, List<String> engineIds) {
		Vector<String> fixedQuestionIds = new Vector<String>();
		for(String id : engineIds) {
			fixedQuestionIds.add(getSolrIdFromInsightEngineId(engineId, id));
		}
		
		return fixedQuestionIds;
	}
	
	/**
	 * Get the solr id based on the engine name and the unique engine id
	 * @param engineId			The engine name
	 * @param id					The insight unique id for that engine
	 * @return						The corresponding solr unique insight id
	 */
	public static String getSolrIdFromInsightEngineId(String engineId, String id) {
		return engineId + "_" + id;
	}
	
	/////////////////// END UTILITY METHODS ///////////////////

	
	///////////////// Use for testing purposes ////////////////////
	// clearly have been doing a lot of testing this way....
	public static void main(String[] args) throws SolrServerException, IOException, KeyManagementException, NoSuchAlgorithmException, KeyStoreException {
		SolrIndexEngine.setUrl("http://localhost:8080/solr");
		SolrIndexEngine e = SolrIndexEngine.getInstance();
		e.deleteAllSolrData();
	}

	
}
