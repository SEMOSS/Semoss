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
import org.apache.solr.client.solrj.SolrQuery.SortClause;
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
import org.apache.solr.client.solrj.response.SpellCheckResponse.Suggestion;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.params.ModifiableSolrParams;

import prerna.util.Constants;
import prerna.util.DIHelper;
import prerna.util.Utility;

public class SolrIndexEngine {

	private static final Logger LOGGER = LogManager.getLogger(SolrIndexEngine.class.getName());

	public enum SOLR_PATHS {
		SOLR_INSIGHTS_PATH, SOLR_INSTANCES_PATH
	}

	private static SolrIndexEngine singleton;
	private static String url;
	private HttpSolrServer insightServer;
	private HttpSolrServer instanceServer;

	private static final String QUERY_RESPONSE = "queryResponse";
	private static final String SPELLCHECK_RESPONSE = "spellcheckResponse";
	private static final String SOLR_INSIGHTS_PATH_NAME = "/insightCore";
	private static final String SOLR_INSTANCES_PATH_NAME = "/instancesCore";
	public static final String NUM_FOUND = "numFound";

	public static final String QUERY_ALL = "*:*";
	
	// Schema Field Names
	public static final String ID = "id";
	public static final String STORAGE_NAME = "name";
	public static final String INDEX_NAME = "index_name";
	public static final String CREATED_ON = "created_on";
	public static final String MODIFIED_ON = "modified_on";
	public static final String USER_ID = "user_id";
	public static final String ENGINES = "engines";
	public static final String CORE_ENGINE = "core_engine";
	public static final String PROPERTIES = "properties";
	public static final String CORE_ENGINE_ID = "core_engine_id";
	public static final String LAYOUT = "layout";
	public static final String ANNOTATION = "annotation";
	public static final String FAVORITES_COUNT = "favorites_count";
	public static final String VIEW_COUNT = "view_count";
	public static final String TAGS = "tags";
	public static final String COMMENT = "comment";
	public static final String USER_SPECIFIED_RELATED = "user_specified_related";
	public static final String QUERY_PROJECTIONS = "query_projections";
	public static final String PARAMS = "params";
	public static final String ALGORITHMS = "algorithms";
	public static final String NON_DB_INSIGHT = "non_db_insight";
	
	// Instance specific schema fields
	public static final String VALUE = "value";
	public static final String INSTANCES = "instances";
	
	public static final String DESC = "desc";
	public static final String ASC = "asc";
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

	public static void main(String[] args) throws SolrServerException, IOException, KeyManagementException, NoSuchAlgorithmException, KeyStoreException {
		SolrIndexEngine.setUrl("http://localhost:8080/solr");
		SolrIndexEngine e = SolrIndexEngine.getInstance();
		e.deleteAllSolrData();
	}

	/**
	 * Creates one instance of the Solr engine.
	 * 
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

		if (SolrIndexEngine.url == null) {
			SolrIndexEngine.url = DIHelper.getInstance().getProperty(Constants.SOLR_URL);
		}

		insightServer = new HttpSolrServer(SolrIndexEngine.url + SOLR_INSIGHTS_PATH_NAME, httpclient);
		instanceServer = new HttpSolrServer(SolrIndexEngine.url + SOLR_INSTANCES_PATH_NAME, httpclient);
	}

	/**
	 * Used to determine if the server is active or not
	 * @return true if the server is active
	 */
	public boolean serverActive() {
		boolean isActive = true;
		try {
			insightServer.ping();
			instanceServer.ping();
		} catch (Exception e) {
			isActive = false;
		}
		return isActive;
	}
	
	/**
	 * Uses the passed in params to add a new Document into Solr
	 * @param uniqueID					new ID to be added
	 * @param fieldData					fields to be added to the new Doc
	 */
	public void addInsight(String uniqueID, Map<String, Object> fieldData) throws SolrServerException, IOException {
		if (serverActive()) {
			// create new Document
			SolrInputDocument doc = createDocument(uniqueID, fieldData);
			LOGGER.info("Adding INSIGHTS with unique ID:  " + uniqueID);
			insightServer.add(doc);
			insightServer.commit();
			LOGGER.info("UniqueID " + uniqueID + "'s INSIGHTS has been added");
			buildSuggester();
		}
	}
	
	public SolrInputDocument createDocument(String uniqueID, Map<String, Object> fieldData) {
		SolrInputDocument doc = new SolrInputDocument();
		// set document ID to uniqueID
		doc.setField(ID, uniqueID);
		// add field names and data to new Document
		for (String fieldname : fieldData.keySet()) {
			doc.setField(fieldname, fieldData.get(fieldname));
		}
		
		return doc;
	}
	
	public void addInsights(Collection<SolrInputDocument> docs) throws SolrServerException, IOException {
		if (serverActive()) {
			if(docs != null && !docs.isEmpty()) {
				LOGGER.info("Adding " + docs.size() + " documents into insight server...");
				insightServer.add(docs);
				insightServer.commit();
				LOGGER.info("Done adding documents in insight server.");
			}
		}
	}

	// make another method to add core_engine, concept, and instances
	public void addInstance(String uniqueID, Map<String, Object> fieldData) throws SolrServerException, IOException {
		if (serverActive()) {
			// create new Document
			SolrInputDocument doc = createDocument(uniqueID, fieldData);
			LOGGER.info("Adding instances with unique ID:  " + uniqueID);
			instanceServer.add(doc);
			instanceServer.commit();
			LOGGER.info("UniqueID " + uniqueID + "'s instances has been added");
		}
	}
	
	public void addInstances(Collection<SolrInputDocument> docs) throws SolrServerException, IOException {
		if (serverActive()) {
			if(!docs.isEmpty()) {
				LOGGER.info("Adding " + docs.size() + " documents into instance server...");
				instanceServer.add(docs);
				instanceServer.commit();
				LOGGER.info("Done adding documents in instance server.");
			}
		}
	}

	public SolrDocumentList getInsight(String uniqueID) throws SolrServerException, IOException {
		SolrDocumentList results = null;
		if(serverActive()) {
			// delete document based on ID
			LOGGER.info("Getting documents with id:  " + uniqueID);
			SolrIndexEngineQueryBuilder queryBuilder = new SolrIndexEngineQueryBuilder();
			queryBuilder.setSearchString(QUERY_ALL);
			Map<String, List<String>> filterForId = new HashMap<String, List<String>>();
			List<String> uniqueIDs = new ArrayList<String>();
			uniqueIDs.add(uniqueID);
			filterForId.put(ID, uniqueIDs);
			queryBuilder.setFilterOptions(filterForId);
			QueryResponse res = getQueryResponse(queryBuilder.getSolrQuery(), SOLR_PATHS.SOLR_INSIGHTS_PATH);
			results = res.getResults();
		}
		return results;
	}
	
	public SolrDocumentList getInsight(List<String> uniqueIDs) throws SolrServerException, IOException {
		SolrDocumentList results = null;
		if(serverActive()) {
			// delete document based on ID
			LOGGER.info("Getting documents with ids:  " + uniqueIDs);
			SolrIndexEngineQueryBuilder queryBuilder = new SolrIndexEngineQueryBuilder();
			queryBuilder.setSearchString(QUERY_ALL);
			Map<String, List<String>> filterForId = new HashMap<String, List<String>>();
			filterForId.put(ID, uniqueIDs);
			queryBuilder.setFilterOptions(filterForId);
			QueryResponse res = getQueryResponse(queryBuilder.getSolrQuery(), SOLR_PATHS.SOLR_INSIGHTS_PATH);
			results = res.getResults();
		}
		return results;
	}
	
	/**
	 * Deletes the specified document based on its Unique ID
	 * @param uniqueID              ID to be deleted
	 */
	public void removeInsight(List<String> uniqueIDs) throws SolrServerException, IOException {
		if (serverActive()) {
			// delete document based on ID
			LOGGER.info("Deleting documents with ids:  " + uniqueIDs);
			insightServer.deleteById(uniqueIDs);
			insightServer.commit();
			LOGGER.info("Documents with uniqueIDs: " + uniqueIDs + " have been deleted");
			buildSuggester();
		}
	}

	/**
	 * Modifies the specified document based on its Unique ID
	 * @param uniqueID              ID to be modified
	 * @param fieldsToModify        specific fields to modify
	 */
	public Map<String, Object> modifyInsight(String uniqueID, Map<String, Object> fieldsToModify) throws SolrServerException, IOException {
		if (serverActive()) {
			SolrIndexEngineQueryBuilder queryBuilder = new SolrIndexEngineQueryBuilder();
			queryBuilder.setSearchString(QUERY_ALL);
			Map<String, List<String>> filterForId = new HashMap<String, List<String>>();
			List<String> idList = new ArrayList<String>();
			idList.add(uniqueID);
			filterForId.put(ID, idList);
			queryBuilder.setFilterOptions(filterForId);
			
//			SolrDocument origDoc = queryDocument(queryBuilder).get(0);
			QueryResponse res = getQueryResponse(queryBuilder.getSolrQuery(), SOLR_PATHS.SOLR_INSIGHTS_PATH);
			SolrDocument origDoc = res.getResults().get(0);
			
			Iterator<Entry<String, Object>> iterator = origDoc.iterator();
			SolrInputDocument doc = new SolrInputDocument();
			doc.get(uniqueID); // getting the doc set based on the id

			Set<String> currFieldNames = new HashSet<String>();
			while (iterator.hasNext()) {
				// getName & getValues
				Entry<String, Object> field = iterator.next();
				String fieldName = field.getKey();
				currFieldNames.add(fieldName);
				// if modified field, grab new value
				if (fieldsToModify.containsKey(fieldName)) {
					doc.setField(fieldName, fieldsToModify.get(fieldName));
				} else {
					// if not modified field, use existing value
					doc.setField(fieldName, field.getValue());
					// also update the map to return what all the values are
					if(fieldName.equals(CREATED_ON) || fieldName.equals(MODIFIED_ON)) {
						try {
							Date d = getSolrDateFormat().parse(field.getValue() + "");
							fieldsToModify.put(fieldName, getDateFormat().format(d));
						} catch (ParseException e) {
							e.printStackTrace();
						}
					} else {
						fieldsToModify.put(fieldName, field.getValue());
					}
				}
			}
			// loop through if new fields are being added
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

	/**
	 * Refines the list of Documents in the search based on the query
	 * @param queryOptions        				options that determine how the SolrDocumentList can be queried/filtered on
	 * @return SolrDocumentList					filtered SolrDocumentList based on the results of the query
	 */
	public SolrDocumentList queryDocument(SolrIndexEngineQueryBuilder queryBuilder) throws SolrServerException, IOException {
		SolrDocumentList results = null;
		if (serverActive()) {
			SolrQuery query = queryBuilder.getSolrQuery();
			String appendedQuerySearch = "";
			QueryResponse res = getQueryResponse(query, SOLR_PATHS.SOLR_INSIGHTS_PATH);
			if(res != null && res.getResults().size() == 0) {
				// Query within the instanceCore only when the normal query returns no results
				Map<String, Object> queryResults = executeInstanceCoreQuery(query.getQuery());
				appendedQuerySearch = (String) queryResults.get(QUERY_RESPONSE);
				query.setQuery(appendedQuerySearch);
				res = getQueryResponse(query, SOLR_PATHS.SOLR_INSIGHTS_PATH);
			}
			results = res.getResults();
		}
		LOGGER.info("Returning results of search");
		return results;
	}

	private static Map<String, List<String>> getSpellCheckResponse(QueryResponse res) {
		Map<String, List<String>> spellCheckRet = new HashMap<String, List<String>>();
		SpellCheckResponse scr = res.getSpellCheckResponse();
		if (scr != null) {
			List<Collation> collations = scr.getCollatedResults();
			if (collations != null) {
				for (Collation c : collations) {
					for (Correction correction : c.getMisspellingsAndCorrections()) {
						String orig = correction.getOriginal();
						String corr = correction.getCorrection();
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
	 * Provides options for how a search can be queried, filtered, faceted,
	 * grouped by, and to find MoreLikeThisParams.MLT to narrow down the results of the return
	 * @param queryOptions               specification to query by
	 * @return result of the query
	 */
	private QueryResponse getQueryResponse(SolrQuery q, SOLR_PATHS path) throws SolrServerException {
		QueryResponse res = null;
		if (serverActive()) {
			if (path.equals(SOLR_PATHS.SOLR_INSIGHTS_PATH)) {
				res = insightServer.query(q);
				LOGGER.info("Querying within the insighCore");
			} else if (path.equals(SOLR_PATHS.SOLR_INSTANCES_PATH)) {
				res = instanceServer.query(q);
				LOGGER.info("Querying within the instanceCore");
			}
		}
		return res;
	}

	public String getUniqueConceptsForInstances(QueryResponse queryResponse) {
		String queryAddition = "";
		Set<String> noDuplication = new HashSet<String>();
		SolrDocumentList results = queryResponse.getResults();

		for (SolrDocument solrDoc : results) {
			for (Entry<String, Object> solrDocFields : solrDoc) {
				String fieldName = solrDocFields.getKey();
				if (fieldName.equals(VALUE)) {
					String concept = (String) solrDocFields.getValue();
					noDuplication.add(Utility.getInstanceName(concept));
				}
			}
		}
		for (String concept : noDuplication) {
			queryAddition += concept + " ";
		}

		LOGGER.info("Based on the instance query add this to the next query: " + queryAddition);
		return queryAddition;
	}

	public Map<String, Object> executeInstanceCoreQuery(String querySearch) throws SolrServerException {
		Map<String, Object> queryResults = new HashMap<String, Object>();
		if (serverActive()) {
			// search for instances
			SolrIndexEngineQueryBuilder queryBuilder = new SolrIndexEngineQueryBuilder();
			queryBuilder.setSearchString(querySearch);
			queryBuilder.setDefaultSearchField(INSTANCES);
			queryBuilder.setQueryType("/spell");
			queryBuilder.setSpellCheck(true);
			queryBuilder.setSpellCheckBuild(true);
			queryBuilder.setSpellCheckCollate(true);
			queryBuilder.setSpellCheckCollateExtendedResults(true);
			queryBuilder.setSpellCheckCount(4);

			SolrQuery query = queryBuilder.getSolrQuery();
			QueryResponse res = getQueryResponse(query, SOLR_PATHS.SOLR_INSTANCES_PATH);
			String appendQuery = getUniqueConceptsForInstances(res);
			//append systems to the search
			querySearch = querySearch + " " + appendQuery;
			LOGGER.info("New search query will be: " + querySearch);
			queryResults.put(QUERY_RESPONSE, querySearch);
			
			Map<String, List<String>> spellCheckResponse = getSpellCheckResponse(res);
			if (spellCheckResponse != null && !spellCheckResponse.isEmpty()) {
				queryResults.put(SPELLCHECK_RESPONSE, spellCheckResponse);
			}
		}		
		return queryResults;
	}

	public List<String> executeAutoCompleteQuery(String term) throws SolrServerException, IOException {
		List<String> insightLists = new ArrayList<String>();
		if(term != null && !term.isEmpty()) {
			SolrIndexEngineQueryBuilder queryBuilder = new SolrIndexEngineQueryBuilder();
			queryBuilder.setPreFixSearch(true);
			queryBuilder.setQueryType("/suggest");
			queryBuilder.setSpellCheck(true);
			queryBuilder.setSpellCheckBuild(true);
			queryBuilder.setSpellCheckQuery(term);
			queryBuilder.setSort(STORAGE_NAME, DESC);
			SolrQuery query = queryBuilder.getSolrQuery();
			QueryResponse resInsight = getQueryResponse(query, SOLR_PATHS.SOLR_INSIGHTS_PATH);
			insightLists.addAll(getAutoSuggestResponse(resInsight));
		}

		LOGGER.info("Suggestions include ::: " + insightLists);
		return insightLists;
	}

	private static List<String> getAutoSuggestResponse(QueryResponse res) {
		List<String> autoSuggestRet = new ArrayList<String>();
		SpellCheckResponse spellRes = res.getSpellCheckResponse();
		if(spellRes != null) {
			List<Suggestion> suggestions = res.getSpellCheckResponse().getSuggestions();
			if (suggestions != null && !suggestions.isEmpty()) {
				for (Suggestion suggestion : suggestions) {
					List<String> alternativeList = suggestion.getAlternatives();
					for (String alternative : alternativeList) {
						autoSuggestRet.add(alternative.replace("<b>", "").replace("</b>", ""));
					}
				}
			}
		}
		return autoSuggestRet;
	}
	
	/**
	 * Returns the query response and spell check based on input files 
	 * @param searchString					Search string for the query
	 * @param searchField					The field to apply for the search
	 * @param sortString					The field to sort the query return
	 * @param limitInt						The limit of insights to return
	 * @param offsetInt						The start document for the insight return
	 * @param filterData					The filter field values (must be exact match)
	 * @return 								Map<String, Object> where the keys are QUERY_RESPONSE and
	 *         								SPELLCHECK_RESPONSE to get query return and spell check values
	 *         								respectively
	 * @throws KeyManagementException
	 * @throws NoSuchAlgorithmException
	 * @throws KeyStoreException
	 * @throws SolrServerException
	 * @throws IOException
	 */
	public Map<String, Object> executeSearchQuery(String searchString, String sortString, Integer offsetInt, Integer limitInt, Map<String, List<String>> filterData) 
			throws KeyManagementException, NoSuchAlgorithmException, KeyStoreException, SolrServerException, IOException 
	{
		SolrIndexEngineQueryBuilder queryBuilder = new SolrIndexEngineQueryBuilder();
		if (searchString != null && !searchString.isEmpty()) {
			queryBuilder.setSearchString(searchString);
		}
		if (sortString != null && !sortString.isEmpty()) {
			queryBuilder.setSort(STORAGE_NAME, sortString.toLowerCase());
		}
		// always add sort by score desc
		queryBuilder.setSort(SCORE, DESC);
		
		if (offsetInt != null) {
			queryBuilder.setOffset(offsetInt);
		}
		if (limitInt != null) {
			queryBuilder.setLimit(limitInt);
		}
		// sets the field weighting for relevant value
		if(searchString != null && !searchString.isEmpty() && !searchString.equals(QUERY_ALL)) {
			queryBuilder.setDefaultDisMaxWeighting();
		}

		List<String> retFields = new ArrayList<String>();
		retFields.add(CORE_ENGINE);
		retFields.add(CORE_ENGINE_ID);
		retFields.add(LAYOUT);
		retFields.add(STORAGE_NAME);
		retFields.add(CREATED_ON);
		retFields.add(USER_ID);
		retFields.add(TAGS);
		retFields.add(SCORE);
		queryBuilder.setReturnFields(retFields);

		if(filterData != null && !filterData.isEmpty()) {
			queryBuilder.setFilterOptions(filterData);
		}
		
		queryBuilder.setQueryType("/spell");
		queryBuilder.setSpellCheck(true);
		queryBuilder.setSpellCheckBuild(true);
		queryBuilder.setSpellCheckCollate(true);
		queryBuilder.setSpellCheckCollateExtendedResults(true);
		queryBuilder.setSpellCheckCount(4);

		return searchDocument(queryBuilder);
	}

	/**
	 * Returns the query response and spell check based on input files
	 * @param queryOptions						A Map containing the query options
	 * @return 									Map<String, Object> where the keys are QUERY_RESPONSE and
	 *         									SPELLCHECK_RESPONSE to get query return and spell check values
	 *         									respectively
	 * @throws SolrServerException
	 * @throws IOException
	 */
	private Map<String, Object> searchDocument(SolrIndexEngineQueryBuilder queryBuilder) throws SolrServerException, IOException {
		Map<String, Object> searchResultMap = new HashMap<String, Object>();
		if (serverActive()) {
			// for spellcheck
			Map<String, List<String>> insightSpellCheck = null;
			Map<String, List<String>> instanceSpellCheck = null;

			SolrQuery query = queryBuilder.getSolrQuery();
			String querySearch = query.get(CommonParams.Q);
			QueryResponse res = getQueryResponse(query, SOLR_PATHS.SOLR_INSIGHTS_PATH);
			SolrDocumentList results = res.getResults();
			// get insight spell check results
			insightSpellCheck = getSpellCheckResponse(res);

			if(res != null && res.getResults().size() == 0) {
				// need to remove sorts since they might not exist
				List<SortClause> sorts = query.getSorts();
				List<SortClause> sortsCopy = new ArrayList<SortClause>();
				sortsCopy.addAll(sorts);
				for(SortClause sort : sortsCopy) {
					query.removeSort(sort);
				}

				// Query within the instanceCore
				Map<String, Object> queryResults = executeInstanceCoreQuery(querySearch);
				instanceSpellCheck = (Map<String, List<String>>) queryResults.get(SPELLCHECK_RESPONSE);

				String appendedQuerySearch = (String) queryResults.get(QUERY_RESPONSE);

				queryBuilder.removeSpellCheckParams();
				queryBuilder.setSearchString(appendedQuerySearch);
				query = queryBuilder.getSolrQuery();
				// re-add sorts
				for (SortClause sort : sortsCopy) {
					query.addSort(sort);
				}

				// query the insight core
				res = getQueryResponse(query, SOLR_PATHS.SOLR_INSIGHTS_PATH);
				results = res.getResults();
			}
			
			searchResultMap.put(QUERY_RESPONSE, results);
			searchResultMap.put(NUM_FOUND, results.getNumFound());
			searchResultMap.put(SPELLCHECK_RESPONSE, mergeCoreSuggestions(insightSpellCheck, instanceSpellCheck));
		}
		LOGGER.info("Done executing search query");
		return searchResultMap;
	}


	/**
	 * Gets the facet/count for each instance of the specified fields
	 * @param searchString						Search string for the query
	 * @param facetList							The list of fields to facet
	 * @return
	 * @throws KeyManagementException
	 * @throws NoSuchAlgorithmException
	 * @throws KeyStoreException
	 * @throws SolrServerException
	 */
	public Map<String, Map<String, Long>> executeQueryFacetResults(String searchString, List<String> facetList)
					throws KeyManagementException, NoSuchAlgorithmException, KeyStoreException, SolrServerException 
	{
		SolrIndexEngineQueryBuilder queryBuilder = new SolrIndexEngineQueryBuilder();
		if (searchString != null && !searchString.isEmpty()) {
			queryBuilder.setSearchString(searchString);
		}
		
		// sets the field weighting for relevant value
		if(searchString != null && !searchString.isEmpty() && !searchString.equals(QUERY_ALL)) {
			queryBuilder.setDefaultDisMaxWeighting();
		}
		// facet still requires a default field or it throws an error 
		queryBuilder.setDefaultSearchField(INDEX_NAME);
		queryBuilder.setFacet(true);
		queryBuilder.setFacetField(facetList);
		queryBuilder.setFacetMinCount(1);
		queryBuilder.setFacetSortCount(true);

		return facetDocument(queryBuilder);
	}

	/**
	 * Gets the facet/count for each instance of the specified fields
	 * @param queryOptions				Options that determine which fields to facet by
	 * @return faceted values of fields
	 */
	private Map<String, Map<String, Long>> facetDocument(SolrIndexEngineQueryBuilder queryBuilder) throws SolrServerException {
		Map<String, Long> innerMap = null;
		Map<String, Map<String, Long>> facetFieldMap = null;
		if (serverActive()) {
			String appendedQuerySearch = "";
			QueryResponse res = null;
			List<FacetField> facetFieldList = null;

			SolrQuery query = queryBuilder.getSolrQuery();
			String querySearch = query.get(CommonParams.Q);

			res = getQueryResponse(query, SOLR_PATHS.SOLR_INSIGHTS_PATH);
			facetFieldList = res.getFacetFields();
			if (facetFieldList != null && facetFieldList.get(0).getValueCount() == 0) {
				// need to remove sorts sinc they might not exist
				List<SortClause> sorts = query.getSorts();
				List<SortClause> sortsCopy = new ArrayList<SortClause>();
				sortsCopy.addAll(sorts);
				for(SortClause sort : sortsCopy) {
					query.removeSort(sort);
				}
				//Query within the instanceCore
				Map<String, Object> queryResults = executeInstanceCoreQuery(querySearch);
				appendedQuerySearch = (String) queryResults.get(QUERY_RESPONSE);
				query.set(CommonParams.Q, appendedQuerySearch);
				// readd sorts
				for(SortClause sort : sortsCopy) {
					query.addSort(sort);
				}
				// query the insight core
				res = getQueryResponse(query, SOLR_PATHS.SOLR_INSIGHTS_PATH);
				facetFieldList = res.getFacetFields();
			}

			if (facetFieldList != null && facetFieldList.size() > 0) {
				facetFieldMap = new LinkedHashMap<String, Map<String, Long>>();
				for (FacetField field : facetFieldList) {
					innerMap = new LinkedHashMap<String, Long>();
					String fieldName = field.getName();
					List<Count> facetInfo = field.getValues();
					if (facetInfo != null) {
						for (FacetField.Count facetInstance : facetInfo) {
							String facetName = facetInstance.getName();
							innerMap.put(facetName, facetInstance.getCount());
						}
					}
					facetFieldMap.put(fieldName, innerMap);
				}
			}
		}
		LOGGER.info("Done executing facet query");
		return facetFieldMap;
	}

	/**
	 * Gets the grouped SolrDocument based on the results of the selected fields to group by
	 * @param searchString					Search string for the query
	 * @param searchField					The field to apply for the search
	 * @param groupOffset					The offset for the group return
	 * @param groupLimit					The limit for the group return
	 * @param groupByField					The field to group by
	 * @param filterData
	 * @return
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
		SolrIndexEngineQueryBuilder queryBuilder = new SolrIndexEngineQueryBuilder();
		if (searchString != null && !searchString.isEmpty()) {
			queryBuilder.setSearchString(searchString);
		}

		queryBuilder.setGroupBy(true);
		if (groupLimit != null) {
			queryBuilder.setGroupLimit(groupLimit);
		} else {
			queryBuilder.setGroupLimit(200);
		}
		if (groupOffset != null) {
			queryBuilder.setGroupOffset(groupOffset);
		} else {
			queryBuilder.setGroupLimit(0);
		}
		if (groupSort != null && !groupSort.isEmpty()) {
			queryBuilder.setSort(STORAGE_NAME, groupSort.toLowerCase());
		}
		// always add sort by score desc
		queryBuilder.setGroupSort(SCORE, DESC);
		//TODO: need to expose number of groups to return to UI
		queryBuilder.setLimit(50);

		
		List<String> groupList = new ArrayList<String>();
		groupList.add(groupByField);
		queryBuilder.setGroupFields(groupList);
		
		// sets the field weighting for relevant value
		if(searchString != null && !searchString.isEmpty() && !searchString.equals(QUERY_ALL)) {
			queryBuilder.setDefaultDisMaxWeighting();
		}
				
		List<String> retFields = new ArrayList<String>();
		retFields.add(CORE_ENGINE);
		retFields.add(CORE_ENGINE_ID);
		retFields.add(LAYOUT);
		retFields.add(STORAGE_NAME);
		retFields.add(CREATED_ON);
		retFields.add(USER_ID);
		retFields.add(TAGS);
		retFields.add(SCORE);
		queryBuilder.setReturnFields(retFields);

		if(filterData != null && !filterData.isEmpty()) {
			queryBuilder.setFilterOptions(filterData);
		}
		
		queryBuilder.setQueryType("/spell");
		queryBuilder.setSpellCheck(true);
		queryBuilder.setSpellCheckBuild(true);
		queryBuilder.setSpellCheckCollate(true);
		queryBuilder.setSpellCheckCollateExtendedResults(true);
		queryBuilder.setSpellCheckCount(4);

		return groupDocument(queryBuilder);
	}

	/**
	 * Gets the grouped SolrDocument based on the results of the selected fields to group by
	 * @param queryOptions			options that determine how the SolrDocumentList will be
	 *           					grouped and viewed
	 * @return grouped SolrDocumentList
	 */
	private Map<String, Object> groupDocument(SolrIndexEngineQueryBuilder queryBuilder) throws SolrServerException, IOException {
		Map<String, Object> groupByResponse = new HashMap<String, Object>();
		
		if (serverActive()) {
			QueryResponse res = null;
			
			Map<String, Map<String, SolrDocumentList>> groupFieldMap = new HashMap<String, Map<String, SolrDocumentList>>();
			GroupResponse groupResponse = null;
			Map<String, SolrDocumentList> innerMap = null;
			
			//for spellcheck
			Map<String, List<String>> insightSpellCheck = null;
			Map<String, List<String>> instanceSpellCheck = null;
			
			SolrQuery query = queryBuilder.getSolrQuery();
			String querySearch = query.get(CommonParams.Q);
			res = getQueryResponse(query, SOLR_PATHS.SOLR_INSIGHTS_PATH);
			//get insight spell check results
			insightSpellCheck = getSpellCheckResponse(res);
			
			groupResponse = res.getGroupResponse();
			if(groupResponse != null && groupResponse.getValues().get(0).getValues().size() == 0) {
				// need to remove sorts since they might not exist
				List<SortClause> sorts = query.getSorts();
				List<SortClause> sortsCopy = new ArrayList<SortClause>();
				sortsCopy.addAll(sorts);
				for(SortClause sort : sortsCopy) {
					query.removeSort(sort);
				}
				
				//Query within the instanceCore
				Map<String, Object> queryResults = executeInstanceCoreQuery(querySearch);
				instanceSpellCheck = (Map<String, List<String>>) queryResults.get(SPELLCHECK_RESPONSE);
				
				String appendedQuerySearch = (String) queryResults.get(QUERY_RESPONSE);
				
				queryBuilder.removeSpellCheckParams();
				queryBuilder.setSearchString(appendedQuerySearch);
				query = queryBuilder.getSolrQuery();
				// readd sorts
				for(SortClause sort : sortsCopy) {
					query.addSort(sort);
				}
				
				// query the insight core
				res = getQueryResponse(query, SOLR_PATHS.SOLR_INSIGHTS_PATH);
				groupResponse = res.getGroupResponse();
			}

			if (groupResponse != null) { 
				groupFieldMap = new HashMap<String, Map<String, SolrDocumentList>>();
				for (GroupCommand gc : groupResponse.getValues()) {
					innerMap = new HashMap<String, SolrDocumentList>();
					String groupBy = gc.getName();
					List<Group> groups = gc.getValues();
					if (groups != null) {
						for (Group g : groups) {
							SolrDocumentList solrDocs = g.getResult();
							innerMap.put(g.getGroupValue(), solrDocs);
						}
					}
					groupFieldMap.put(groupBy, innerMap);
				}
			}
			
			groupByResponse.put(QUERY_RESPONSE, groupFieldMap);
			groupByResponse.put(SPELLCHECK_RESPONSE, mergeCoreSuggestions(insightSpellCheck, instanceSpellCheck));
		}

		LOGGER.info("Done executing group by query");
		return groupByResponse;
	}
	
	private Map<String, List<String>> mergeCoreSuggestions(Map<String, List<String>> insightCoreSpelling, Map<String, List<String>> instanceCoreSpelling) {
		Map<String, List<String>> allResponse = new HashMap<String, List<String>>(); 
		//add insight suggestions
		if (insightCoreSpelling != null && !insightCoreSpelling.isEmpty()) {
			allResponse.putAll(insightCoreSpelling);
		}
		//add instance suggestions
		if (instanceCoreSpelling != null && !instanceCoreSpelling.isEmpty()) {
			for (String searchString : instanceCoreSpelling.keySet()) {
				if(allResponse.containsKey(searchString)) {
					List<String> currSpellingCorrections = allResponse.get(searchString);
					List<String> newSpellingCorrections = instanceCoreSpelling.get(searchString);
					for(String newSpelling : newSpellingCorrections) {
						if(!currSpellingCorrections.contains(newSpelling)) {
							currSpellingCorrections.add(newSpelling);
						}
					}
				} else {
					allResponse.put(searchString, instanceCoreSpelling.get(searchString));
				}
			}
		}
		return allResponse;
	}
	
	/**
	 * Used to verify if specified engine is already contained within Solr If
	 * the method returns a false then the engine needs to be added to Solr
	 * @param engineName           name of the engine to verify existence
	 * @return true if the engine already exists
	 */
	public boolean containsEngine(String engineName) {
		// check if db currently exists
		LOGGER.info("checking if engine " + engineName + " needs to be added to solr");
		SolrIndexEngineQueryBuilder builder = new SolrIndexEngineQueryBuilder();
		builder.setSearchString(engineName);
		builder.setDefaultSearchField(SolrIndexEngine.CORE_ENGINE);
		builder.setLimit(1);
		
		SolrDocumentList queryRet = null;
		try {
			queryRet = queryDocument(builder);
		} catch (SolrServerException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		if (queryRet != null && queryRet.size() != 0) {
			LOGGER.info("Engine " + engineName + " already exists inside solr");
		} else {
			LOGGER.info("queryRet.size() = 0 ... so add engine");
		}
		return (queryRet.size() != 0);
	}

	/**
	 * Deletes all insights related to a specified engine
	 * @param engineName      engine name to delete
	 */
	public void deleteEngine(String engineName) {
		if (serverActive()) {
			try {
				LOGGER.info("DELETING ENGINE FROM SOLR " + engineName);
				String query = CORE_ENGINE + ":" + engineName;
				LOGGER.info("deleted query is " + query);
				insightServer.deleteByQuery(query);
				insightServer.commit();
				LOGGER.info("successfully removed " + engineName + " from solr" + engineName);
			} catch (SolrServerException e1) {
				e1.printStackTrace();
			} catch (IOException e1) {
				e1.printStackTrace();
			}
		}
	}

	/**
	 * Deletes all insights within Solr
	 */
	public void deleteAllSolrData() throws IOException {
		if (serverActive()) {
			try {
				LOGGER.info("PREPARING TO DELETE ALL SOLR DATA");
				insightServer.deleteByQuery(QUERY_ALL);
				instanceServer.deleteByQuery(QUERY_ALL);
				LOGGER.info("ALL SOLR DATA DELETED");
				insightServer.commit();
				instanceServer.commit();
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
	
	public static DateFormat getSolrDateFormat() {
		return new SimpleDateFormat("EEE MMM dd HH:mm:ss ZZZ yyyy");
	}
	
	public static List<String> getSolrIdFromInsightEngineId(String engineName, List<String> engineIds) {
		Vector<String> fixedQuestionIds = new Vector<String>();
		for(String id : engineIds) {
			fixedQuestionIds.add(getSolrIdFromInsightEngineId(engineName, id));
		}
		
		return fixedQuestionIds;
	}
	
	public static String getSolrIdFromInsightEngineId(String engineName, String id) {
		return engineName + "_" + id;
	}
	
}
