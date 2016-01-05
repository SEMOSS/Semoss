package prerna.solr;

import java.io.IOException;
import java.security.KeyManagementException;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Set;

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
import org.apache.solr.common.params.FacetParams;
import org.apache.solr.common.params.GroupParams;
import org.apache.solr.common.params.HighlightParams;
import org.apache.solr.common.params.MoreLikeThisParams;
import org.apache.solr.common.params.SpellingParams;
import org.apache.solr.common.util.NamedList;

import prerna.util.Constants;
import prerna.util.DIHelper;

public class SolrIndexEngine {

	private static final Logger LOGGER = LogManager.getLogger(SolrIndexEngine.class.getName());

	private static SolrIndexEngine singleton;
	private static String url;
	private HttpSolrServer insightServer;
	private HttpSolrServer instanceServer;

	private static final String QUERY_RESPONSE = "queryResponse";
	private static final String SPELLCHECK_RESPONSE = "spellcheckResponse";
	private static final String SOLR_INSIGHTS_PATH = "/insightCore";
	private static final String SOLR_INSTANCES_PATH = "/instancesCore";

	// Schema Field Names
	public static final String ID = "id";
	public static final String NAME = "name";
	public static final String CREATED_ON = "created_on";
	public static final String MODIFIED_ON = "modified_on";
	public static final String USER_ID = "user_id";
	public static final String ENGINES = "engines";
	public static final String CORE_ENGINE = "core_engine";
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
	public static final String CONCEPT = "concept";
	public static final String INSTANCES = "instances";


	/**
	 * Sets a constant url for Solr
	 * 
	 * @param url - string of Solr's url
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

		insightServer = new HttpSolrServer(SolrIndexEngine.url + SOLR_INSIGHTS_PATH, httpclient);
		instanceServer = new HttpSolrServer(SolrIndexEngine.url + SOLR_INSTANCES_PATH, httpclient);
	}

	/**
	 * Uses the passed in params to add a new Document into Solr
	 * 
	 * @param uniqueID
	 *            - new ID to be added
	 * @param fieldData
	 *            - fields to be added to the new Doc
	 */
	public void addInsight(String uniqueID, Map<String, Object> fieldData) throws SolrServerException, IOException {
		if (serverActive()) {
			// create new Document
			SolrInputDocument doc = new SolrInputDocument();
			// set document ID to uniqueID
			doc.setField(ID, uniqueID);
			// add field names and data to new Document
			for (String fieldname : fieldData.keySet()) {
				doc.setField(fieldname, fieldData.get(fieldname));
			}
			LOGGER.info("Adding INSIGHTS with unique ID:  " + uniqueID);
			insightServer.add(doc);
			insightServer.commit();
			LOGGER.info("UniqueID " + uniqueID + "'s INSIGHTS has been added");
		}
	}

	//make another method to add core_engine, concept, and instances
	public void addInstance(String uniqueID, Map<String, Object> fieldData) throws SolrServerException, IOException {
		if (serverActive()) {
			// create new Document
			SolrInputDocument doc = new SolrInputDocument();
			// set document ID to uniqueID
			doc.setField(ID, uniqueID);
			// add field names and data to new Document
			for (String fieldName : fieldData.keySet()) {
				doc.setField(fieldName, fieldData.get(fieldName));
			}
			LOGGER.info("Adding instances with unique ID:  " + uniqueID);
			instanceServer.add(doc);
			instanceServer.commit();
			LOGGER.info("UniqueID " + uniqueID + "'s instances has been added");
		}
	}

	public void queryInstance (String uniqueID, Map<String, Object> fieldData){
		SolrDocumentList solrE = null;
		Map<String, Object> query = new HashMap<>();
		List<String> queryFields = new ArrayList<>();
		for(String fieldName: fieldData.keySet()){
			queryFields.add(fieldName);
		}
		queryFields.add(ID);
		query.put(CommonParams.FL, queryFields);
		try {
			solrE = queryDocument(query);
		} catch (SolrServerException | IOException e) {
			e.printStackTrace();
		}
	}

	/**
	 * Deletes the specified document based on its Unique ID
	 * @param uniqueID - ID to be deleted
	 */
	public void removeInsight(List<String> uniqueID) throws SolrServerException, IOException {
		if (serverActive()) {
			// delete document based on ID
			LOGGER.info("Deleting document:  " + uniqueID);
			insightServer.deleteById(uniqueID);
			insightServer.commit();
			LOGGER.info("UniqueID " + uniqueID + "'s doc has been deleted");
		}
	}

	/**
	 * Modifies the specified document based on its Unique ID
	 * @param uniqueID - ID to be modified
	 * @param fieldsToModify - specific fields to modify
	 */
	public void modifyInsight(String uniqueID, Map<String, Object> fieldsToModify) throws SolrServerException, IOException {
		if (serverActive()) {
			Map<String, Object> queryMap = new HashMap<String, Object>();
			queryMap.put(CommonParams.Q, ID + ":" + uniqueID);
			SolrDocument origDoc = queryDocument(queryMap).get(0);
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
				}
			}
			// loop through if new fields are being added
			for (String newField : fieldsToModify.keySet()) {
				if (!currFieldNames.contains(newField)) {
					doc.setField(newField, fieldsToModify.get(newField));
				}
			}
			// when committing, automatically overrides existing field values
			// with the new ones
			LOGGER.info("Modifying document:  " + uniqueID);
			insightServer.add(doc);
			insightServer.commit();
			LOGGER.info("UniqueID " + uniqueID + "'s doc has been modified");
		}
	}

	/**
	 * Refines the list of Documents in the search based on the query
	 * 
	 * @param queryOptions
	 *            - options that determine how the SolrDocumentList can be
	 *            queried/filtered on
	 * @return filtered SolrDocumentList based on the results of the query
	 */
	public SolrDocumentList queryDocument(Map<String, Object> queryOptions) throws SolrServerException, IOException {
		SolrDocumentList results = null;
		if (serverActive()) {
			// report number of results found from query
			LOGGER.info("Reporting number of results found from query");
			QueryResponse res = getQueryResponse(queryOptions);
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
						if(spellCheckRet.containsKey(orig)) {
							List<String> suggestions = spellCheckRet.get(orig);
							suggestions.add(corr);
						} else {
							List<String> suggestions = new ArrayList<String>();
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
	 * @param queryOptions 				specification to query by
	 * @return result of the query
	 */
	private QueryResponse getQueryResponse(Map<String, Object> queryOptions) throws SolrServerException {
		QueryResponse res = null;
		if (serverActive()) {

			SolrQuery Q = new SolrQuery();
			// Default
			Q.setQuery("*:*");
			LOGGER.info("Docs will be queried by default query");

			// COMMON FILTERS
			// q & df- If query is specified, match based on relevance will be
			// returned
			// Default will be set to search for everything
			if (queryOptions.get(CommonParams.Q) != null) {
				LOGGER.info("Changing the default query search");
				String f = (String) queryOptions.get(CommonParams.Q);
				Q.set(CommonParams.Q, f);
				LOGGER.info("Query search specified to: " + f);
				if (queryOptions.get(CommonParams.DF) != null) {
					String s = (String) queryOptions.get(CommonParams.DF);
					Q.set(CommonParams.DF, s);
					LOGGER.info("Search for the query specifically in field: " + s);
				} else {
					Q.set(CommonParams.DF, "all_text");
					LOGGER.info("Search field has no specified...search all_text");
				}
			}

			// fq - filters search based on exact match
			if (queryOptions.get(CommonParams.FQ) != null) {
				Map<String, String> query = (Map<String, String>) queryOptions.get(CommonParams.FQ);
				for (String fQuery : query.keySet()) {
					Q.addFilterQuery(fQuery + ":" + query.get(fQuery));
					LOGGER.info("Filter query search based on an exact match of query: " + query.get(fQuery));
				}
			}

			// sorts - orders list asc/desc or by relevance
			if (queryOptions.get(CommonParams.SORT) != null) {
				String sort = (String) queryOptions.get(CommonParams.SORT);
				String desc = "desc";
				String asc = "asc";
				String relevance = "relevance";
				if (sort.equals(desc)) {
					Q.setSort(NAME, SolrQuery.ORDER.desc);
					LOGGER.info("Sorting list of documents in descending order");
				} else if (sort.equals(asc)) {
					Q.setSort(NAME, SolrQuery.ORDER.asc);
					LOGGER.info("Sorting list of documents in ascending order");
				} else if (sort.equals(relevance)) {
					Q.removeSort(NAME);
					LOGGER.info("Sorting list of documents in order of relevance");
				}

			}

			// rows - returns specified int of insights
			if (queryOptions.get(CommonParams.ROWS) != null) {
				int numRows = (int) queryOptions.get(CommonParams.ROWS);
				Q.setRows(numRows);
				LOGGER.info("Specified number of rows/insights to be returned is: " + numRows);
			} else {
				Q.setRows(200);
				LOGGER.info("Default number of row returns is set to 200");
			}

			// fl - this is determining which fields in the schema will be
			// returned
			if (queryOptions.get(CommonParams.FL) != null) {
				List<String> fields = (List<String>) queryOptions.get(CommonParams.FL);
				for (String f : fields) {
					Q.addField(f);
				}
				LOGGER.info("For the list of doc that will be returned, the field: " + fields + " will be returned");
			}

			//////// FacetParams.FACET FILTERS

			// group - set group to true
			if ( queryOptions.get(FacetParams.FACET) != null && (boolean) queryOptions.get(FacetParams.FACET) || queryOptions.get(FacetParams.FACET_FIELD) != null) {
				Q.setFacet(true);
				LOGGER.info("Facet set to true");
				// facet.field - calculates count of total insights
				if (queryOptions.get(FacetParams.FACET_FIELD) != null) {
					List<String> fieldList = (List<String>) queryOptions.get(FacetParams.FACET_FIELD);
					Q.addFacetField(fieldList.toArray(new String[] {}));
					LOGGER.info("For the list of doc that will be returned, the field: " + fieldList + " will be returned");
				} else if (queryOptions.get(FacetParams.FACET_FIELD) == null) {
					LOGGER.error("MUST CONTAIN FacetParams.FACET_FIELD");
				}

				if(queryOptions.get(FacetParams.FACET_MINCOUNT) != null) {
					Q.setFacetMinCount( (int) queryOptions.get(FacetParams.FACET_MINCOUNT));
				}
				if(queryOptions.get(FacetParams.FACET_SORT_COUNT) != null && (boolean) queryOptions.get(FacetParams.FACET_SORT_COUNT) ) {
					Q.set(FacetParams.FACET_SORT_COUNT, true);
				}
			}


			//////// GroupParams.GROUP

			// group - sets group to true
			if (queryOptions.get(GroupParams.GROUP) != null || queryOptions.get(GroupParams.GROUP_FIELD) != null) {
				Q.set(GroupParams.GROUP, "true");
				LOGGER.info("GroupBy set to true");
				// groupField - specifies the field(s) to group by
				if (queryOptions.get(GroupParams.GROUP_FIELD) != null) {
					List<String> groupByList = (List<String>) queryOptions.get(GroupParams.GROUP_FIELD);
					Q.set(GroupParams.GROUP_FIELD, groupByList.toArray(new String[] {}));
					LOGGER.info("Group doc by field: " + groupByList);
				}
			}

			// groupLimit - specifies the # of results to return
			if (queryOptions.get(GroupParams.GROUP_LIMIT) != null) {
				int numGroupLimit = (int) queryOptions.get(GroupParams.GROUP_LIMIT);
				Q.set(GroupParams.GROUP_LIMIT, numGroupLimit);
				LOGGER.info("Number of doc returns is: " + numGroupLimit);

			}

			// groupOffset - specifies the starting return result
			if (queryOptions.get(GroupParams.GROUP_OFFSET) != null) {
				int numOffSet = (int) queryOptions.get(GroupParams.GROUP_OFFSET);
				Q.set(GroupParams.GROUP_OFFSET, numOffSet);
				LOGGER.info("Starting number for doc return is: " + numOffSet);
			}

			//////// MORELIKETHIS
			// MoreLikeThisParams.MLT - enable MoreLikeThis results
			if (queryOptions.get(MoreLikeThisParams.MLT) != null || queryOptions.get(MoreLikeThisParams.SIMILARITY_FIELDS) != null) {
				Q.set(MoreLikeThisParams.MLT, "true");
				LOGGER.info("MoreLikeThisParams.MLT set to true");
				// fields to use for similarity
				if (queryOptions.get(MoreLikeThisParams.SIMILARITY_FIELDS) != null) {
					List<String> mltList = (List<String>) queryOptions.get(MoreLikeThisParams.SIMILARITY_FIELDS);
					Q.set(MoreLikeThisParams.SIMILARITY_FIELDS, mltList.toArray(new String[] {}));
					LOGGER.info("MoreLikeThisParams.MLT field to use for similarities is for MoreLikeThisParams.MLT: " + mltList);
					// frequency words will be ignored if not included in set
					// number of documents
					if (queryOptions.get(MoreLikeThisParams.MIN_DOC_FREQ) != null) {
						int ignoreDoc = (int) queryOptions.get(MoreLikeThisParams.MIN_DOC_FREQ);
						Q.set(MoreLikeThisParams.MIN_DOC_FREQ, ignoreDoc);
						LOGGER.info(
								"number frequency words will be ignored if not included in set number of documents for MoreLikeThisParams.MLT: "
										+ ignoreDoc);
					}
					// the frequency below which terms will be ignored in the
					// source doc
					if (queryOptions.get(MoreLikeThisParams.MIN_TERM_FREQ) != null) {
						int ignoreTerms = (int) queryOptions.get(MoreLikeThisParams.MIN_TERM_FREQ);
						Q.set(MoreLikeThisParams.MIN_TERM_FREQ, ignoreTerms);
						LOGGER.info("the frequency below which terms will be ignored in the source doc for MoreLikeThisParams.MLT: "
								+ ignoreTerms);
					}
				} else if (queryOptions.get(MoreLikeThisParams.SIMILARITY_FIELDS) == null || queryOptions.get(MoreLikeThisParams.MIN_DOC_FREQ) == null
						|| queryOptions.get(MoreLikeThisParams.MIN_TERM_FREQ) == null) {
					LOGGER.error("MUST CONTAIN FL, MINDF, and MINTF QUERIES");
				}
			}

			// sets the min word length requirement to be returned
			if (queryOptions.get(MoreLikeThisParams.MIN_WORD_LEN) != null) {
				int minLength = (int) queryOptions.get(MoreLikeThisParams.MIN_WORD_LEN);
				Q.set(MoreLikeThisParams.MIN_WORD_LEN, minLength);
				LOGGER.info("the min word length requirement to be returned is for MoreLikeThisParams.MLT: " + minLength);
			}

			// sets max number of MoreLikeThisParams.MLT terms that will be included in any
			// generated query
			if (queryOptions.get(MoreLikeThisParams.MAX_QUERY_TERMS) != null) {
				int maxTerms = (int) queryOptions.get(MoreLikeThisParams.MAX_QUERY_TERMS);
				Q.set(MoreLikeThisParams.MAX_QUERY_TERMS, maxTerms);
				LOGGER.info(
						"max number of MoreLikeThisParams.MLT terms that will be included in any generated query for MoreLikeThisParams.MLT: " + maxTerms);
			}

			// sets the number of documents that will be returned
			if (queryOptions.get(MoreLikeThisParams.DOC_COUNT) != null) {
				int mltCount = (int) queryOptions.get(MoreLikeThisParams.DOC_COUNT);
				Q.set(MoreLikeThisParams.DOC_COUNT, mltCount);
				LOGGER.info("number of document that will be returned for MoreLikeThisParams.MLT: " + mltCount);
			}

			//////// SPELLCHECK
			if (queryOptions.get(SpellingParams.SPELLCHECK_PREFIX) != null) {
				Q.set(SpellingParams.SPELLCHECK_PREFIX, "on");
				Q.set(CommonParams.QT, "/spell");
				Q.set(SpellingParams.SPELLCHECK_COLLATE, "true");
				Q.set(SpellingParams.SPELLCHECK_COLLATE_EXTENDED_RESULTS, "true");
				Q.set(SpellingParams.SPELLCHECK_ONLY_MORE_POPULAR, "true"); //suggestions will be returned by popularity
				Q.set(SpellingParams.SPELLCHECK_COUNT, "4"); //# of suggestions to return

				LOGGER.info("SpellingParams.SPELLCHECK_PREFIX set to true");
			}

			//////// HIGHLIGHTING
			if(queryOptions.get(HighlightParams.HIGHLIGHT) != null){
				Q.set(HighlightParams.HIGHLIGHT, "true");
				Q.set(HighlightParams.FIELDS, "*");
			}

			System.out.println("query is ::: " + Q.getQuery());

			// report number of results found from query
			res = insightServer.query(Q);
		}
		return res;
	}

	/**
	 * Deletes all insights within Solr
	 */
	public void deleteAllSolrData() throws IOException {
		if (serverActive()) {
			try {
				LOGGER.info("PREPARING TO DELETE ALL SOLR DATA");
				insightServer.deleteByQuery("*:*");
				instanceServer.deleteByQuery("*:*");
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
	 * Deletes all insights related to a specified engine
	 * 
	 * @param engineName- engine name to delete
	 */
	public void deleteEngine(String engineName) {
		if (serverActive()) {
			try {
				LOGGER.info("DELETING ENGINE FROM SOLR " + engineName);
				String query = CORE_ENGINE + ":" + engineName;
				LOGGER.info("deleted query is " + query);
				insightServer.deleteByQuery(query);
				insightServer.commit();
				LOGGER.info("successfully removed from solr" + engineName);
			} catch (SolrServerException e1) {
				e1.printStackTrace();
			} catch (IOException e1) {
				e1.printStackTrace();
			}
		}
	}

	/**
	 * Used to verify if specified engine is already contained within Solr If
	 * the method returns a false then the engine needs to be added to Solr
	 * 
	 * @param engineName
	 *            - name of the engine to verify existence
	 * @return true if the engine already exists
	 */
	public boolean containsEngine(String engineName) {
		// check if db currently exists
		LOGGER.info(engineName + " is being added ");
		Map<String, Object> querySolr = new HashMap<String, Object>();
		querySolr.put(CommonParams.Q, engineName);
		querySolr.put(CommonParams.DF, SolrIndexEngine.CORE_ENGINE);
		querySolr.put(CommonParams.ROWS, 1);
		SolrDocumentList queryRet = null;
		try {
			queryRet = queryDocument(querySolr);
		} catch (SolrServerException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		if (queryRet.size() != 0) {
			LOGGER.info("queryRet.size() = " + (queryRet.size() != 0));
		} else {
			LOGGER.info("queryRet.size() = " + (queryRet.size() != 0) + "...so add engine");
		}
		return (queryRet.size() != 0);
	}

	/**
	 * Used to provide a uniform and Solr-friendly format for date/time
	 * 
	 * @return Date format
	 */
	public static DateFormat getDateFormat() {
		return new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'");
	}

	/**
	 * Used to determine if the server is active or not
	 * 
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
	 * Returns the query response and spell check based on input files
	 * @param searchString						Search string for the query
	 * @param searchField						The field to apply for the search
	 * @param sortString						The field to sort the query return
	 * @param limitInt 							The limit of insights to return
	 * @param offsetInt 						The start document for the insight return
	 * @param filterData						The filter field values (must be exact match)
	 * @return									Map<String, Object> where the keys are QUERY_RESPONSE and SPELLCHECK_RESPONSE to get query return
	 * 											and spell check values respectively
	 * @throws KeyManagementException
	 * @throws NoSuchAlgorithmException
	 * @throws KeyStoreException
	 * @throws SolrServerException
	 * @throws IOException
	 */
	public Map<String, Object> executeSearchQuery(String searchString, String searchField, String sortString, Integer offsetInt, Integer limitInt, Map<String, List<String>> filterData) 
			throws KeyManagementException, NoSuchAlgorithmException, KeyStoreException, SolrServerException, IOException {
		Map<String, Object> queryData = new HashMap<String, Object>();
		if(searchString != null && !searchString.isEmpty()) {
			queryData.put(CommonParams.Q, searchString);
			queryData.put(HighlightParams.HIGHLIGHT, true); 
		}
		if(searchField != null && !searchField.isEmpty()) {
			queryData.put(CommonParams.DF, searchField);
		}
		if(sortString != null && !sortString.isEmpty()) {
			queryData.put(CommonParams.SORT, sortString);
		}
		if(offsetInt != null) {
			queryData.put(CommonParams.START, offsetInt);
		}
		if(limitInt != null) {
			queryData.put(CommonParams.ROWS, limitInt);
		}

		List<String> retFields = new ArrayList<String>();
		retFields.add(CORE_ENGINE);
		retFields.add(CORE_ENGINE_ID);
		retFields.add(LAYOUT);
		retFields.add(NAME);
		retFields.add(CREATED_ON);
		retFields.add(USER_ID);
		retFields.add(TAGS);
		queryData.put(CommonParams.FL, retFields);

		addFilterResultsToQueryMap(queryData, filterData);

		return SolrIndexEngine.getInstance().executeSearchQuery(queryData);
	}

	/**
	 * Returns the query response and spell check based on input files
	 * @param queryOptions					A Map containing the query options
	 * @return								Map<String, Object> where the keys are QUERY_RESPONSE and SPELLCHECK_RESPONSE to get query return
	 * 										and spell check values respectively
	 * @throws SolrServerException
	 * @throws IOException
	 */
	private Map<String, Object> executeSearchQuery(Map<String, Object> queryOptions) throws SolrServerException, IOException {
		Map<String, Object> searchResultMap = new HashMap<String, Object>();
		if (serverActive()) {
			// report number of results found from query
			LOGGER.info("Running search query now...");
			// adding return for spell checks
			queryOptions.put(SpellingParams.SPELLCHECK_PREFIX, true);
			QueryResponse res = getQueryResponse(queryOptions);
			SolrDocumentList results = res.getResults();
			searchResultMap.put(QUERY_RESPONSE, results);
			Map<String, List<String>> spellCheckResponse = getSpellCheckResponse(res);
			if(spellCheckResponse != null && !spellCheckResponse.isEmpty()) {
				searchResultMap.put(SPELLCHECK_RESPONSE, spellCheckResponse);
			}
		}
		LOGGER.info("Returning results of search");
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
	public Map<String, Map<String, Long>> executeQueryFacetResults(String searchString, String searchField, List<String> facetList) 
			throws KeyManagementException, NoSuchAlgorithmException, KeyStoreException, SolrServerException {

		Map<String, Object> queryData = new HashMap<String, Object>();

		if(searchString != null && !searchString.isEmpty()) {
			queryData.put(CommonParams.Q, searchString);
		}
		if(searchField != null && !searchField.isEmpty()){
			queryData.put(CommonParams.DF, searchField);
		}
		queryData.put(FacetParams.FACET_FIELD, facetList);
		queryData.put(FacetParams.FACET, true);
		queryData.put(FacetParams.FACET_MINCOUNT, 1);
		queryData.put(FacetParams.FACET_SORT_COUNT, true);

		return SolrIndexEngine.getInstance().facetDocument(queryData);
	}

	/**
	 * Gets the facet/count for each instance of the specified fields
	 * @param queryOptions				options that determine which fields to facet by
	 * @return 							faceted values of fields
	 */
	private Map<String, Map<String, Long>> facetDocument(Map<String, Object> queryOptions) throws SolrServerException {
		Map<String, Long> innerMap = null;
		Map<String, Map<String, Long>> facetFieldMap = null;
		if (serverActive()) {
			// report number of results found from query
			QueryResponse res = getQueryResponse(queryOptions);
			List<FacetField> facetFieldList = res.getFacetFields();
			if (facetFieldList != null && facetFieldList.size() > 0) {
				facetFieldMap = new LinkedHashMap<String, Map<String, Long>>();
				for (FacetField field : facetFieldList) {
					innerMap = new LinkedHashMap<String, Long>();
					String fieldName = field.getName();
					List<Count> facetInfo = field.getValues();
					if (facetInfo != null) {
						for (FacetField.Count facetInstance : facetInfo) {
							String local = "LocalMasterDatabase";
							String facetName = facetInstance.getName();
							if (!Objects.equals(facetName, local)){
								//String prerna = "prerna.ui.components.playsheets.";
								String prerna = ".";
								if(facetName.contains(prerna)){
									int endIndex = ((String) facetName).lastIndexOf(prerna) + 1;
									if (endIndex != -1) {
										String newString = ((String) facetName).substring(endIndex, ((String) facetName).length());
										innerMap.put(newString, facetInstance.getCount());									
									}
								} else{
									innerMap.put(facetName, facetInstance.getCount());
								}
							}
						}
					}
					facetFieldMap.put(fieldName, innerMap);
				}
			}
		}
		LOGGER.info("Returning facetDocument's field name string, instance of field name string, and long count for the field name");
		return facetFieldMap;
	}

	/**
	 * Gets the grouped SolrDocument based on the results of the selected fields
	 * to group by
	 * @param searchString						Search string for the query
	 * @param searchField						The field to apply for the search
	 * @param groupOffset						The offset for the group return
	 * @param groupLimit						The limit for the group return
	 * @param groupByField						The field to group by
	 * @param filterData 
	 * @return
	 * @throws KeyManagementException
	 * @throws NoSuchAlgorithmException
	 * @throws KeyStoreException
	 * @throws SolrServerException
	 * @throws IOException
	 */
	public Map<String, Object> executeQueryGroupBy(String searchString, String searchField, Integer groupOffset, Integer groupLimit, String groupByField, Map<String, List<String>> filterData)
			throws KeyManagementException, NoSuchAlgorithmException, KeyStoreException, SolrServerException, IOException {
		Map<String, Object> queryData = new HashMap<>();
		if(searchString != null && !searchString.isEmpty()) {
			queryData.put(CommonParams.Q, searchString);
		}
		if(searchField != null && !searchField.isEmpty()) {
			queryData.put(CommonParams.DF, searchString);
		}

		if(groupLimit != null) {
			queryData.put(GroupParams.GROUP_LIMIT, groupLimit);
		} else {
			queryData.put(GroupParams.GROUP_LIMIT, 200);
		}
		if(groupOffset != null) {
			queryData.put(GroupParams.GROUP_OFFSET, groupOffset);
		} else {
			queryData.put(GroupParams.GROUP_OFFSET, 0);
		}
		List<String> groupList = new ArrayList<String>();
		groupList.add(groupByField);
		queryData.put(GroupParams.GROUP_FIELD, groupList);

		List<String> retFields = new ArrayList<String>();
		retFields.add(CORE_ENGINE);
		retFields.add(CORE_ENGINE_ID);
		retFields.add(LAYOUT);
		retFields.add(NAME);
		retFields.add(CREATED_ON);
		retFields.add(USER_ID);
		retFields.add(TAGS);
		queryData.put(CommonParams.FL, retFields);
		
		addFilterResultsToQueryMap(queryData, filterData);

		return SolrIndexEngine.getInstance().groupDocument(queryData);
	}

	/**
	 * Gets the grouped SolrDocument based on the results of the selected fields
	 * to group by
	 * @param queryOptions- options that determine how the SolrDocumentList will be grouped and viewed
	 * @return grouped SolrDocumentList
	 */
	private Map<String, Object> groupDocument(Map<String, Object> queryOptions) throws SolrServerException, IOException {
		Map<String, Object> groupByResponse = new HashMap<String, Object>();

		if (serverActive()) {
			LOGGER.info("Reporting group by query now...");
			// adding return for spell checks
			queryOptions.put(SpellingParams.SPELLCHECK_PREFIX, true);
			QueryResponse response = getQueryResponse(queryOptions);

			Map<String, Map<String, SolrDocumentList>> groupFieldMap = null;
			GroupResponse groupResponse = response.getGroupResponse();
			Map<String, SolrDocumentList> innerMap = null;
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
				groupByResponse.put(QUERY_RESPONSE, groupFieldMap);
			}
			
			Map<String, List<String>> spellCheckResponse = getSpellCheckResponse(response);
			if(spellCheckResponse != null && !spellCheckResponse.isEmpty()) {
				groupByResponse.put(SPELLCHECK_RESPONSE, spellCheckResponse);
			}
		}

		LOGGER.info("Returning SolrDocumentList for Group Search");
		return groupByResponse;
	}

	/**
	 * Gets the 'Most Like This' SolrDocuments based on document's similarity to the specified query
	 * @param searchString						Search string for the query
	 * @param searchField						The field to apply for the search
	 * @param docFrequency						The minimum doc frequency for each return doc
	 * @param termFrequency						The minimum term frequency for each return doc
	 * @param mltOffset							The offset for the response return
	 * @param mltLimit							The limit for the response return
	 * @param mltField							The field to execute mlt on
	 * @return
	 * @throws KeyManagementException
	 * @throws NoSuchAlgorithmException
	 * @throws KeyStoreException
	 * @throws SolrServerException
	 */
	public Map<String, SolrDocumentList> executeQueryMLTResponse(String searchString, String searchField, Integer docFrequency, Integer termFrequency, Integer mltOffset, Integer mltLimit, String mltField) 
			throws KeyManagementException, NoSuchAlgorithmException, KeyStoreException, SolrServerException{
		Map<String, Object> queryData = new HashMap<>();

		if(searchString != null && !searchString.isEmpty()) {
			queryData.put(CommonParams.Q, searchString);
		}
		if(searchField != null && !searchField.isEmpty()) {
			queryData.put(CommonParams.DF, searchString);
		}
		if(docFrequency != null) {
			queryData.put(MoreLikeThisParams.MIN_DOC_FREQ, docFrequency);
		} else {
			queryData.put(MoreLikeThisParams.MIN_DOC_FREQ, 1);
		}
		if(termFrequency != null) {
			queryData.put(MoreLikeThisParams.MIN_TERM_FREQ, termFrequency);
		} else {
			queryData.put(MoreLikeThisParams.MIN_TERM_FREQ, 1);
		}
		//NEED TO ADD LIMIT AND OFFSET FOR THIS... WHAT IS THE PARAM NAME FOR THIS

		
		List<String> retFields = new ArrayList<String>();
		retFields.add(CORE_ENGINE);
		retFields.add(CORE_ENGINE_ID);
		retFields.add(LAYOUT);
		retFields.add(NAME);
		retFields.add(CREATED_ON);
		retFields.add(USER_ID);
		retFields.add(TAGS);
		queryData.put(CommonParams.FL, retFields);
		
		List<String> mltList = new ArrayList<String>();
		mltList.add(mltField);
		queryData.put(MoreLikeThisParams.SIMILARITY_FIELDS, mltList);

		return SolrIndexEngine.getInstance().mltDocument(queryData);	
	}

	/**
	 * Gets the 'Most Like This' SolrDocuments based on document's similarity to the specified query
	 * @param queryOptions 					options that determine, amongst other things, how similar a
	 *           							SolrDocumentList is to a specified query and what fields to
	 *            							compare similarity on
	 * @return list of SolrDocumentList that are similar to the specified query
	 */
	private Map<String, SolrDocumentList> mltDocument(Map<String, Object> queryOptions) throws SolrServerException {
		// returning instance of field, solrdoc list of mlt
		Map<String, SolrDocumentList> mltMap = null;
		if (serverActive()) {
			QueryResponse x = getQueryResponse(queryOptions);
			NamedList mlt = (NamedList) x.getResponse().get("moreLikeThis");
			if (mlt != null && mlt.size() > 0) {
				mltMap = new HashMap<String, SolrDocumentList>();
				for (int i = 0; i < mlt.size(); i++) {
					String name = mlt.getName(i);
					SolrDocumentList val = (SolrDocumentList) mlt.getVal(i);
					mltMap.put(name, val);
				}
			}
		}
		LOGGER.info("Returning SolrDocumentList for More Like This search");
		return mltMap;
	}
	
	private void addFilterResultsToQueryMap(Map<String, Object> queryData, Map<String, List<String>> filterData) {
		Map<String, String> filterMap = new HashMap<String, String>();
		if (filterData != null) {
			for (String fieldName : filterData.keySet()) {
				List<String> filterValuesList = filterData.get(fieldName);
				StringBuilder filterStr = new StringBuilder();
				for (int i = 0; i < filterValuesList.size(); i++) {
					if (i == filterValuesList.size() - 1) {
						filterStr.append(filterValuesList.get(i));
					} else {
						filterStr.append(filterValuesList.get(i) + " OR ");
					}
				}
				filterMap.put(fieldName, "(" + filterStr.toString() + ")");
			}
		}
		queryData.put(CommonParams.FQ, filterMap);
	}

}
