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
import org.apache.solr.common.params.MoreLikeThisParams;
import org.apache.solr.common.params.SpellingParams;
import org.apache.solr.common.util.NamedList;

import prerna.util.Constants;
import prerna.util.DIHelper;

public class SolrIndexEngine {

	private static SolrIndexEngine singleton;
	private static String url;
	private HttpSolrServer server;

	private static final String QUERY_RESPONSE = "queryResponse";
	private static final String SPELLCHECK_RESPONSE = "spellcheckResponse";
	
	// Common Search
	public static final String REQUEST_HANDLER = CommonParams.QT;
	public static final String QUERY = CommonParams.Q;
	public static final String SEARCH_FIELD = CommonParams.DF;
	public static final String ADD_FIELD = CommonParams.FL;
	public static final String SET_START = CommonParams.START;
	public static final String SET_ROWS = CommonParams.ROWS;
	public static final String FITLER_QUERY = CommonParams.FQ;
	public static final String FIELD_SORT = CommonParams.SORT;

	// Facet
	public static final String FACET = FacetParams.FACET;
	public static final String FACET_FIELD = FacetParams.FACET_FIELD;
	public static final String FACET_QUERY = FacetParams.FACET_QUERY;
	public static final String FACET_MIN_COUNT = FacetParams.FACET_MINCOUNT;
	public static final String FACET_SORT_COUNT = FacetParams.FACET_SORT_COUNT;
	
	// GroupBy
	public static final String GROUPBY = GroupParams.GROUP;
	public static final String GROUP_FIELD = GroupParams.GROUP_FIELD;
	public static final String GROUP_LIMIT = GroupParams.GROUP_LIMIT;
	public static final String GROUP_OFFSET = GroupParams.GROUP_OFFSET;

	// MoreLikeThis
	public static final String MLT = MoreLikeThisParams.MLT;
	public static final String MLT_FIELD = MoreLikeThisParams.SIMILARITY_FIELDS;
	public static final String MLT_MINDF = MoreLikeThisParams.MIN_DOC_FREQ; // MinimumDocumentFrequency
	public static final String MLT_MINTF = MoreLikeThisParams.MIN_TERM_FREQ; // MinimumTermFrequency
	public static final String MLT_WORD_LENGHT = MoreLikeThisParams.MIN_WORD_LEN; // MinimumWordLength
	public static final String MLT_QUERY_TERM = MoreLikeThisParams.MAX_QUERY_TERMS; // MaxQueryTerms
	public static final String MLT_COUNT = MoreLikeThisParams.DOC_COUNT;
	public static final String MLT_BOOST = MoreLikeThisParams.BOOST;

	// SpellCheck
	public static final String SPELL_CHECK = SpellingParams.SPELLCHECK_PREFIX;
	public static final String COLLATE = SpellingParams.SPELLCHECK_COLLATE;
	public static final String EXTENDED_COLLATE = SpellingParams.SPELLCHECK_COLLATE_EXTENDED_RESULTS;

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

	private static final Logger LOGGER = LogManager.getLogger(SolrIndexEngine.class.getName());

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
		System.out.println("solrindexengine: " + e);
		Map<String, Object> queryEngine = new HashMap<>();

		queryEngine.put(QUERY, "matr");
		queryEngine.put(SEARCH_FIELD, "all_text");
		queryEngine.put(SPELL_CHECK, "on");

		QueryResponse x = singleton.getQueryResponse(queryEngine);
		Map<String, List<String>> y = getSpellCheckResponse(x);
		System.out.println(y);
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

		server = new HttpSolrServer(SolrIndexEngine.url, httpclient);
	}

	/**
	 * Uses the passed in params to add a new Document into Solr
	 * 
	 * @param uniqueID
	 *            - new ID to be added
	 * @param fieldData
	 *            - fields to be added to the new Doc
	 */
	public void addDocument(String uniqueID, Map<String, Object> fieldData) throws SolrServerException, IOException {
		if (serverActive()) {
			// create new Document
			SolrInputDocument doc = new SolrInputDocument();
			// set document ID to uniqueID
			doc.setField("id", uniqueID);
			// add field names and data to new Document
			for (String fieldname : fieldData.keySet()) {
				doc.setField(fieldname, fieldData.get(fieldname));
			}
			LOGGER.info("Adding document with unique ID:  " + uniqueID);
			server.add(doc);
			server.commit();
			LOGGER.info("UniqueID " + uniqueID + "'s doc has been added");
		}
	}

	/**
	 * Deletes the specified document based on its Unique ID
	 * 
	 * @param uniqueID
	 *            - ID to be deleted
	 */
	public void removeDocument(List<String> uniqueID) throws SolrServerException, IOException {
		if (serverActive()) {
			// delete document based on ID
			LOGGER.info("Deleting document:  " + uniqueID);
			server.deleteById(uniqueID);
			server.commit();
			LOGGER.info("UniqueID " + uniqueID + "'s doc has been deleted");
		}
	}

	/**
	 * Modifies the specified document based on its Unique ID
	 * 
	 * @param uniqueID - ID to be modified
	 * @param fieldsToModify - specific fields to modify
	 */
	public void modifyFields(String uniqueID, Map<String, Object> fieldsToModify)
			throws SolrServerException, IOException {
		if (serverActive()) {
			Map<String, Object> queryMap = new HashMap<String, Object>();
			queryMap.put(QUERY, ID + ":" + uniqueID);
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
			server.add(doc);
			server.commit();
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

	public Map<String, Object> executeQuery(Map<String, Object> queryOptions) throws SolrServerException, IOException {
		Map<String, Object> searchResultMap = new HashMap<String, Object>();
		if (serverActive()) {
			// report number of results found from query
			LOGGER.info("Reporting number of results found from query");
			QueryResponse res = getQueryResponse(queryOptions);
			SolrDocumentList results = res.getResults();
			searchResultMap.put(QUERY_RESPONSE, results);
			Map<String, List<String>> spellCheckResponse = getSpellCheckResponse(res);
			if(spellCheckResponse != null && spellCheckResponse.isEmpty()) {
				searchResultMap.put(SPELLCHECK_RESPONSE, spellCheckResponse);
			}
		}
		LOGGER.info("Returning results of search");
		return searchResultMap;
	}

	/**
	 * Gets the facet/count for each instance of the specified fields
	 * 
	 * @param queryOptions
	 *            - options that determine which fields to facet by
	 * @return faceted instances of fields
	 */
	public Map<String, Map<String, Long>> facetDocument(Map<String, Object> queryOptions) throws SolrServerException {
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
							innerMap.put(facetInstance.getName(), facetInstance.getCount());
						}
					}
					facetFieldMap.put(fieldName, innerMap);
				}
			}
		}
		// returning: field name string (ie.core_engine), MAP[ instance of th
		// field name (ie. movie_db), count of instance of field name (ie. 79)]
		LOGGER.info("Returning facetDocument's field name string, instance of field name string, and long count for the field name");
		return facetFieldMap;

	}

	/**
	 * Gets the grouped SolrDocument based on the results of the selected fields
	 * to group by
	 * 
	 * @param queryOptions- options that determine how the SolrDocumentList will be grouped and viewed
	 * @return grouped SolrDocumentList
	 */
	public Map<String, Map<String, SolrDocumentList>> groupDocument(Map<String, Object> queryOptions) throws SolrServerException, IOException {
		Map<String, SolrDocumentList> innerMap = null;
		Map<String, Map<String, SolrDocumentList>> groupFieldMap = null;

		if (serverActive()) {
			QueryResponse response = getQueryResponse(queryOptions);
			GroupResponse groupResponse = response.getGroupResponse();

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
		}

		LOGGER.info("Returning SolrDocumentList for Group Search");
		return groupFieldMap;
	}

	/**
	 * Gets the 'Most Like This' SolrDocuments based on document's similarity to
	 * the specified query
	 * 
	 * @param queryOptions
	 *            - options that determine, amongst other things, how similar a
	 *            SolrDocumentList is to a specified query and what fields to
	 *            compare similarity on
	 * @return list of SolrDocumentList that are similar to the specified query
	 */
	public Map<String, SolrDocumentList> mltDocument(Map<String, Object> queryOptions) throws SolrServerException {
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
	 * grouped by, and to find MLT to narrow down the results of the return
	 * 
	 * @param queryOptions
	 *            - specification to query by
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
			if (queryOptions.get(QUERY) != null) {
				LOGGER.info("Changing the default query search");
				String f = (String) queryOptions.get(QUERY);
				Q.set(QUERY, f);
				LOGGER.info("Query search specified to: " + f);
				if (queryOptions.get(SEARCH_FIELD) != null) {
					String s = (String) queryOptions.get(SEARCH_FIELD);
					Q.set(SEARCH_FIELD, s);
					LOGGER.info("Search for the query specifically in field: " + s);
				} else {
					Q.set(SEARCH_FIELD, "all_text");
					LOGGER.info("Search field has no specified...search all_text");
				}
			}

			// fq - filters search based on exact match
			if (queryOptions.get(FITLER_QUERY) != null) {
				Map<String, String> query = (Map<String, String>) queryOptions.get(FITLER_QUERY);
				for (String fQuery : query.keySet()) {
					Q.addFilterQuery(fQuery + ":" + query.get(fQuery));
					LOGGER.info("Filter query search based on an exact match of query: " + query.get(fQuery));
				}
			}

			// sorts - orders list asc/desc or by relevance
			if (queryOptions.get(FIELD_SORT) != null) {
				Map<String, List<String>> sort = (Map<String, List<String>>) queryOptions.get(FIELD_SORT);
				for (String fsort : sort.keySet()) {
					List<String> sortField = sort.get(fsort);
					String desc = "Descending";
					String asc = "Ascending";
					String relevance = "Relevance";
					if (fsort.equals(desc)) {
						for (String fields : sortField) {
							Q.setSort(fields, SolrQuery.ORDER.desc);
							LOGGER.info("Sorting list of documents in descending order");
						}
					} else if (fsort.equals(asc)) {
						for (String fields : sortField) {
							Q.setSort(fields, SolrQuery.ORDER.asc);
							LOGGER.info("Sorting list of documents in ascending order");
						}
					} else if (fsort.equals(relevance)) {
						for (String fields : sortField) {
							Q.removeSort(fields);
							LOGGER.info("Sorting list of documents in order of relevance");
						}
					}
				}
			}

			// // start - returns list of insights starting from the specified
			// int
			// if (queryOptions.get(SET_START) != null) {
			// int numStart = (int) queryOptions.get(SET_START);
			// Q.setStart(numStart);
			// }

			// rows - returns specified int of insights
			if (queryOptions.get(SET_ROWS) != null) {
				int numRows = (int) queryOptions.get(SET_ROWS);
				Q.setRows(numRows);
				LOGGER.info("Specified number of rows/insights to be returned is: " + numRows);
			} else {
				Q.setRows(200);
				LOGGER.info("Default number of row returns is set to 200");
			}

			// fl - this is determining which fields in the schema will be
			// returned
			if (queryOptions.get(ADD_FIELD) != null) {
				List<String> fields = (List<String>) queryOptions.get(ADD_FIELD);
				for (String f : fields) {
					Q.addField(f);
					LOGGER.info("For the list of doc that will be returned, the field: " + f + " will be returned");
				}
			}

			//////// FACET FILTERS

			// group - set group to true
			if ( queryOptions.get(FACET) != null && (boolean) queryOptions.get(FACET) || queryOptions.get(FACET_FIELD) != null) {
				Q.setFacet(true);
				LOGGER.info("Facet set to true");
				// facet.field - calculates count of total insights
				if (queryOptions.get(FACET_FIELD) != null) {
					List<String> fieldList = (List<String>) queryOptions.get(FACET_FIELD);
					Q.addFacetField(fieldList.toArray(new String[] {}));
					LOGGER.info("For the list of doc that will be returned, the field: " + fieldList + " will be returned");
				} else if (queryOptions.get(FACET_FIELD) == null) {
					LOGGER.error("MUST CONTAIN FACET_FIELD");
				}
				
				if(queryOptions.get(FACET_MIN_COUNT) != null) {
					Q.setFacetMinCount( (int) queryOptions.get(FACET_MIN_COUNT));
				}
				if(queryOptions.get(FACET_SORT_COUNT) != null && (boolean) queryOptions.get(FACET_SORT_COUNT) ) {
					Q.set(FACET_SORT_COUNT, true);
				}
			}

			// // minCount - specifies minimum number that are necessary for the
			// // facet field to be visible
			// // no maxCount
			// if (queryOptions.get(MIN_COUNT) != null) {
			// int numRows = (int) queryOptions.get(MIN_COUNT);
			// Q.setFacetMinCount(numRows);
			// }

			//////// GROUPBY

			// group - sets group to true
			if (queryOptions.get(GROUPBY) != null || queryOptions.get(GROUP_FIELD) != null) {
				Q.set(GROUPBY, "true");
				LOGGER.info("GroupBy set to true");
				// groupField - specifies the field(s) to group by
				if (queryOptions.get(GROUP_FIELD) != null) {
					List<String> groupByList = (List<String>) queryOptions.get(GROUP_FIELD);
					Q.set(GROUP_FIELD, groupByList.toArray(new String[] {}));
					LOGGER.info("Group doc by field: " + groupByList);
				}
			}

			// groupLimit - specifies the # of results to return
			if (queryOptions.get(GROUP_LIMIT) != null) {
				int numGroupLimit = (int) queryOptions.get(GROUP_LIMIT);
				Q.set(GROUP_LIMIT, numGroupLimit);
				LOGGER.info("Number of doc returns is: " + numGroupLimit);

			}

			// groupOffset - specifies the starting return result
			if (queryOptions.get(GROUP_OFFSET) != null) {
				int numOffSet = (int) queryOptions.get(GROUP_OFFSET);
				Q.set(GROUP_OFFSET, numOffSet);
				LOGGER.info("Starting number for doc return is: " + numOffSet);
			}

			//////// MORELIKETHIS
			// MLT - enable MoreLikeThis results
			if (queryOptions.get(MLT) != null || queryOptions.get(MLT_FIELD) != null) {
				Q.set(MLT, "true");
				LOGGER.info("MLT set to true");
				// fields to use for similarity
				if (queryOptions.get(MLT_FIELD) != null) {
					List<String> mltList = (List<String>) queryOptions.get(MLT_FIELD);
					Q.set(MLT_FIELD, mltList.toArray(new String[] {}));
					LOGGER.info("MLT field to use for similarities is for MLT: " + mltList);
					// frequency words will be ignored if not included in set
					// number of documents
					if (queryOptions.get(MLT_MINDF) != null) {
						int ignoreDoc = (int) queryOptions.get(MLT_MINDF);
						Q.set(MLT_MINDF, ignoreDoc);
						LOGGER.info(
								"number frequency words will be ignored if not included in set number of documents for MLT: "
										+ ignoreDoc);
					}
					// the frequency below which terms will be ignored in the
					// source doc
					if (queryOptions.get(MLT_MINTF) != null) {
						int ignoreTerms = (int) queryOptions.get(MLT_MINTF);
						Q.set(MLT_MINTF, ignoreTerms);
						LOGGER.info("the frequency below which terms will be ignored in the source doc for MLT: "
								+ ignoreTerms);
					}
				} else if (queryOptions.get(MLT_FIELD) == null || queryOptions.get(MLT_MINDF) == null
						|| queryOptions.get(MLT_MINTF) == null) {
					LOGGER.error("MUST CONTAIN FL, MINDF, and MINTF QUERIES");
				}
			}

			// sets the min word length requirement to be returned
			if (queryOptions.get(MLT_WORD_LENGHT) != null) {
				int minLength = (int) queryOptions.get(MLT_WORD_LENGHT);
				Q.set(MLT_WORD_LENGHT, minLength);
				LOGGER.info("the min word length requirement to be returned is for MLT: " + minLength);
			}

			// sets max number of MLT terms that will be included in any
			// generated query
			if (queryOptions.get(MLT_QUERY_TERM) != null) {
				int maxTerms = (int) queryOptions.get(MLT_QUERY_TERM);
				Q.set(MLT_QUERY_TERM, maxTerms);
				LOGGER.info(
						"max number of MLT terms that will be included in any generated query for MLT: " + maxTerms);
			}

			// sets the number of documents that will be returned
			if (queryOptions.get(MLT_COUNT) != null) {
				int mltCount = (int) queryOptions.get(MLT_COUNT);
				Q.set(MLT_COUNT, mltCount);
				LOGGER.info("number of document that will be returned for MLT: " + mltCount);
			}

			//////// SPELLCHECK
			if (queryOptions.get(SPELL_CHECK) != null) {
				Q.set(SPELL_CHECK, "on");
				Q.set(REQUEST_HANDLER, "/spell");
				Q.set(COLLATE, "true");
				Q.set(EXTENDED_COLLATE, "true");

				LOGGER.info("SPELL_CHECK set to true");
			}

			System.out.println("query is ::: " + Q.getQuery());

			// report number of results found from query
			res = server.query(Q);
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
				server.deleteByQuery("*:*");
				LOGGER.info("ALL SOLR DATA DELETED");
				server.commit();
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
	 * @param engineName
	 *            - engine name to delete
	 */
	public void deleteEngine(String engineName) {
		if (serverActive()) {
			try {
				LOGGER.info("DELETING ENGINE FROM SOLR " + engineName);
				String query = CORE_ENGINE + ":" + engineName;
				LOGGER.info("deleted query is " + query);
				server.deleteByQuery(query);
				server.commit();
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
		querySolr.put(SolrIndexEngine.QUERY, engineName);
		querySolr.put(SolrIndexEngine.SEARCH_FIELD, SolrIndexEngine.CORE_ENGINE);
		querySolr.put(SolrIndexEngine.SET_ROWS, 1);
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
			server.ping();
		} catch (Exception e) {
			isActive = false;
		}
		return isActive;
	}
}
