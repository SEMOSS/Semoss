package prerna.solr;

import java.io.IOException;
import java.security.KeyManagementException;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
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
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.params.FacetParams;
import org.apache.solr.common.params.GroupParams;
import org.apache.solr.common.params.MoreLikeThisParams;
import org.apache.solr.common.util.NamedList;

import prerna.util.Constants;
import prerna.util.DIHelper;

public class SolrIndexEngine {

	private static SolrIndexEngine singleton;
	private static String url;
	private HttpSolrServer server;

	// Common Search
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
	public static final String MIN_COUNT = FacetParams.FACET_MINCOUNT;

	// GroupBy
	public static final String GROUPBY = GroupParams.GROUP;
	public static final String GROUP_FIELD = GroupParams.GROUP_FIELD;
	public static final String GROUP_LIMIT = GroupParams.GROUP_LIMIT;
	public static final String GROUP_OFFSET = GroupParams.GROUP_OFFSET;

	// MoreLikeThis
	public static final String MLT = MoreLikeThisParams.MLT;
	public static final String MLT_FIELD = MoreLikeThisParams.SIMILARITY_FIELDS;
	public static final String MLT_MINDF = MoreLikeThisParams.MIN_DOC_FREQ; // Minimum DocumentFrequency
	public static final String MLT_MINTF = MoreLikeThisParams.MIN_TERM_FREQ; // Minimum Term Frequency
	public static final String MLT_WORD_LENGHT = MoreLikeThisParams.MIN_WORD_LEN; // Minimum Word Length
	public static final String MLT_QUERY_TERM = MoreLikeThisParams.MAX_QUERY_TERMS; // Max Query Terms
	public static final String MLT_COUNT = MoreLikeThisParams.DOC_COUNT;
	public static final String MLT_BOOST = MoreLikeThisParams.BOOST;

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

	public static void setUrl(String url) {
		SolrIndexEngine.url = url;
	}

	
	public static void main(String[] args) throws SolrServerException, IOException, KeyManagementException,	NoSuchAlgorithmException, KeyStoreException {
//
//		SolrIndexEngine.setUrl("http://localhost:8080/solr");
//		SolrIndexEngine e = SolrIndexEngine.getInstance();
//		Map<String, Object> queryEngine = new HashMap<>();
//
//		queryEngine.put(QUERY, "Show the matrix regression scatter plot to estimate Domestic Revenue");
//		queryEngine.put(SEARCH_FIELD, "all_text");
//		queryEngine.put(MLT, "true");
//		queryEngine.put(SET_ROWS, 2);
//		List<String> mltField = new ArrayList<>();
//		mltField.add("all_text");
//		queryEngine.put(MLT_FIELD, mltField);
//		queryEngine.put(MLT_MINDF, 1);
//		queryEngine.put(MLT_MINTF, 1);
//		queryEngine.put(MLT_COUNT, 2);
//
//		SolrDocumentList results = new SolrDocumentList();
//		QueryResponse x = e.getQueryResponse(queryEngine);
//
//		NamedList mlt = (NamedList) x.getResponse().get("moreLikeThis");
//		for (int i = 0; i < mlt.size(); i++) {
//			String name = mlt.getName(i);
//			System.out.println("Name: " + name);
//			SolrDocumentList val = (SolrDocumentList) mlt.getVal(i);
//			Iterator<SolrDocument> valIter = val.iterator();
//			while (valIter.hasNext()) {
//				System.out.println(valIter.next());
//			}
//		}
	}

	public static SolrIndexEngine getInstance() throws KeyManagementException, NoSuchAlgorithmException, KeyStoreException {
		if (singleton == null) {
			singleton = new SolrIndexEngine();
		}
		return singleton;
	}

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
			server.add(doc);
			server.commit();
		}
	}

	public void removeDocument(List<String> uniqueID) throws SolrServerException, IOException {
		if (serverActive()) {
			// delete document based on ID
			server.deleteById(uniqueID);
			server.commit();
		}
	}

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
			server.add(doc);
			server.commit();
		}
	}

	public SolrDocumentList queryDocument(Map<String, Object> queryOptions) throws SolrServerException, IOException {
		SolrDocumentList results = null;
		if (serverActive()) {
			// report number of results found from query
			QueryResponse res = getQueryResponse(queryOptions);
			results = res.getResults();
		}
		return results;
	}

	@SuppressWarnings("null")
	public Map<String, Map<String, Long>> facetDocument(Map<String, Object> queryOptions) throws SolrServerException {
		Map<String, Long> innerMap = null;
		Map<String, Map<String, Long>> facetFieldMap = null;
		if (serverActive()) {
			// report number of results found from query
			QueryResponse res = getQueryResponse(queryOptions);
			List<FacetField> facetFieldList = res.getFacetFields();
			if (facetFieldList != null && facetFieldList.size() > 0) {
				innerMap = new HashMap<String, Long>();
				for (FacetField field : facetFieldList) {
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
		return facetFieldMap;
	}

	@SuppressWarnings("null")
	public Map<String, Map<String, SolrDocumentList>> groupDocument(Map<String, Object> queryOptions)
			throws SolrServerException, IOException {
		Map<String, SolrDocumentList> innerMap = null;
		Map<String, Map<String, SolrDocumentList>> groupFieldMap = null;
		QueryResponse x = getQueryResponse(queryOptions);
		GroupResponse z = x.getGroupResponse();
		for (GroupCommand gc : z.getValues()) {
			String groupBy = gc.getName();
			List<Group> Y = gc.getValues();
			if (Y != null) {
				for (Group g : Y) {
					SolrDocumentList solrDocs = g.getResult();
					innerMap.put(g.getGroupValue(), solrDocs);
				}
			}
			groupFieldMap.put(groupBy, innerMap);
		}
		return groupFieldMap;
	}

	@SuppressWarnings("rawtypes")
	public Map<String, Object> mltDocument(Map<String, Object> queryOptions) throws SolrServerException {
		// returning instance of field, solrdoc list of mlt
		Map<String, Object> mltMap = new HashMap<>();
		SolrDocument results = null;
		QueryResponse x = getQueryResponse(queryOptions);
		NamedList mlt = (NamedList) x.getResponse().get("moreLikeThis");
		for (int i = 0; i < mlt.size(); i++) {
			String name = mlt.getName(i);
			SolrDocumentList val = (SolrDocumentList) mlt.getVal(i);
			Iterator<SolrDocument> valIter = val.iterator();
			while (valIter.hasNext()) {
				results = valIter.next();
			}
			mltMap.put(name, results);
		}
		return mltMap;
	}

	@SuppressWarnings("unchecked")
	private QueryResponse getQueryResponse(Map<String, Object> queryOptions) throws SolrServerException {
		QueryResponse res = null;
		if (serverActive()) {

			SolrQuery Q = new SolrQuery();
			// Default
			Q.setQuery("*:*");

			// COMMON FILTERS
			// q & df- If query is specified, match based on relevance will be
			// returned
			// Default will be set to search for everything
			if (queryOptions.get(QUERY) != null) {
				String f = (String) queryOptions.get(QUERY);
				Q.set(QUERY, f);
				if (queryOptions.get(SEARCH_FIELD) != null) {
					String s = (String) queryOptions.get(SEARCH_FIELD);
					Q.set(SEARCH_FIELD, s);
				} else {
					Q.set(SEARCH_FIELD, "all_text");
				}
			}

			// fq - filters search based on exact match
			if (queryOptions.get(FITLER_QUERY) != null) {
				Map<String, String> query = (Map<String, String>) queryOptions.get(FITLER_QUERY);
				for (String fQuery : query.keySet()) {
					Q.addFilterQuery(fQuery + ":" + query.get(fQuery));
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
						}
					} else if (fsort.equals(asc)) {
						for (String fields : sortField) {
							Q.setSort(fields, SolrQuery.ORDER.asc);
						}
					} else if (fsort.equals(relevance)) {
						for (String fields : sortField) {
							Q.removeSort(fields);
						}
					}
				}
			}

			// start - returns list of insights starting from the specified int
			if (queryOptions.get(SET_START) != null) {
				int numStart = (int) queryOptions.get(SET_START);
				Q.setStart(numStart);
			}

			// rows - returns specified int of insights
			if (queryOptions.get(SET_ROWS) != null) {
				int numRows = (int) queryOptions.get(SET_ROWS);
				Q.setRows(numRows);
			} else {
				Q.setRows(200);
			}

			// fl - this is determining which fields in the schema will be
			// returned
			if (queryOptions.get(ADD_FIELD) != null) {
				List<String> fields = (List<String>) queryOptions.get(ADD_FIELD);
				for (String f : fields) {
					Q.addField(f);
				}
			}

			//////// FACET FILTERS

			// group - set group to true
			if (queryOptions.get(FACET) != null) {
				Q.setFacet(true);
				// facet.field - calculates count of total insights
				if (queryOptions.get(FACET_FIELD) != null) {
					List<String> fieldList = (List<String>) queryOptions.get(FACET_FIELD);
					for (String field : fieldList) {
						Q.set(FACET_FIELD + "=" + field);
					}

				} else if (queryOptions.get(FACET_FIELD) == null) {
					LOGGER.error("MUST CONTAIN FACET_FIELD");
				}
			}

			// minCount - specifies minimum number that are necessary for the
			// facet field to be visible
			// no maxCount
			if (queryOptions.get(MIN_COUNT) != null) {
				int numRows = (int) queryOptions.get(MIN_COUNT);
				Q.setFacetMinCount(numRows);
			}

			//////// GROUPBY

			// group - sets group to true
			if (queryOptions.get(GROUPBY) != null) {
				Q.set(GROUPBY, "true");

				// groupField - specifies the field(s) to group by
				if (queryOptions.get(GROUP_FIELD) != null) {
					List<String> fieldList = (List<String>) queryOptions.get(GROUP_FIELD);
					for (String field : fieldList) {
						Q.set(GROUP_FIELD, field);
					}
				}
			}

			// groupLimit - specifies the # of results to return
			if (queryOptions.get(GROUP_LIMIT) != null) {
				int numGroupLimit = (int) queryOptions.get(GROUP_LIMIT);
				Q.set(GROUP_LIMIT, numGroupLimit);

			}

			// groupOffset - specifies the starting return result
			if (queryOptions.get(GROUP_OFFSET) != null) {
				int numOffSet = (int) queryOptions.get(GROUP_OFFSET);
				Q.set(GROUP_OFFSET, numOffSet);
			}

			//////// MORELIKETHIS
			// MLT - enable MoreLikeThis results
			if (queryOptions.get(MLT) != null) {
				Q.set(MLT, "true");
				// fields to use for similarity
				if (queryOptions.get(MLT_FIELD) != null) {
					List<String> fieldList = (List<String>) queryOptions.get(MLT_FIELD);
					for (String field : fieldList) {
						Q.set(MLT_FIELD, field);
					}
					// frequency words will be ignored if not included in set
					// number of documents
					if (queryOptions.get(MLT_MINDF) != null) {
						int ignoreTerms = (int) queryOptions.get(MLT_MINDF);
						Q.set(MLT_MINDF, ignoreTerms);
					}
					// the frequency below which terms will be ignored in the
					// source doc
					if (queryOptions.get(MLT_MINTF) != null) {
						int ignoreTerms = (int) queryOptions.get(MLT_MINTF);
						Q.set(MLT_MINTF, ignoreTerms);
					}
				} else if (queryOptions.get(MLT_FIELD) == null || queryOptions.get(MLT_MINDF) == null
						|| queryOptions.get(MLT_MINTF) == null) {
					LOGGER.error("MUST CONTAIN FL, MINDF, and MINTF QUERIES");
				}
			}

			// sets the min word length requirement to be returned
			if (queryOptions.get(MLT_WORD_LENGHT) != null) {
				int ignoreTerms = (int) queryOptions.get(MLT_WORD_LENGHT);
				Q.set(MLT_WORD_LENGHT, ignoreTerms);
			}

			// sets max number of MLT terms that will be included in any
			// generated query
			if (queryOptions.get(MLT_QUERY_TERM) != null) {
				int ignoreTerms = (int) queryOptions.get(MLT_QUERY_TERM);
				Q.set(MLT_QUERY_TERM, ignoreTerms);
			}

			// sets the number of documents that will be returned
			if (queryOptions.get(MLT_COUNT) != null) {
				int ignoreTerms = (int) queryOptions.get(MLT_COUNT);
				Q.set(MLT_COUNT, ignoreTerms);
			}

			// sets whether to boost terms in query based on 'score' or not
			if (queryOptions.get(MLT_BOOST) != null) {
				Q.set(MLT_BOOST, "true");
			}

			System.out.println("query is ::: " + Q.getQuery());

			// report number of results found from query
			res = server.query(Q);
		}
		return res;

	}

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

	public static DateFormat getDateFormat() {
		return new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'");
	}

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
