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
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.common.SolrInputDocument;

import prerna.util.Constants;
import prerna.util.DIHelper;

public class SolrIndexEngine {

	private static SolrIndexEngine singleton;
	private static String url;
	private HttpSolrServer server;

	public static final String SET_DEFAULT = "setDefault"; // setQuery
	public static final String SEARCH_FIELD = "df";
	public static final String ADD_FIELD = "addField";
	public static final String SET_START = "setStart";
	public static final String SET_ROWS = "setRows";
	public static final String FACET_FIELD = "facetField"; // addFacetField
	public static final String FITLER_QUERY = "filterQuery"; // addFilterQuery
	public static final String REMOVE_FACET = "removeFacet"; // removeFacetField
	public static final String STATS_FACETS = "statsFieldFacets"; // addStatsFieldFacets
	public static final String NUMERIC_FACET = "numericRangeFacet"; // addNumericRangeFacet
	public static final String PREFIX_FACET = "prefixFacet"; // setFacetPrefix
	public static final String FACET_QUERY = "facetQuery"; // addFacetQuery
	public static final String FACET_LIMIT = "facetLimit"; // setFacetLimit
	public static final String MIN_COUNT = "facetMinCount"; // setFacetMinCount
	public static final String FACET_MISSING = "facetMissing"; // setFacetMissing
	public static final String FIELD_SORT = "facetSort"; // setFacetSort

	// schema field names
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

	public static void main(String[] args) throws SolrServerException, IOException, KeyManagementException,
			NoSuchAlgorithmException, KeyStoreException {
		// try {
		// logger.info("deleting all of solr");
		// SolrIndexEngine.setUrl("http://localhost:8080/solr");
		// SolrIndexEngine e = SolrIndexEngine.getInstance();
		// e.deleteAllSolrData();
		// logger.info("done deleting all of solr");
		// } catch (KeyManagementException e) {
		// e.printStackTrace();
		// } catch (NoSuchAlgorithmException e) {
		// e.printStackTrace();
		// } catch (KeyStoreException e) {
		// e.printStackTrace();
		// } catch (IOException e) {
		// e.printStackTrace();
		// }

		SolrIndexEngine.setUrl("http://localhost:8080/solr");
		SolrIndexEngine e = SolrIndexEngine.getInstance();
		Map<String, Object> queryEngine = new HashMap<>();
		List<String> facetField = new ArrayList<>();
		facetField.add(CORE_ENGINE);
		queryEngine.put(FACET_FIELD, facetField);

		// add all query requests to engine
		Map<String, Long> x = e.facetDocument(queryEngine);
		System.out.println(x);
	}

	public static SolrIndexEngine getInstance()
			throws KeyManagementException, NoSuchAlgorithmException, KeyStoreException {
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
			queryMap.put(SET_DEFAULT, ID + ":" + uniqueID);
			SolrDocument origDoc = queryDocument(queryMap).get(0);
			Iterator<Entry<String, Object>> iterator = origDoc.iterator(); // iterating
																			// through
																			// it
																			// modified
																			// list

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

	public Map<String, Long> facetDocument(Map<String, Object> queryOptions) throws SolrServerException {
		Map<String, Long> facetFieldMap = null;
		if (serverActive()) {
			// report number of results found from query
			QueryResponse res = getQueryResponse(queryOptions);
			List<FacetField> facetFieldList = res.getFacetFields();
			if (facetFieldList != null && facetFieldList.size() > 0) {
				facetFieldMap = new HashMap<String, Long>();
				for (FacetField field : facetFieldList) {
					List<Count> facetInfo = field.getValues();
					if (facetInfo != null) {
						for (FacetField.Count facetInstance : facetInfo) {
							facetFieldMap.put(facetInstance.getName(), facetInstance.getCount());
						}
					}
				}
			}
		}
		return facetFieldMap;
	}

	@SuppressWarnings("unchecked")
	private QueryResponse getQueryResponse(Map<String, Object> queryOptions) throws SolrServerException {
		QueryResponse res = null;
		if (serverActive()) {

			SolrQuery Q = new SolrQuery();
			// Default
			Q.setQuery("*:*");
			Q.setFacet(true);

			// COMMON FILTERS
			// q & df- If query is specified, match based on relevance will be
			// returned
			// Default will be set to search for everything
			if (queryOptions.get(SET_DEFAULT) != null) {
				String f = (String) queryOptions.get(SET_DEFAULT);
				Q.set("q", f);
				if (queryOptions.get(SEARCH_FIELD) != null) {
					String s = (String) queryOptions.get(SEARCH_FIELD);
					Q.set("df", s);
				} else {
					Q.set("df", "all_text");
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

			if (queryOptions.get(FACET_QUERY) != null) {
				Map<String, String> query = (Map<String, String>) queryOptions.get(FACET_QUERY);
				for (String fQuery : query.keySet()) {
					Q.addFacetQuery(fQuery + ":" + query.get(fQuery));
				}
			}

			// facet.field - calculates count of total insights
			if (queryOptions.get(FACET_FIELD) != null) {
				List<String> query = (List<String>) queryOptions.get(FACET_FIELD);
				for (String fQuery : query) {
					Q.addFacetField(fQuery);
				}
				// facet.prefix - Restricts the possible constraints to only
				// indexed values with a specified prefix
				// cannot be set to case insensitive
				if (queryOptions.get(PREFIX_FACET) != null) {
					List<String> prefix = (List<String>) queryOptions.get(PREFIX_FACET);
					for (String fprefix : prefix) {
						Q.setFacetPrefix(fprefix);
					}
				}
			}

			// minCount - specifies minimum number that are necessary for the
			// facet field to be visible
			// no maxCount
			if (queryOptions.get(MIN_COUNT) != null) {
				int numRows = (int) queryOptions.get(MIN_COUNT);
				Q.setFacetMinCount(numRows);
			}

			// FacetLimit - limits the number of facets that are shows.
			if (queryOptions.get(FACET_LIMIT) != null) {
				int numLimit = (int) queryOptions.get(FACET_LIMIT);
				Q.setFacetLimit(numLimit);
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
		Map<String, Object> querySolr = new HashMap<String, Object>();
		querySolr.put(SolrIndexEngine.SET_DEFAULT, engineName);
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
