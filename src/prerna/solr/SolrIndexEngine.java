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
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.HttpSolrServer;
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

	//schema field names
	public static final String ID = "id";
	public static final String NAME = "name";
	public static final String CREATED_ON = "created_on";
	public static final String MODIFIED_ON = "modified_on";
	public static final String USER_ID = "user_id";
	public static final String ENGINES = "engines";
	public static final String CORE_ENGINE = "core_engine";
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
	
	public static void setUrl(String url) {
		SolrIndexEngine.url = url;
	}

	public static SolrIndexEngine getInstsance() throws KeyManagementException, NoSuchAlgorithmException, KeyStoreException {
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

	public void removeDocument(List<String> uniqueID) throws SolrServerException, IOException {
		// delete document based on ID
		server.deleteById(uniqueID);
		server.commit();
	}

	public void modifyFields(String uniqueID, Map<String, Object> fieldsToModify) throws SolrServerException, IOException {
		Map<String, Object> queryMap = new HashMap<String, Object>();
		queryMap.put(SET_DEFAULT, ID + ":" + uniqueID);
		SolrDocument origDoc = queryDocument(queryMap).get(0);
		Iterator<Entry<String, Object>> iterator = origDoc.iterator();  //iterating through it modified list

		SolrInputDocument doc = new SolrInputDocument();
		doc.get(uniqueID); //getting the doc set based on the id
		
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
		// when committing, automatically overrides existing field values with the new ones
		server.add(doc);
		server.commit();
	}

	@SuppressWarnings({ "unchecked" })
	public SolrDocumentList queryDocument(Map<String, Object> queryOptions) throws SolrServerException, IOException {

		SolrQuery Q = new SolrQuery();
		//Default
		Q.setQuery("*:*");

		//COMMON FILTERS
		//q & df- If query is specified, match based on relevance will be returned 
		//Default will be set to search for everything
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

		//fq - filters search based on exact match
		if (queryOptions.get(FITLER_QUERY) != null) {
			Map<String, String> query = (Map<String, String>) queryOptions.get(FITLER_QUERY);
			for (String fQuery : query.keySet()) {
				Q.addFilterQuery(fQuery + ":" + query.get(fQuery));
			}
		}

		//sorts - orders list asc/desc or  by relevance 
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
		}

		// fl - this is determining which fields in the schema will be returned
		if (queryOptions.get(ADD_FIELD) != null) {
			List<String> fields = (List<String>) queryOptions.get(ADD_FIELD);
			for (String f : fields) {
				Q.addField(f);
			}
		}

		//FACET INFO FILTERS

		//facet.query - calculated count on any query
		if (queryOptions.get(FACET_QUERY) != null) {
			Map<String, String> query = (Map<String, String>) queryOptions.get(FACET_QUERY);
			for (String fQuery : query.keySet()) {
				Q.addFacetQuery(fQuery + ":" + query.get(fQuery) );
			}
		}

		// facet.field - calculates count of total insights 
		if (queryOptions.get(FACET_FIELD) != null) {
			List<String> query = (List<String>) queryOptions.get(FACET_FIELD);
			for (String fQuery : query) {
				Q.addFacetField(fQuery);
			}
			// facet.prefix -  Restricts the possible constraints to only indexed values with a specified prefix
			//cannot be set to case insensitive
			if (queryOptions.get(PREFIX_FACET) != null) {
				List<String> prefix = (List<String>) queryOptions.get(PREFIX_FACET);
				for (String fprefix : prefix) {
					Q.setFacetPrefix(fprefix);
				}
			}
		}

		// minCount - specifies minimum number that are necessary for the facet field to be visible
		//no maxCount
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
		QueryResponse res = server.query(Q);
		SolrDocumentList results = res.getResults();
		long numFound = results.getNumFound();
		System.out.println("num hits: " + numFound);
		System.out.println(Q);

		return results;

		//		for (int i = 0; i < results.size(); ++i) {
		//			SolrDocument getDoc = results.get(i);
		//			for (Iterator<Entry<String, Object>> fieldIterator = getDoc.iterator(); fieldIterator.hasNext();) {
		//				Entry<String, Object> entry = fieldIterator.next();
		//				System.out.println(entry.getKey() + ": " + entry.getValue());
		//
		//			} System.out.println();
		//		}
		//
		//
		//		List<FacetField> facetFieldList = res.getFacetFields();
		//		for (int i = 0; i < facetFieldList.size(); i++) {
		//			FacetField facetField = facetFieldList.get(i);
		//			List<Count> facetInfo = facetField.getValues();
		//			for (FacetField.Count facetInstance : facetInfo) {
		//				System.out.println(facetInstance.getName() + " : " + facetInstance.getCount());
		//			}
		//		}
	}

	public void deleteAllSolrData() {
		try {
			server.deleteByQuery("*:*");
		} catch (SolrServerException ex) {
			throw new RuntimeException("Failed to delete data in Solr. " + ex.getMessage(), ex);
		} catch (IOException ex) {
			throw new RuntimeException("Failed to delete data in Solr. " + ex.getMessage(), ex);
		}
	}

	public void deleteEngine(String engineName){
		try {
			server.deleteByQuery("core_engine:" + engineName);
		} catch (SolrServerException e1) {
			e1.printStackTrace();
		} catch (IOException e1) {
			e1.printStackTrace();
		}
	}
	
	public static DateFormat getDateFormat() {
		return new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'");
	}
}
