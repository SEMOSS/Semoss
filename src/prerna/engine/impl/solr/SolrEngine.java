package prerna.engine.impl.solr;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URL;
import java.security.KeyManagementException;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Vector;

import org.apache.http.conn.ssl.SSLConnectionSocketFactory;
import org.apache.http.conn.ssl.TrustSelfSignedStrategy;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.ssl.SSLContextBuilder;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.HttpSolrServer;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;

import com.google.common.reflect.TypeToken;
import com.google.gson.Gson;
import com.google.gson.JsonIOException;
import com.google.gson.JsonSyntaxException;

import prerna.engine.api.IEngine;
import prerna.engine.impl.AbstractEngine;
import prerna.rdf.query.builder.IQueryInterpreter;
import prerna.test.TestUtilityMethods;
import prerna.util.Constants;
import prerna.util.OWLER;

public class SolrEngine extends AbstractEngine {
	
	private static final Logger LOGGER = LogManager.getLogger(SolrEngine.class.getName());
	public static final String SCHEMA_HEADERS_KEY = "headers";
	public static final String SCHEMA_DATA_TYPE_KEY = "dataTypes";
	public static final String SCHEMA_UNIQUE_HEADER_KEY = "uniqueKey";
	public static final String SCHEMA_UNIQUE_HEADER_DATA_TYPE_KEY = "uniqueKeyType";
	
	// the solr server
	private SolrServer solrServer;
	// the base url of the solr engine
	private String solrBaseURL;
	// the solr core name
	private String solrCoreName;
	// the combination fo the solr base url and the core name
	private String solrCoreURL;
	
	@Override
	public void openDB(String propFile) {
		super.openDB(propFile);
		SSLContextBuilder builder = new SSLContextBuilder();
		try {
			builder.loadTrustMaterial(null, new TrustSelfSignedStrategy());
			SSLConnectionSocketFactory sslsf = new SSLConnectionSocketFactory(builder.build());
			CloseableHttpClient httpclient = HttpClients.custom().setSSLSocketFactory(sslsf).build();

			this.solrBaseURL = prop.get(Constants.SOLR_URL).toString();
			this.solrCoreName = prop.get(Constants.SOLR_CORE_NAME).toString();
			this.solrCoreURL = solrBaseURL + "/" + solrCoreName;
			
//			this.solrBaseURL = "http://localhost:8080/solr";
//			this.solrCoreName = "insightCore";
//			this.solrCoreURL = solrBaseURL + "/" + solrCoreName;
			
			this.solrServer = new HttpSolrServer(solrCoreURL, httpclient);
		} catch (NoSuchAlgorithmException | KeyStoreException | KeyManagementException e) {
			e.printStackTrace();
			throw new IllegalArgumentException("Could not connect to Solr Engine " + this.engineName);
		}
	}

	@Override
	public SolrDocumentList execQuery(String query) {
		//TODO:
		//TODO:
		//TODO:
		//TODO:
		//TODO:
		// this does not work....
		SolrQuery solrQuery = new SolrQuery(query);
		SolrDocumentList docList = null;
		try {
			System.out.println("solrQuery exectuted on server!! " + solrQuery.toString());
			QueryResponse response = solrServer.query(solrQuery);
			docList = response.getResults();
		} catch (SolrServerException e) {
			e.printStackTrace();
		}
		return docList;
	}
	
	/**
	 * Query the solr core
	 * @param query
	 * @return
	 */
	public SolrDocumentList execQuery(SolrQuery query) {
		SolrQuery solrQuery = query;
		SolrDocumentList docList = null;
		try {
			QueryResponse response = solrServer.query(solrQuery);
			docList = response.getResults();
		} catch (SolrServerException e) {
			e.printStackTrace();
		}
		return docList;
	}

	@Override
	public Vector<Object> getEntityOfType(String type) {
		// TODO Auto-generated method stub
		Vector<Object> values = new Vector<Object>();
		SolrQuery query = new SolrQuery();
		query.setQuery(type+":*");
		query.setStart(0);
		query.setRows(500);		
		QueryResponse response = null;
		try {
		    response = solrServer.query(query);
		} catch (SolrServerException e) {/* */ }
		SolrDocumentList list = response.getResults();
		for(int i = 0; i < list.size(); i++){
			SolrDocument doc = list.get(i);
			Object value = doc.get(type);
			values.add(value);
			System.out.println(value);
		}
		return null;
	}
	

	@Override
	public Vector<Object> getCleanSelect(String query) {
		// TODO Auto-generated method stub
		return null;
	}
	
	@Override
	public void insertData(String query) {
		// TODO Auto-generated method stub

	}

	@Override
	public void removeData(String query) {
		// TODO Auto-generated method stub

	}

	/**
	 * Used to determine if the server is active or not This call is basically
	 * made before any other method is called
	 * @return true if the server is active
	 */
	public boolean serverActive() {
		boolean isActive = true;
		try {
			solrServer.ping();
		} catch (Exception e) {
			isActive = false;
		}
		return isActive;
	}
	
	@Override
	public void commit() {
		try {
			this.solrServer.commit();
		} catch (SolrServerException | IOException e) {
			e.printStackTrace();
		}
	}

	@Override
	public IQueryInterpreter getQueryInterpreter() {
		return new SolrInterpreter();
	}
	
	@Override
	public ENGINE_TYPE getEngineType() {
		return ENGINE_TYPE.SOLR;
	}
	
	/**
	 * Gets the list of core from http://localhost:8080/solr/admin/cores?action=STATUS&wt=json
	 * @return
	 */
	public ArrayList<String> getCoreList() {
		String coreURL = solrBaseURL + "/admin/cores?action=STATUS&wt=json";
		InputStream coreInput;
		ArrayList<String> coreList = new ArrayList<>();
		System.out.println("Query Core List: " + coreURL);

		try {
			coreInput = new URL(coreURL).openStream();
			Map<String, Object> coreMap = new Gson().fromJson(new InputStreamReader(coreInput, "UTF-8"), new TypeToken<Map<String, Object>>() {}.getType());
			Map<String, Object> statusObject = (Map<String, Object>) coreMap.get("status");
			for (String s : statusObject.keySet()) {
				//TODO check if engine exists
				coreList.add(s);
			}

			System.out.println(
					"****************************************************************************************************");
			System.out.println(
					"******************************          core List          ********************************************");
			System.out.println(
					"****************************************************************************************************");
			for (int i = 0; i < coreList.size(); i++) {
				System.out.println(coreList.get(i));
			}
			System.out.println(
					"****************************************************************************************************");
			System.out.println(
					"****************************************************************************************************");

		} catch (IOException e2) {
			// TODO Auto-generated catch block
			e2.printStackTrace();
		}
		return coreList;
	}
	
	public void createOWL() {
		String path = "C:\\workspace\\Semoss_Dev\\db\\Solr_OWL.OWL";
		OWLER owl = new OWLER(path, IEngine.ENGINE_TYPE.SOLR);
		
		Map<String, Object> schemaData = getSchema(this.solrBaseURL, this.solrCoreName);
		
		List<String> columnHeaders = (List<String>) schemaData.get(SCHEMA_HEADERS_KEY);
		List<String> columnTypes = (List<String>) schemaData.get(SCHEMA_DATA_TYPE_KEY);
		String key = (String) schemaData.get(SCHEMA_UNIQUE_HEADER_KEY);
		String keyType = (String) schemaData.get(SCHEMA_UNIQUE_HEADER_DATA_TYPE_KEY);

		owl.addConcept(key, keyType);
		for(int i = 0; i < columnHeaders.size(); i++) {
			String column = columnHeaders.get(i);
			// the list of column headers
			// also contains the key
			// so dont add it as a prop of itself
			if(column.equals(key)) {
				continue;
			}
			owl.addProp(key, columnHeaders.get(i), columnTypes.get(i));
		}
		owl.commit();
		try {
			owl.export();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	//////////////////////////////////////////////////////////////////////////////////
	//////////////////////////////////////////////////////////////////////////////////
	///////////////////////// STATIC UTILITY METHODS /////////////////////////////////
	//////////////////////////////////////////////////////////////////////////////////
	//////////////////////////////////////////////////////////////////////////////////

	
	/**
	 * Method to get the schema from 
	 * @return 
	 */
	public static Map<String, Object> getSchema(String solrURL, String coreName) {
		String schemaURL = solrURL + "/" + coreName + "/schema/fields";
		LOGGER.info("Schema query: " + schemaURL);
		/*
		 * This object will hold 3 things
		 * 1) headers -> pointing to an array
		 * 2) types -> pointing to an array
		 * ** header + types will match by index
		 * 3) uniqueKey -> pointing to a string
		 * 4) uniqueKeyType -> pointing to a string
		 */
		Map<String, Object> schemaData = new HashMap<String, Object>();
		
		InputStream input = null;
		try {
			// connect to the url to get the schema
			input = new URL(schemaURL).openStream();
			Map<String, Object> map = new Gson().fromJson(new InputStreamReader(input, "UTF-8"), new TypeToken<Map<String, Object>>() {}.getType());
			List fields = (List) map.get("fields");
			int size = fields.size();
			List<String> headers = new Vector<String>(size);
			List<String> types = new Vector<String>(size);
			
			for (int i = 0; i < fields.size(); i++) {
				// grab each field and see if we should add it to the schema
				Map<String, Object> fieldsObject = (Map<String, Object>) fields.get(i);
				
				// right now we will only grab values that are stored
				// TODO: figure out how to add indexed fields that are not stored
				// 		so we can search using them but return the sotred vars
				Boolean stored = (Boolean) fieldsObject.get("stored");
				if (stored) {
					String column = (String) fieldsObject.get("name");
					headers.add(column);
					String type = (String) fieldsObject.get("type");
					types.add(type);
				}
				
				Boolean uniqueKey = (Boolean) fieldsObject.get("uniqueKey");
				if(uniqueKey != null && uniqueKey) {
					String column = (String) fieldsObject.get("name");
					schemaData.put(SCHEMA_UNIQUE_HEADER_KEY, column);
					String type = (String) fieldsObject.get("type");
					schemaData.put(SCHEMA_UNIQUE_HEADER_DATA_TYPE_KEY, type);
				}
			}
			
			schemaData.put(SCHEMA_HEADERS_KEY, headers);
			schemaData.put(SCHEMA_DATA_TYPE_KEY, types);
		} catch (IOException | JsonIOException | JsonSyntaxException e) {
			e.printStackTrace();
			throw new IllegalArgumentException("Could not process the solr core's schema information at " + schemaURL);
		} finally {
			if(input != null) {
				try {
					input.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}
		
		return schemaData;
	}
	
	/**
	 * Method to determine if we can connect to a solr instance with a given URL + core name
	 * @param solrEngineURL
	 * @param coreName
	 * @return
	 */
	public static boolean ping(String solrURL, String coreName) {
		SSLContextBuilder builder = new SSLContextBuilder();
		try {
			builder.loadTrustMaterial(null, new TrustSelfSignedStrategy());
			SSLConnectionSocketFactory sslsf = new SSLConnectionSocketFactory(builder.build());
			CloseableHttpClient httpclient = HttpClients.custom().setSSLSocketFactory(sslsf).build();

			String solrCoreUrl = solrURL + "/" + coreName;
			HttpSolrServer solrServer = new HttpSolrServer(solrCoreUrl, httpclient);
			solrServer.ping();
		} catch (NoSuchAlgorithmException | KeyStoreException | KeyManagementException | SolrServerException | IOException e) {
			return false;
		}

		return true;
	}
	
	public static void main(String[] args) throws KeyManagementException, NoSuchAlgorithmException, KeyStoreException {
		// getCoreList();
		TestUtilityMethods.loadDIHelper();
		SolrEngine solr = new SolrEngine();
		solr.openDB(null);
		solr.setEngineName("insightCore");
		solr.createOWL();
	}

}
