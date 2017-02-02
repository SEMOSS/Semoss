package prerna.solr;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.security.KeyManagementException;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.solr.client.solrj.SolrServerException;

import prerna.poi.main.TextExtractor;

public final class SolrImportUtility {

	// search field values in Insight schema
	private static final String INSIGHT_ID_FINDER = SolrIndexEngine.ID + " : ";

	private static final String STORAGE_NAME_FINDER = SolrIndexEngine.STORAGE_NAME + " : ";
	private static final String INDEX_NAME_FINDER = SolrIndexEngine.INDEX_NAME + " : ";
	
	private static final String MODIFIED_ON_FINDER = SolrIndexEngine.MODIFIED_ON + " : ";
	private static final String CREATED_ON_FINDER = SolrIndexEngine.CREATED_ON + " : ";
	private static final String LAST_VIEWED_ON_FINDER = SolrIndexEngine.LAST_VIEWED_ON + " : ";
	
	private static final String CORE_ENGINE_FINDER = SolrIndexEngine.CORE_ENGINE + " : ";
	private static final String CORE_ENGINE_ID_FINDER = SolrIndexEngine.CORE_ENGINE_ID + " : ";
	private static final String ENGINES_FINDER = SolrIndexEngine.ENGINES + " : ";

	private static final String UP_VOTES_FINDER = SolrIndexEngine.UP_VOTES + " : ";
	private static final String VIEW_COUNT_FINDER = SolrIndexEngine.VIEW_COUNT + " : "; 
	
	private static final String TAGS_FINDER = SolrIndexEngine.TAGS + " : "; 
	private static final String LAYOUT_FINDER = SolrIndexEngine.LAYOUT + " : ";
	private static final String USERID_FINDER = SolrIndexEngine.USER_ID + " : ";
	private static final String ANNOTATION_FINDER = SolrIndexEngine.ANNOTATION + " : ";
	private static final String COMMENT_FINDER = SolrIndexEngine.COMMENT + " : "; 
	private static final String USER_SPECIFIED_RELATED_FINDER = SolrIndexEngine.USER_SPECIFIED_RELATED + " : ";
	private static final String QUERY_PROJECTIONS_FINDER = SolrIndexEngine.QUERY_PROJECTIONS + " : "; 
	private static final String IMAGE_FINDER = SolrIndexEngine.IMAGE + " : ";

	// this is a utility class
	// not meant to create an instance
	// simple use defined method to import files with insight metadata into solr
	// note, the file has a very specific format
	// not intended to be created by hand.. required knowledge of the schema to construct
	private SolrImportUtility() {
		
	}
	
	/**
	 * Will index the metadata of an insight in the provided filePath into the solr index engine
	 * @param filePath					The file path where the metadata of the insight sits
	 * @throws IOException				Throws exception if the filePath is not accurate or if not 
	 * 									all the required metadata fields are found
	 */
	public static void processSolrTextDocument(String filePath) throws IOException {
		// The input file may contain multiple insights that all need to be indexed
		// the contents of each single document are contained within the tags
		final String regex = "<SolrInputDocument>(.+?)</SolrInputDocument>";

		String fileContent = null;
		try {
			// extracts the entire document as a string...
			// TODO: would be better to stream this in... but also need to perform splitting based on the solr tags
			fileContent = TextExtractor.readFile(filePath, StandardCharsets.UTF_8);
		} catch (IOException e1) {
			e1.printStackTrace();
		}
		
		// loop through the string and separate based on each solr input document
		final Pattern pattern = Pattern.compile(regex, Pattern.DOTALL);
		final Matcher matcher = pattern.matcher(fileContent);
		while(matcher.find()) {
			try {
				// for each individual document, process it and add it into the solr engine
				processSolrDocumentString(matcher.group());
			} catch (KeyManagementException e) {
				e.printStackTrace();
			} catch (NoSuchAlgorithmException e) {
				e.printStackTrace();
			} catch (KeyStoreException e) {
				e.printStackTrace();
			} catch (SolrServerException e) {
				e.printStackTrace();
			} catch (IOException e) {
				e.printStackTrace();
				String message = "";
				if(e.getMessage().isEmpty()) {
					message = "Error loading the SolrText file for database located at " + filePath;
				} else {
					message = e.getMessage();
				}
				throw new IOException(message);
			}
		}
	}
	
	/**
	 * Splits the input document string for the insight metadata and adds it to the solr engine
	 * @param solrDocStr					The string containing the insight metadata
	 * 										The information is stored in a specific format... see example below
												<SolrInputDocument>
													id : Movie_RDBMS_10
													modified_on : Tue Jun 07 05:19:03 EDT 2016
													layout : Grid
													datamaker_name : TinkerFrame
													core_engine_id : 10
													core_engine : Movie_RDBMS
													created_on : Tue Jun 07 05:19:03 EDT 2016
													user_id : default
													engines : LocalMasterDatabase
													name : save this
													tags : Semoss
												</SolrInputDocument>
											Each key corresponds to a schema entry and the value corresponds to the value
	 * @throws KeyManagementException
	 * @throws NoSuchAlgorithmException
	 * @throws KeyStoreException
	 * @throws IOException
	 * @throws SolrServerException
	 */
	private static void processSolrDocumentString(String solrDocStr) throws KeyManagementException, NoSuchAlgorithmException, KeyStoreException, IOException, SolrServerException {
		try {
			SolrIndexEngine solrE = SolrIndexEngine.getInstance();

			// create all field values in schema
			// need to store all these values associated with the specific insight
			
			///////////////// THIS IS THE START OF ALL THE FIELDS TO COLLECT /////////////////
			String id = null;
			
			String storageName = null;
			String indexName = null;

			String modifiedDate = null;
			String createdOnDate = null;
			String lastViewedDate = null;
			
			String layout = null;
			String coreEngine = null;
			String coreEngineId = null;
			String engineName = null;

			String upVoteCount = null;
			String viewCount = null;
			
			String userID = null;
			String annotation = null;
			String tag = null;
			String comment = null;
			String userSpecified = null;
			String queryProjections = null;
			String params = null;
			String algorithms = null;
			String image = null;

			List<String> enginesList = new ArrayList<String>();
			List<String> tagsList = new ArrayList<String>();
			List<String> commentsList = new ArrayList<String>();
			List<String> userSpecifiedList = new ArrayList<String>();
			List<String> queryProjectionsList = new ArrayList<String>();
			List<String> paramsList = new ArrayList<String>();
			List<String> algorithmsList = new ArrayList<String>();
			///////////////// THIS IS THE END OF ALL THE FIELDS TO COLLECT /////////////////

			
			// here we take the document and go through every line and collect the data to assign/store
			// into the above fields that have been predefined
			// split based on new line
			String regex = "(.+?)(\\n|\\r)";
			Pattern pattern = Pattern.compile(regex);
			Matcher matcher = pattern.matcher(solrDocStr);
			while(matcher.find()) {
				String currentLine = matcher.group();
				// start of a giant series of if statements to know where to store the appropriate fields
				if (currentLine.startsWith(INSIGHT_ID_FINDER)) {
					id = currentLine.substring(currentLine.indexOf(':') + 2).trim();
				} else if (currentLine.startsWith(MODIFIED_ON_FINDER)) {
					modifiedDate = currentLine.substring(currentLine.indexOf(':') + 2).trim();
				} else if (currentLine.startsWith(LAST_VIEWED_ON_FINDER)) {
					lastViewedDate = currentLine.substring(currentLine.indexOf(':') + 2).trim();
				} else if (currentLine.startsWith(LAYOUT_FINDER)) {
					layout = currentLine.substring(currentLine.indexOf(':') + 2).trim();
				} else if (currentLine.startsWith(CORE_ENGINE_FINDER)) {
					coreEngine = currentLine.substring(currentLine.indexOf(':') + 2).trim();
				}  else if (currentLine.startsWith(CORE_ENGINE_ID_FINDER)) {
					coreEngineId = currentLine.substring(currentLine.indexOf(':') + 2).trim();
				} else if (currentLine.startsWith(CREATED_ON_FINDER)) {
					createdOnDate = currentLine.substring(currentLine.indexOf(':') + 2).trim();
				} else if (currentLine.startsWith(USERID_FINDER)) {
					userID = currentLine.substring(currentLine.indexOf(':') + 2).trim();
				} else if (currentLine.startsWith(ENGINES_FINDER)) {
					engineName = currentLine.substring(currentLine.indexOf(':') + 2).trim();
					enginesList.add(engineName);
				} else if (currentLine.startsWith(STORAGE_NAME_FINDER)) {
					storageName = currentLine.substring(currentLine.indexOf(':') + 2).trim();
				} else if (currentLine.startsWith(INDEX_NAME_FINDER)) {
					indexName = currentLine.substring(currentLine.indexOf(':') + 2).trim();
				} else if (currentLine.startsWith(ANNOTATION_FINDER)) {
					annotation = currentLine.substring(currentLine.indexOf(':') + 2).trim();
				} else if (currentLine.startsWith(UP_VOTES_FINDER)) {
					upVoteCount = currentLine.substring(currentLine.indexOf(':') + 2).trim();
				} else if (currentLine.startsWith(VIEW_COUNT_FINDER)) {
					viewCount = currentLine.substring(currentLine.indexOf(':') + 2).trim();
				} else if (currentLine.startsWith(TAGS_FINDER)) {
					tag = currentLine.substring(currentLine.indexOf(':') + 2).trim();
					tagsList.add(tag);
				} else if (currentLine.startsWith(COMMENT_FINDER)) {
					comment = currentLine.substring(currentLine.indexOf(':') + 2).trim();
					commentsList.add(comment);
				} else if (currentLine.startsWith(USER_SPECIFIED_RELATED_FINDER)) {
					userSpecified = currentLine.substring(currentLine.indexOf(':') + 2).trim();
					userSpecifiedList.add(userSpecified);
				} else if (currentLine.startsWith(QUERY_PROJECTIONS_FINDER)) {
					queryProjections = currentLine.substring(currentLine.indexOf(':') + 2).trim();
					queryProjectionsList.add(queryProjections);
				} else if (currentLine.startsWith(IMAGE_FINDER)) {
					image = currentLine.substring(currentLine.indexOf(':') + 2).trim();
				}  
				
			}
			
			// once we have stored all the values
			// we need to store it in the appropriate map
			// we also perform a check to make sure all the mandatory fields are present

			// the map to store all the information
			Map<String, Object> insightQueryResults = new HashMap<String, Object>();
			
			///////////////// THIS IS THE START OF FIELDS THAT ARE REQUIRED /////////////////
			// id is required
			if(id == null || id.isEmpty()) {
				throw new IOException("SolrInputDocument does not contain an id or id is empty...");
			}
			// modified on date is required
			if(modifiedDate == null || modifiedDate.isEmpty()) {
				throw new IOException("SolrInputDocument does not contain an modifiedDate or modifiedDate is empty...");
			} else {
				insightQueryResults.put(SolrIndexEngine.MODIFIED_ON, modifiedDate);
			}
			// created on date is required
			if(createdOnDate == null || createdOnDate.isEmpty()) {
				throw new IOException("SolrInputDocument does not contain an createdOnDate or createdOnDate is empty...");
			} else {
				insightQueryResults.put(SolrIndexEngine.CREATED_ON, createdOnDate);
			}
			// layout is required
			if(layout == null || layout.isEmpty()) {
				throw new IOException("SolrInputDocument does not contain an layout or layout is empty...");
			} else {
				insightQueryResults.put(SolrIndexEngine.LAYOUT, layout);
			}
			// core engine is required
			if(coreEngine == null || coreEngine.isEmpty()) {
				throw new IOException("SolrInputDocument does not contain an coreEngine or coreEngine is empty...");
			} else {
				insightQueryResults.put(SolrIndexEngine.CORE_ENGINE, coreEngine);
			}
			// core engine id -> the id of the insight within the core_engine's insight rdbms database, is required
			if(coreEngineId == null) {
				throw new IOException("SolrInputDocument does not contain a coreEngineId...");
			} else {
				insightQueryResults.put(SolrIndexEngine.CORE_ENGINE_ID, coreEngineId);
			}
			// user id is required
			if(userID == null || userID.isEmpty()) {
				throw new IOException("SolrInputDocument does not contain an userID or userID is empty...");
			} else {
				insightQueryResults.put(SolrIndexEngine.USER_ID, userID);
			}
			// the engines used for this insight are required
			if(enginesList == null || enginesList.isEmpty()) {
				throw new IOException("SolrInputDocument does not contain an enginesList or enginesList is empty...");
			} else {
				insightQueryResults.put(SolrIndexEngine.ENGINES, enginesList);
			}
			// the name of the insight is required
			// note the name is stored as both a string for exact match and also parsed
			// thus, the name will appear twice, as storage_name and index_name
			if(storageName == null || storageName.isEmpty()) {
				throw new IOException("SolrInputDocument does not contain a storage name or storageName name is empty...");
			} else {
				insightQueryResults.put(SolrIndexEngine.STORAGE_NAME, storageName);
			}
			if(indexName == null || indexName.isEmpty()) {
				throw new IOException("SolrInputDocument does not contain an index name or index name is empty...");
			} else {
				insightQueryResults.put(SolrIndexEngine.INDEX_NAME, indexName);
			}
			///////////////// THIS IS THE END OF FIELDS THAT ARE REQUIRED /////////////////
			
			// here we add fields if present that are not required
			if(lastViewedDate != null && !lastViewedDate.isEmpty()) {
				insightQueryResults.put(SolrIndexEngine.LAST_VIEWED_ON, lastViewedDate);
			}
			if(annotation != null && !annotation.isEmpty()) {
				insightQueryResults.put(SolrIndexEngine.ANNOTATION, annotation);
			}
			if(upVoteCount != null && !upVoteCount.isEmpty()) {
				insightQueryResults.put(SolrIndexEngine.UP_VOTES, upVoteCount);
			}
			if(viewCount != null && !viewCount.isEmpty()) {
				insightQueryResults.put(SolrIndexEngine.VIEW_COUNT, viewCount);
			}
			if(tagsList != null && !tagsList.isEmpty()) {
				insightQueryResults.put(SolrIndexEngine.TAGS, tagsList);
			}
			if(commentsList != null && !commentsList.isEmpty()) {
				insightQueryResults.put(SolrIndexEngine.COMMENT, commentsList);
			}
			if(userSpecifiedList != null && !userSpecifiedList.isEmpty()) {
				insightQueryResults.put(SolrIndexEngine.USER_SPECIFIED_RELATED, userSpecifiedList);
			}
			if(queryProjectionsList != null && !queryProjectionsList.isEmpty()) {
				insightQueryResults.put(SolrIndexEngine.QUERY_PROJECTIONS, queryProjectionsList);
			}
			if(image != null && !image.isEmpty()) {
				insightQueryResults.put(SolrIndexEngine.IMAGE, image);
			}
			// now we are done with adding all the fields
			// lets go ahead and add it to the engine
			solrE.addInsight(id, insightQueryResults);
			
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		}
	}
	
}
