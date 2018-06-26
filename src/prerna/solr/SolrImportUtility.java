//package prerna.solr;
//
//import java.io.FileNotFoundException;
//import java.io.IOException;
//import java.nio.charset.StandardCharsets;
//import java.security.KeyManagementException;
//import java.security.KeyStoreException;
//import java.security.NoSuchAlgorithmException;
//import java.text.ParseException;
//import java.util.ArrayList;
//import java.util.Date;
//import java.util.List;
//import java.util.regex.Matcher;
//import java.util.regex.Pattern;
//
//import org.apache.solr.client.solrj.SolrServerException;
//import org.apache.solr.common.SolrInputDocument;
//
//import prerna.poi.main.TextExtractor;
//
//public final class SolrImportUtility {
//
//	// search field values in Insight schema
//	private static final String INSIGHT_ID_FINDER = SolrIndexEngine.ID + " : ";
//
//	private static final String STORAGE_NAME_FINDER = SolrIndexEngine.STORAGE_NAME + " : ";
//
//	private static final String MODIFIED_ON_FINDER = SolrIndexEngine.MODIFIED_ON + " : ";
//	private static final String CREATED_ON_FINDER = SolrIndexEngine.CREATED_ON + " : ";
//	private static final String LAST_VIEWED_ON_FINDER = SolrIndexEngine.LAST_VIEWED_ON + " : ";
//
//	private static final String APP_ID_FINDER = SolrIndexEngine.APP_ID + " : ";
//	private static final String APP_NAME_FINDER = SolrIndexEngine.APP_NAME + " : ";
//	private static final String APP_INSIGHT_ID_FINDER = SolrIndexEngine.APP_INSIGHT_ID + " : ";
//
//	private static final String UP_VOTES_FINDER = SolrIndexEngine.UP_VOTES + " : ";
//	private static final String VIEW_COUNT_FINDER = SolrIndexEngine.VIEW_COUNT + " : "; 
//
//	private static final String TAGS_FINDER = SolrIndexEngine.TAGS + " : "; 
//	private static final String DESCRIPTION_FINDER = SolrIndexEngine.DESCRIPTION + " : "; 
//	private static final String LAYOUT_FINDER = SolrIndexEngine.LAYOUT + " : ";
//	private static final String USERID_FINDER = SolrIndexEngine.USER_ID + " : ";
//
//	// this is a utility class
//	// not meant to create an instance
//	// simple use defined method to import files with insight metadata into solr
//	// note, the file has a very specific format
//	// not intended to be created by hand.. required knowledge of the schema to construct
//	private SolrImportUtility() {
//
//	}
//
//	/**
//	 * Will index the metadata of an insight in the provided filePath into the solr index engine
//	 * @param filePath					The file path where the metadata of the insight sits
//	 * @throws IOException				Throws exception if the filePath is not accurate or if not 
//	 * 									all the required metadata fields are found
//	 */
//	public static void processSolrTextDocument(String filePath) throws IOException {
//		// The input file may contain multiple insights that all need to be indexed
//		// the contents of each single document are contained within the tags
//		final String regex = "<SolrInputDocument>(.+?)</SolrInputDocument>";
//
//		String fileContent = null;
//		try {
//			// extracts the entire document as a string...
//			// TODO: would be better to stream this in... but also need to perform splitting based on the solr tags
//			fileContent = TextExtractor.readFile(filePath, StandardCharsets.UTF_8);
//		} catch (IOException e1) {
//			e1.printStackTrace();
//		}
//
//		List<SolrInputDocument> docs = new ArrayList<SolrInputDocument>();
//
//		// loop through the string and separate based on each solr input document
//		final Pattern pattern = Pattern.compile(regex, Pattern.DOTALL);
//		final Matcher matcher = pattern.matcher(fileContent);
//		while(matcher.find()) {
//			try {
//				// for each individual document, process it and add it into the solr engine
//				processSolrDocumentString(matcher.group(), docs);
//			} catch (IOException e) {
//				e.printStackTrace();
//				String message = "";
//				if(e.getMessage().isEmpty()) {
//					message = "Error loading the SolrText file for database located at " + filePath;
//				} else {
//					message = e.getMessage();
//				}
//				throw new IOException(message);
//			} catch (ParseException e) {
//				e.printStackTrace();
//			}
//		}
//		try {
//			SolrIndexEngine.getInstance().addInsights(docs);
//		} catch (KeyManagementException | NoSuchAlgorithmException | KeyStoreException | SolrServerException e) {
//			e.printStackTrace();
//		}
//	}
//
//	/**
//	 * Splits the input document string for the insight metadata and adds it to the solr engine
//	 * @param solrDocStr					The string containing the insight metadata
//	 * 										The information is stored in a specific format... see example below
//												<SolrInputDocument>
//													id : Movie_RDBMS_10
//													modified_on : Tue Jun 07 05:19:03 EDT 2016
//													layout : Grid
//													datamaker_name : TinkerFrame
//													core_engine_id : 10
//													core_engine : Movie_RDBMS
//													created_on : Tue Jun 07 05:19:03 EDT 2016
//													user_id : default
//													engines : LocalMasterDatabase
//													name : save this
//													tags : Semoss
//												</SolrInputDocument>
//											Each key corresponds to a schema entry and the value corresponds to the value
//	 * @param docs 
//	 * @throws KeyManagementException
//	 * @throws NoSuchAlgorithmException
//	 * @throws KeyStoreException
//	 * @throws IOException
//	 * @throws SolrServerException
//	 * @throws ParseException 
//	 */
//	private static void processSolrDocumentString(String solrDocStr, List<SolrInputDocument> docs) throws ParseException, IOException {
//		try {
//			SolrInputDocument doc = new SolrInputDocument();
//
//			// create all field values in schema
//			// need to store all these values associated with the specific insight
//
//			///////////////// THIS IS THE START OF ALL THE FIELDS TO COLLECT /////////////////
//			String id = null;
//
//			String storageName = null;
//
//			String modifiedDate = null;
//			String createdOnDate = null;
//			String lastViewedDate = null;
//
//			String appId = null;
//			String appName = null;
//			String appInsightId = null;
//
//			String upVoteCount = null;
//			String viewCount = null;
//
//			String tag = null;
//			String description = null;
//			String layout = null;
//			String userID = null;
//
//			List<String> tagsList = new ArrayList<String>();
//			///////////////// THIS IS THE END OF ALL THE FIELDS TO COLLECT /////////////////
//
//
//			// here we take the document and go through every line and collect the data to assign/store
//			// into the above fields that have been predefined
//			// split based on new line
//			String regex = "(.+?)(\\n|\\r)";
//			Pattern pattern = Pattern.compile(regex);
//			Matcher matcher = pattern.matcher(solrDocStr);
//			while(matcher.find()) {
//				String currentLine = matcher.group();
//				// start of a giant series of if statements to know where to store the appropriate fields
//				if (currentLine.startsWith(INSIGHT_ID_FINDER)) {
//					id = currentLine.substring(currentLine.indexOf(':') + 2).trim();
//				} else if (currentLine.startsWith(CREATED_ON_FINDER)) {
//					Date date = SolrIndexEngine.getSolrDateFormat().parse( currentLine.substring(currentLine.indexOf(':') + 2).trim() );
//					createdOnDate = SolrIndexEngine.getDateFormat().format( date );
//				} else if (currentLine.startsWith(MODIFIED_ON_FINDER)) {
//					Date date = SolrIndexEngine.getSolrDateFormat().parse( currentLine.substring(currentLine.indexOf(':') + 2).trim() );
//					modifiedDate = SolrIndexEngine.getDateFormat().format( date );
//				} else if (currentLine.startsWith(LAST_VIEWED_ON_FINDER)) {
//					Date date = SolrIndexEngine.getSolrDateFormat().parse( currentLine.substring(currentLine.indexOf(':') + 2).trim() );
//					lastViewedDate = SolrIndexEngine.getDateFormat().format( date );
//				} else if (currentLine.startsWith(LAYOUT_FINDER)) {
//					layout = currentLine.substring(currentLine.indexOf(':') + 2).trim();
//				} else if (currentLine.startsWith(APP_ID_FINDER)) {
//					appId = currentLine.substring(currentLine.indexOf(':') + 2).trim();
//				} else if (currentLine.startsWith(APP_NAME_FINDER)) {
//					appName = currentLine.substring(currentLine.indexOf(':') + 2).trim();
//				} else if (currentLine.startsWith(APP_INSIGHT_ID_FINDER)) {
//					appInsightId = currentLine.substring(currentLine.indexOf(':') + 2).trim();
//				} else if (currentLine.startsWith(USERID_FINDER)) {
//					userID = currentLine.substring(currentLine.indexOf(':') + 2).trim();
//				} else if (currentLine.startsWith(STORAGE_NAME_FINDER)) {
//					storageName = currentLine.substring(currentLine.indexOf(':') + 2).trim();
//				} else if (currentLine.startsWith(UP_VOTES_FINDER)) {
//					upVoteCount = currentLine.substring(currentLine.indexOf(':') + 2).trim();
//				} else if (currentLine.startsWith(VIEW_COUNT_FINDER)) {
//					viewCount = currentLine.substring(currentLine.indexOf(':') + 2).trim();
//				} else if (currentLine.startsWith(TAGS_FINDER)) {
//					tag = currentLine.substring(currentLine.indexOf(':') + 2).trim();
//					tagsList.add(tag);
//				} else if (currentLine.startsWith(DESCRIPTION_FINDER)) {
//					description = currentLine.substring(currentLine.indexOf(':') + 2).trim();
//				}
//			}
//
//			// once we have stored all the values
//			// add to the solr document
//			// we also perform a check to make sure all the mandatory fields are present
//
//			///////////////// THIS IS THE START OF FIELDS THAT ARE REQUIRED /////////////////
//			// id is required
//			if(id == null || id.isEmpty()) {
//				throw new IOException("SolrInputDocument does not contain an id or id is empty...");
//			} else {
//				doc.addField(SolrIndexEngine.ID, id);
//			}
//			// modified on date is required
//			if(modifiedDate == null || modifiedDate.isEmpty()) {
//				throw new IOException("SolrInputDocument does not contain an modifiedDate or modifiedDate is empty...");
//			} else {
//				doc.addField(SolrIndexEngine.MODIFIED_ON, modifiedDate);
//			}
//			// created on date is required
//			if(createdOnDate == null || createdOnDate.isEmpty()) {
//				throw new IOException("SolrInputDocument does not contain an createdOnDate or createdOnDate is empty...");
//			} else {
//				doc.addField(SolrIndexEngine.CREATED_ON, createdOnDate);
//			}
//			// layout is required
//			if(layout == null || layout.isEmpty()) {
//				throw new IOException("SolrInputDocument does not contain an layout or layout is empty...");
//			} else {
//				doc.addField(SolrIndexEngine.LAYOUT, layout);
//			}
//			// app id is required
//			if(appId == null || appId.isEmpty()) {
//				throw new IOException("SolrInputDocument does not contain an app_id or app_id is empty...");
//			} else {
//				doc.addField(SolrIndexEngine.APP_ID, appId);
//			}
//			// app name is required
//			if(appName == null || appName.isEmpty()) {
//				throw new IOException("SolrInputDocument does not contain an app_name or app_name is empty...");
//			} else {
//				doc.addField(SolrIndexEngine.APP_NAME, appId);
//			}
//			// app insight id the id of the insight within the core_engine's insight rdbms database, is required
//			if(appInsightId == null) {
//				throw new IOException("SolrInputDocument does not contain a coreEngineId...");
//			} else {
//				doc.addField(SolrIndexEngine.APP_INSIGHT_ID, appInsightId);
//			}
//			// user id is required
//			if(userID == null || userID.isEmpty()) {
//				throw new IOException("SolrInputDocument does not contain an userID or userID is empty...");
//			} else {
//				doc.addField(SolrIndexEngine.USER_ID, userID);
//			}
//			// the name of the insight is required
//			// note the name is stored as both a string for exact match and also parsed
//			// thus, the name will appear twice, as storage_name and index_name
//			if(storageName == null || storageName.isEmpty()) {
//				throw new IOException("SolrInputDocument does not contain a storage name or storageName name is empty...");
//			} else {
//				doc.addField(SolrIndexEngine.STORAGE_NAME, storageName);
//			}
//			///////////////// THIS IS THE END OF FIELDS THAT ARE REQUIRED /////////////////
//
//			// here we add fields if present that are not required
//			if(lastViewedDate != null && !lastViewedDate.isEmpty()) {
//				doc.addField(SolrIndexEngine.LAST_VIEWED_ON, lastViewedDate);
//			}
//			if(description != null && !description.isEmpty()) {
//				doc.addField(SolrIndexEngine.DESCRIPTION, description);
//			}
//			if(upVoteCount != null && !upVoteCount.isEmpty()) {
//				doc.addField(SolrIndexEngine.UP_VOTES, upVoteCount);
//			} else {
//				doc.addField(SolrIndexEngine.UP_VOTES, 0);
//			}
//			if(viewCount != null && !viewCount.isEmpty()) {
//				doc.addField(SolrIndexEngine.VIEW_COUNT, viewCount);
//			} else {
//				doc.addField(SolrIndexEngine.VIEW_COUNT, 0);
//			}
//			if(tagsList != null && !tagsList.isEmpty()) {
//				doc.addField(SolrIndexEngine.TAGS, tagsList);
//			}
//			// now we are done with adding all the fields
//			// lets go ahead and add it to the list of documents
//			docs.add(doc);
//		} catch (FileNotFoundException e) {
//			e.printStackTrace();
//		}
//	}
//
//}
