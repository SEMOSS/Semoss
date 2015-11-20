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

	// search field values in schema
	private static final String ID_FINDER = SolrIndexEngine.ID + " : ";
	private static final String MODIFIED_ON_FINDER = SolrIndexEngine.MODIFIED_ON + " : ";
	private static final String LAYOUT_FINDER = SolrIndexEngine.LAYOUT + " : ";
	private static final String CORE_ENGINE_FINDER = SolrIndexEngine.CORE_ENGINE + " : ";
	private static final String CORE_ENGINE_ID_FINDER = SolrIndexEngine.CORE_ENGINE_ID + " : ";
	private static final String CREATED_ON_FINDER = SolrIndexEngine.CREATED_ON + " : ";
	private static final String USERID_FINDER = SolrIndexEngine.USER_ID + " : ";
	private static final String ENGINES_FINDER = SolrIndexEngine.ENGINES + " : ";
	private static final String NAME_FINDER = SolrIndexEngine.NAME + " : ";
	
	private static final String ANNOTATION_FINDER = SolrIndexEngine.ANNOTATION + " : ";
	private static final String FAVORITES_COUNT_FINDER = SolrIndexEngine.FAVORITES_COUNT + " : ";
	private static final String VIEW_COUNT_FINDER = SolrIndexEngine.VIEW_COUNT + " : "; 
	private static final String TAGS_FINDER = SolrIndexEngine.TAGS + " : "; 
	private static final String COMMENT_FINDER = SolrIndexEngine.COMMENT + " : "; 
	private static final String USER_SPECIFIED_RELATED_FINDER = SolrIndexEngine.USER_SPECIFIED_RELATED + " : ";
	private static final String QUERY_PROJECTIONS_FINDER = SolrIndexEngine.QUERY_PROJECTIONS + " : "; 
	private static final String PARAMS_FINDER = SolrIndexEngine.PARAMS + " : ";
	private static final String ALGORITHMS_FINDER = SolrIndexEngine.ALGORITHMS + " : "; 
	
	private SolrImportUtility() {
		
	}
	
	public static void processSolrTextDocument(String filePath) throws IOException {
		final String regex = "<SolrInputDocument>(.+?)</SolrInputDocument>";

		String fileContent = null;
		try {
			fileContent = TextExtractor.readFile(filePath, StandardCharsets.UTF_8);
		} catch (IOException e1) {
			e1.printStackTrace();
		}
		
		final Pattern pattern = Pattern.compile(regex, Pattern.DOTALL);
		final Matcher matcher = pattern.matcher(fileContent);
		while(matcher.find()) {
			try {
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
	
	private static void processSolrDocumentString(String solrDocStr) throws KeyManagementException, NoSuchAlgorithmException, KeyStoreException, IOException, SolrServerException {
		try {
			SolrIndexEngine solrE = SolrIndexEngine.getInstance();

			// create all field values in schema
			String id = null;
			String modifiedDate = null;
			String layout = null;
			String coreEngine = null;
			Integer coreEngineId = null;
			String createdOnDate = null;
			String userID = null;
			String engineName = null;
			String name = null;
			String annotation = null;
			String favoriteCount = null;
			String viewCount = null;
			String tag = null;
			String comment = null;
			String userSpecified = null;
			String queryProjections = null;
			String params = null;
			String algorithms = null;
			
			List<String> enginesList = new ArrayList<String>();
			List<String> tagsList = new ArrayList<String>();
			List<String> commentsList = new ArrayList<String>();
			List<String> userSpecifiedList = new ArrayList<String>();
			List<String> queryProjectionsList = new ArrayList<String>();
			List<String> paramsList = new ArrayList<String>();
			List<String> algorithmsList = new ArrayList<String>();
			
			
			// split based on new line
			String regex = "(.+?)(\\n|\\r)";
			Pattern pattern = Pattern.compile(regex);
			Matcher matcher = pattern.matcher(solrDocStr);
			while(matcher.find()) {
				String currentLine = matcher.group();
				
				if (currentLine.startsWith(ID_FINDER)) {
					id = currentLine.substring(currentLine.indexOf(':') + 2).trim();
				} else if (currentLine.startsWith(MODIFIED_ON_FINDER)) {
					modifiedDate = currentLine.substring(currentLine.indexOf(':') + 2).trim();
				} else if (currentLine.startsWith(LAYOUT_FINDER)) {
					layout = currentLine.substring(currentLine.indexOf(':') + 2).trim();
				} else if (currentLine.startsWith(CORE_ENGINE_FINDER)) {
					coreEngine = currentLine.substring(currentLine.indexOf(':') + 2).trim();
				}  else if (currentLine.startsWith(CORE_ENGINE_ID_FINDER)) {
					coreEngineId = Integer.parseInt(currentLine.substring(currentLine.indexOf(':') + 2).trim());
				} else if (currentLine.startsWith(CREATED_ON_FINDER)) {
					createdOnDate = currentLine.substring(currentLine.indexOf(':') + 2).trim();
				} else if (currentLine.startsWith(USERID_FINDER)) {
					userID = currentLine.substring(currentLine.indexOf(':') + 2).trim();
				} else if (currentLine.startsWith(ENGINES_FINDER)) {
					engineName = currentLine.substring(currentLine.indexOf(':') + 2).trim();
					enginesList.add(engineName);
				} else if (currentLine.startsWith(NAME_FINDER)) {
					name = currentLine.substring(currentLine.indexOf(':') + 2).trim();
				} else if (currentLine.startsWith(ANNOTATION_FINDER)) {
					annotation = currentLine.substring(currentLine.indexOf(':') + 2).trim();
				} else if (currentLine.startsWith(FAVORITES_COUNT_FINDER)) {
					favoriteCount = currentLine.substring(currentLine.indexOf(':') + 2).trim();
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
				} else if (currentLine.startsWith(PARAMS_FINDER)) {
					params = currentLine.substring(currentLine.indexOf(':') + 2).trim();
					paramsList.add(params);
				} else if (currentLine.startsWith(ALGORITHMS_FINDER)) {
					algorithms = currentLine.substring(currentLine.indexOf(':') + 2).trim();
					algorithmsList.add(algorithms);
				}			
			}
			
			// add to map
			Map<String, Object> queryResults = new HashMap<String, Object>();
			if(id == null || id.isEmpty()) {
				throw new IOException("SolrInputDocument does not contain an id or id is empty...");
			} 			
			if(modifiedDate == null || modifiedDate.isEmpty()) {
				throw new IOException("SolrInputDocument does not contain an modifiedDate or modifiedDate is empty...");
			} else {
				queryResults.put(SolrIndexEngine.MODIFIED_ON, modifiedDate);
			}
			
			if(layout == null || layout.isEmpty()) {
				throw new IOException("SolrInputDocument does not contain an layout or layout is empty...");
			} else {
				queryResults.put(SolrIndexEngine.LAYOUT, layout);
			}
			
			if(coreEngine == null || coreEngine.isEmpty()) {
				throw new IOException("SolrInputDocument does not contain an coreEngine or coreEngine is empty...");
			} else {
				queryResults.put(SolrIndexEngine.CORE_ENGINE, coreEngine);
			}
			
			if(coreEngineId == null) {
				throw new IOException("SolrInputDocument does not contain a coreEngineId...");
			} else {
				queryResults.put(SolrIndexEngine.CORE_ENGINE_ID, coreEngineId);
			}
			
			if(createdOnDate == null || createdOnDate.isEmpty()) {
				throw new IOException("SolrInputDocument does not contain an createdOnDate or createdOnDate is empty...");
			} else {
				queryResults.put(SolrIndexEngine.CREATED_ON, createdOnDate);
			}
			
			if(userID == null || userID.isEmpty()) {
				throw new IOException("SolrInputDocument does not contain an userID or userID is empty...");
			} else {
				queryResults.put(SolrIndexEngine.USER_ID, userID);
			}
			
			if(enginesList == null || enginesList.isEmpty()) {
				throw new IOException("SolrInputDocument does not contain an enginesList or enginesList is empty...");
			} else {
				queryResults.put(SolrIndexEngine.ENGINES, enginesList);
			}
			
			if(name == null || name.isEmpty()) {
				throw new IOException("SolrInputDocument does not contain an name or name is empty...");
			} else {
				queryResults.put(SolrIndexEngine.NAME, name);
			}
		
			
			if(annotation == null || annotation.isEmpty()) {
				queryResults.put(SolrIndexEngine.ANNOTATION, annotation);
			}
			if(favoriteCount == null || favoriteCount.isEmpty()) {
				queryResults.put(SolrIndexEngine.FAVORITES_COUNT, favoriteCount);
			}
			if(viewCount == null || viewCount.isEmpty()) {
				queryResults.put(SolrIndexEngine.VIEW_COUNT, viewCount);
			}
			if(tagsList == null || tagsList.isEmpty()) {
				queryResults.put(SolrIndexEngine.TAGS, tagsList);
			}
			if(commentsList == null || commentsList.isEmpty()) {
				queryResults.put(SolrIndexEngine.COMMENT, commentsList);
			}
			if(userSpecifiedList == null || userSpecifiedList.isEmpty()) {
				queryResults.put(SolrIndexEngine.USER_SPECIFIED_RELATED, userSpecifiedList);
			}
			if(queryProjectionsList == null || queryProjectionsList.isEmpty()) {
				queryResults.put(SolrIndexEngine.QUERY_PROJECTIONS, queryProjectionsList);
			}
			if(paramsList == null || paramsList.isEmpty()) {
				queryResults.put(SolrIndexEngine.PARAMS, paramsList);
			}
			if(algorithmsList == null || algorithmsList.isEmpty()) {
				queryResults.put(SolrIndexEngine.ALGORITHMS, algorithmsList);
			}
						
			// add to solr engine
			solrE.addDocument(id, queryResults);
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		}
	}
	
}
