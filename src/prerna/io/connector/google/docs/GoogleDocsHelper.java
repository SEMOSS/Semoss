package prerna.io.connector.google.docs;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.*;

import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.LogManager;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.ToNumberPolicy;
import com.google.gson.reflect.TypeToken;

import prerna.io.connector.google.GoogleLoginUtils;
import prerna.sablecc2.om.execptions.SemossPixelException;
import prerna.security.HttpHelperUtility;
import org.apache.hc.core5.http.ContentType;
import prerna.util.Constants;

public class GoogleDocsHelper {

	private static final Logger classLogger = LogManager.getLogger(GoogleDocsHelper.class);
	
	private static final Gson GSON = new GsonBuilder()
			.disableHtmlEscaping()
			.setObjectToNumberStrategy(ToNumberPolicy.LONG_OR_DOUBLE)
			.setPrettyPrinting()
			.create();

	private static final String MIME_TYPE = "application/vnd.google-apps.document";
	private static final String DOCUMENT_ID_KEY = "documentId";
	private static final String TITLE_KEY = "title";
	private static final String CONTENT_KEY = "content";

	private static final String PARAGRAPH = "paragraph";
	private static final String ELEMENTS = "elements";
	private static final String TEXT_RUN = "textRun";
	private static final String FILES = "files";
	private static final String REQUESTS = "requests";
	private static final String DELETE_CONTENT_RANGE = "deleteContentRange";
	private static final String RANGE = "range";
	private static final String BODY = "body";
	private static final String START_INDEX = "startIndex";
	private static final String END_INDEX = "endIndex";
	private static final String INSERT_TEXT = "insertText";
	private static final String LOCATION = "location";
	private static final String INDEX = "index";
	private static final String TEXT = "text";

	private static final String SUCCESS_KEY = "success";
	private static final String STATUS_KEY = "status";
	
	private static final String AUTHORIZATION = "Authorization";
	private static final String BEARER = "Bearer ";
	private static final String GET = "GET";
	private static final String QUERY_PARAM_TEMPLATE = "mimeType='%s'";
	private static final String FIELDS_PARAM = "files(id,name)";
	
	private static final String DRIVE_API_URL = "https://www.googleapis.com/drive/v3/files";
	private static final String GOOGLE_DOCS_CREATE_URL = "https://docs.googleapis.com/v1/documents";
	private static final String GOOGLE_DOCS_GET_URL = "https://docs.googleapis.com/v1/documents/%s";
	private static final String GOOGLE_DOCS_BATCH_UPDATE_URL = "https://docs.googleapis.com/v1/documents/%s:batchUpdate";
	private static final String GOOGLE_DRIVE_FILE_URL = "https://www.googleapis.com/drive/v3/files/%s";
	private static final String GOOGLE_DRIVE_FILES_LIST_URL = "https://www.googleapis.com/drive/v3/files?q=mimeType='application/vnd.google-apps.document'&fields=files(id,name)";

	private GoogleDocsHelper() {

	}
	
	/**
	 * 
	 * @param accessToken
	 * @param title
	 * @param content
	 * @return
	 * @throws Exception
	 */
    public static Map<String, Object> createDoc(String accessToken, String title, String content) throws Exception {
        try {
        	if (title == null || title.trim().isEmpty()) {
        	    throw new IllegalArgumentException("Document title is required and cannot be empty.");
        	}
        	if (titleExists(accessToken, title)) {
        	    throw new IllegalArgumentException("Title " + title + " already exists");
        	}
            Map<String, String> headers = GoogleLoginUtils.getBearerHeader(accessToken);
            Map<String, String> body = new HashMap<>();
            body.put(TITLE_KEY, title);
            String jsonBody = GSON.toJson(body);
            String response = HttpHelperUtility.postRequestStringBody(GOOGLE_DOCS_CREATE_URL, headers, jsonBody, ContentType.APPLICATION_JSON, null, null, null );
            Map<String, Object> json = GSON.fromJson(response, new TypeToken<Map<String, Object>>() {}.getType());
            String docId = (String) json.get(DOCUMENT_ID_KEY);
            if(content != null) {
            	updateDoc(accessToken, docId, content);
            }
            Map<String, Object> map = new HashMap<>();
            map.put(Constants.USER_MAP_ID, json.get(DOCUMENT_ID_KEY));
            map.put(SUCCESS_KEY, true);
            return map;
        } catch (Exception e) {
        	classLogger.error(Constants.STACKTRACE, e);
			throw e;
        }
    }
    
    /**
	 * 
	 * @param accessToken
	 * @param docId
	 * @return
	 * @throws Exception
	 */
    @SuppressWarnings("unchecked")
	public static Map<String, Object> readDoc(String accessToken, String docId) throws Exception {
        try {
            Map<String, String> headers = GoogleLoginUtils.getBearerHeader(accessToken);
            String url = String.format(GOOGLE_DOCS_GET_URL, docId);
            String response = HttpHelperUtility.getRequest(url, headers, null, null, null);
            Map<String, Object> json = GSON.fromJson(response, new TypeToken<Map<String, Object>>() {}.getType());
            String title = (String) json.get(TITLE_KEY);
            StringBuilder contentText = new StringBuilder();
            Map<String, Object> body = (Map<String, Object>) json.get(BODY);
            if (body != null) {
                List<Map<String, Object>> contentList = (List<Map<String, Object>>) body.get(CONTENT_KEY);
                if (contentList != null) {
                    for (Map<String, Object> contentItem : contentList) {
                        Map<String, Object> paragraph = (Map<String, Object>) contentItem.get(PARAGRAPH);
                        if (paragraph != null) {
                            List<Map<String, Object>> elements = (List<Map<String, Object>>) paragraph.get(ELEMENTS);
                            if (elements != null) {
                                for (Map<String, Object> element : elements) {
                                    Map<String, Object> textRun = (Map<String, Object>) element.get(TEXT_RUN);
                                    if (textRun != null) {
                                        String text = (String) textRun.get(CONTENT_KEY);
                                        if (text != null) {
                                            contentText.append(text);
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
            Map<String, Object> map = new HashMap<>();
            map.put(TITLE_KEY, title);
            map.put(CONTENT_KEY, contentText.toString());
            return map;
        } catch (Exception e) {
        	classLogger.error(Constants.STACKTRACE, e);
			throw e;
        }
    }

    /**
	 * 
	 * @param accessToken
	 * @param docId
	 * @param newContent
	 * @return
	 * @throws Exception
	 */
    @SuppressWarnings("unchecked")
	public static Map<String, Object> updateDoc(String accessToken, String docId, String newContent) throws Exception {
        try {
        	Map<String, String> headers = GoogleLoginUtils.getBearerHeader(accessToken);
            String getDocUrl = String.format(GOOGLE_DOCS_GET_URL, docId);
            String docResponse = HttpHelperUtility.getRequest(getDocUrl, headers, null, null, null);
            Map<String, Object> docJson = GSON.fromJson(docResponse, new TypeToken<Map<String, Object>>() {}.getType());
            Map<String, Object> body = (Map<String, Object>) docJson.get(BODY);
            List<Map<String, Object>> content = (List<Map<String, Object>>) body.get(CONTENT_KEY);
            int endIndex = 1;
            if (content != null && !content.isEmpty()) {
                Map<String, Object> lastElement = content.get(content.size() - 1);
                Object endIdxObj = lastElement.get(END_INDEX);
                if (endIdxObj instanceof Number) {
                    endIndex = ((Number) endIdxObj).intValue();
                }
            }
            List<Map<String, Object>> requests = new ArrayList<>();
            int startIndex = 1;
            int deleteEndIndex = endIndex - 1;
            if (deleteEndIndex > startIndex) {
                Map<String, Object> range = new HashMap<>();
                range.put(START_INDEX, startIndex);
                range.put(END_INDEX, deleteEndIndex);
                Map<String, Object> deleteContentRange = new HashMap<>();
                deleteContentRange.put(RANGE, range);
                Map<String, Object> deleteRequest = new HashMap<>();
                deleteRequest.put(DELETE_CONTENT_RANGE, deleteContentRange);
                requests.add(deleteRequest);
            }
            Map<String, Object> location = new HashMap<>();
            location.put(INDEX, 1);
            Map<String, Object> insertText = new HashMap<>();
            insertText.put(LOCATION, location);
            insertText.put(TEXT, newContent);
            Map<String, Object> insertTextRequest = new HashMap<>();
            insertTextRequest.put(INSERT_TEXT, insertText);
            requests.add(insertTextRequest);
            Map<String, Object> payload = new HashMap<>();
            payload.put(REQUESTS, requests);
            String jsonBody = GSON.toJson(payload);
            String updateUrl = String.format(GOOGLE_DOCS_BATCH_UPDATE_URL, docId);
            HttpHelperUtility.postRequestStringBody(updateUrl, headers, jsonBody, ContentType.APPLICATION_JSON, null, null, null);
            Map<String, Object> map = new HashMap<>();
            map.put(STATUS_KEY, true);
            return map;
        } catch (Exception e) {
        	classLogger.error(Constants.STACKTRACE, e);
			classLogger.warn("Failed to update document", e.getMessage());
			throw e;
        }
    }
    
    /**
	 * 
	 * @param accessToken
	 * @param docId
	 * @return
	 * @throws Exception
	 */
    public static Map<String, Object> deleteDoc(String accessToken, String docId) throws Exception {
        try {
            Map<String, String> headers = GoogleLoginUtils.getBearerHeader(accessToken);
            String url = String.format(GOOGLE_DRIVE_FILE_URL, docId);
            HttpHelperUtility.deleteRequestStringBody(url, headers, null, null, null);
            Map<String, Object> map = new HashMap<>();
            map.put(STATUS_KEY, true);
            return map;
        } catch (Exception e) {
        	classLogger.error(Constants.STACKTRACE, e);
			throw e;
        }
    }

    /**
	 * 
	 * @param accessToken
	 * @param title
	 * @return
	 * @throws Exception
	 */
    @SuppressWarnings("unchecked")
	public static boolean titleExists(String accessToken, String title) throws Exception {
        try {
            Map<String, String> headers = GoogleLoginUtils.getBearerHeader(accessToken);
            String response = HttpHelperUtility.getRequest(GOOGLE_DRIVE_FILES_LIST_URL, headers, null, null, null);
            Map<String, Object> json = GSON.fromJson(response, new TypeToken<Map<String, Object>>() {}.getType());
            List<Map<String, Object>> files = (List<Map<String, Object>>) json.get(FILES);
            for (Map<String, Object> file : files) {
                if (file.get(Constants.USER_MAP_NAME) != null && file.get(Constants.USER_MAP_NAME).toString().equalsIgnoreCase(title)) {
                    return true;
                }
            }
            return false;
        } catch (Exception e) {
        	classLogger.error(Constants.STACKTRACE, e);
			throw e;
        }
    }
    
    /**
	 * 
	 * @param accessToken
	 * @return
	 * @throws Exception
	 */
    @SuppressWarnings("unchecked")
    public static List<Map<String, Object>> getDocsIdList(String accessToken) throws Exception {
        List<Map<String, Object>> docList = new ArrayList<>();
        try {
            String queryParam = String.format(QUERY_PARAM_TEMPLATE, MIME_TYPE);
            String fullUrl = DRIVE_API_URL + "?q=" + java.net.URLEncoder.encode(queryParam, "UTF-8") + "&fields=" + java.net.URLEncoder.encode(FIELDS_PARAM, "UTF-8");
            HttpURLConnection conn = (HttpURLConnection) new URL(fullUrl).openConnection();
            conn.setRequestMethod(GET);
            conn.setRequestProperty(AUTHORIZATION, BEARER + accessToken);
            int responseCode = conn.getResponseCode();
            if (responseCode != 200) {
                classLogger.error("Failed to list Google Docs. Response Code: " + responseCode);
                throw new SemossPixelException("Drive API error: HTTP " + responseCode);
            }
            try (BufferedReader in = new BufferedReader(new InputStreamReader(conn.getInputStream()))) {
                Map<String, Object> json = GSON.fromJson(in, new TypeToken<Map<String, Object>>() {}.getType());
                List<Map<String, Object>> files = (List<Map<String, Object>>) json.get(FILES);
                for (Map<String, Object> file : files) {
                	Map<String, Object> map = new HashMap<>();
                    String name = (String) file.get(Constants.USER_MAP_NAME);
                    String id = (String) file.get(Constants.USER_MAP_ID);
                    if (name != null && id != null) {
                    	map.put(TITLE_KEY, name);
                    	map.put(Constants.USER_MAP_ID, id);
                        docList.add(map);
                    }
                }
            }
            conn.disconnect();
        } catch (Exception e) {
        	classLogger.error(Constants.STACKTRACE, e);
			throw e;
        }
        return docList;
    }
}
