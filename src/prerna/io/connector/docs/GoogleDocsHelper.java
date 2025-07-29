package prerna.io.connector.docs;

import java.util.*;

import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.LogManager;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.reflect.TypeToken;

import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.execptions.SemossPixelException;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.security.HttpHelperUtility;
import org.apache.hc.core5.http.ContentType;

public class GoogleDocsHelper {

    private static final String SUCCESS_KEY = "success";

	private static final Logger classLogger = LogManager.getLogger(GoogleDocsHelper.class);
    
    private static final String BEARER = "Bearer ";
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
    private static final String HEADER_AUTHORIZATION = "Authorization";
    private static final String HEADER_CONTENT_TYPE = "Content-Type";
    private static final String CONTENT_TYPE_JSON = "application/json";
    private static final String STATUS_KEY = "status";
    private static final String DOCUMENT_ID_KEY = "documentId";
    private static final String TITLE_KEY = "title";
    private static final String CONTENT_KEY = "content";
    private static final String NAME_KEY = "name";
    private static final String GOOGLE_DOCS_CREATE_URL = "https://docs.googleapis.com/v1/documents";
    private static final String GOOGLE_DOCS_GET_URL = "https://docs.googleapis.com/v1/documents/%s";
    private static final String GOOGLE_DOCS_BATCH_UPDATE_URL = "https://docs.googleapis.com/v1/documents/%s:batchUpdate";
    private static final String GOOGLE_DRIVE_FILE_URL = "https://www.googleapis.com/drive/v3/files/%s";
    private static final String GOOGLE_DRIVE_FILES_LIST_URL = "https://www.googleapis.com/drive/v3/files?q=mimeType='application/vnd.google-apps.document'&fields=files(id,name)";

    private static final Gson gson = new GsonBuilder().setPrettyPrinting().create();

    public static NounMetadata createDoc(String accessToken, String title, String content) {
        try {
        	if (title == null) {
        	    throw new IllegalArgumentException("Title must not be null");
        	}
        	if (titleExists(accessToken, title)) {
        	    throw new IllegalArgumentException("Title " + title + " already exists");
        	}
            Map<String, String> headers = getBearerHeader(accessToken);
            Map<String, String> body = new HashMap<>();
            body.put(TITLE_KEY, title);
            String jsonBody = gson.toJson(body);
            String response = HttpHelperUtility.postRequestStringBody(GOOGLE_DOCS_CREATE_URL, headers, jsonBody, ContentType.APPLICATION_JSON, null, null, null );
            Map<String, Object> json = gson.fromJson(response, new TypeToken<Map<String, Object>>() {}.getType());
            String docId = (String) json.get(DOCUMENT_ID_KEY);
            if(content != null) {
            	updateDoc(accessToken, docId, content);
            }
            Map<String, Object> result = new HashMap<>();
            result.put(DOCUMENT_ID_KEY, json.get(DOCUMENT_ID_KEY));
            result.put(SUCCESS_KEY, true);
            return new NounMetadata(result, PixelDataType.CUSTOM_DATA_STRUCTURE, PixelOperationType.OPERATION);
        } catch (Exception e) {
            classLogger.error("Error creating doc", e);
            throw new SemossPixelException("Failed to create document: " + e.getMessage());
        }
    }

    @SuppressWarnings("unchecked")
	public static NounMetadata readDoc(String accessToken, String docId) {
        try {
            Map<String, String> headers = getBearerHeader(accessToken);
            String url = String.format(GOOGLE_DOCS_GET_URL, docId);
            String response = HttpHelperUtility.getRequest(url, headers, null, null, null);
            Map<String, Object> json = gson.fromJson(response, new TypeToken<Map<String, Object>>() {}.getType());
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
            Map<String, Object> result = new HashMap<>();
            result.put(TITLE_KEY, title);
            result.put(CONTENT_KEY, contentText.toString());
            return new NounMetadata(result, PixelDataType.CUSTOM_DATA_STRUCTURE, PixelOperationType.OPERATION);
        } catch (Exception e) {
            classLogger.error("Error reading doc", e);
            throw new SemossPixelException("Failed to read document: " + e.getMessage(), e);
        }
    }

    @SuppressWarnings("unchecked")
	public static NounMetadata updateDoc(String accessToken, String docId, String newText) {
        try {
        	Map<String, String> headers = getBearerHeader(accessToken);
            String getDocUrl = String.format(GOOGLE_DOCS_GET_URL, docId);
            String docResponse = HttpHelperUtility.getRequest(getDocUrl, headers, null, null, null);
            Map<String, Object> docJson = gson.fromJson(docResponse, new TypeToken<Map<String, Object>>() {}.getType());
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
            insertText.put(TEXT, newText);
            Map<String, Object> insertTextRequest = new HashMap<>();
            insertTextRequest.put(INSERT_TEXT, insertText);
            requests.add(insertTextRequest);
            Map<String, Object> payload = new HashMap<>();
            payload.put(REQUESTS, requests);
            String jsonBody = gson.toJson(payload);
            String updateUrl = String.format(GOOGLE_DOCS_BATCH_UPDATE_URL, docId);
            HttpHelperUtility.postRequestStringBody(updateUrl, headers, jsonBody, ContentType.APPLICATION_JSON, null, null, null);
            Map<String, Object> result = new HashMap<>();
            result.put(STATUS_KEY, true);
            return new NounMetadata(result, PixelDataType.CUSTOM_DATA_STRUCTURE, PixelOperationType.OPERATION);
        } catch (Exception e) {
            classLogger.error("Error updating doc", e);
            throw new SemossPixelException("Failed to update document: " + e.getMessage());
        }
    }

    public static NounMetadata deleteDoc(String accessToken, String docId) {
        try {
            Map<String, String> headers = getBearerHeader(accessToken);
            String url = String.format(GOOGLE_DRIVE_FILE_URL, docId);
            HttpHelperUtility.deleteRequestStringBody(url, headers, null, null, null);
            Map<String, Object> result = new HashMap<>();
            result.put(STATUS_KEY, true);
            return new NounMetadata(result, PixelDataType.CUSTOM_DATA_STRUCTURE, PixelOperationType.OPERATION);
        } catch (Exception e) {
            classLogger.error("Error deleting doc", e);
            throw new SemossPixelException("Failed to delete document: " + e.getMessage());
        }
    }

    @SuppressWarnings("unchecked")
	public static boolean titleExists(String accessToken, String title) {
        try {
            Map<String, String> headers = getBearerHeader(accessToken);
            String response = HttpHelperUtility.getRequest(GOOGLE_DRIVE_FILES_LIST_URL, headers, null, null, null);
            Map<String, Object> json = gson.fromJson(response, new TypeToken<Map<String, Object>>() {}.getType());
            List<Map<String, Object>> files = (List<Map<String, Object>>) json.get(FILES);
            for (Map<String, Object> file : files) {
                if (file.get(NAME_KEY) != null && file.get(NAME_KEY).toString().equalsIgnoreCase(title)) {
                    return true;
                }
            }
            return false;
        } catch (Exception e) {
            classLogger.error("Error checking title existence", e);
            throw new SemossPixelException("Failed to check document title: " + e.getMessage());
        }
    }

    public static Map<String, String> getBearerHeader(String accessToken) {
        Map<String, String> headers = new HashMap<>();
        headers.put(HEADER_AUTHORIZATION, BEARER + accessToken);
        headers.put(HEADER_CONTENT_TYPE, CONTENT_TYPE_JSON);
        return headers;
    }
}
