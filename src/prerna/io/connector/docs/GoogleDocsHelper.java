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

    private static final Logger classLogger = LogManager.getLogger(GoogleDocsHelper.class);

    private static final String STATUS_KEY = "status";
    private static final String DOCUMENT_ID_KEY = "documentId";
    private static final String TITLE_KEY = "title";
    private static final String CONTENT_KEY = "content";
    public static final String GOOGLE_DOCS_CREATE_URL = "https://docs.googleapis.com/v1/documents";
    public static final String GOOGLE_DOCS_GET_URL = "https://docs.googleapis.com/v1/documents/";
    public static final String GOOGLE_DOCS_BATCH_UPDATE_URL = "https://docs.googleapis.com/v1/documents/%s:batchUpdate";
    public static final String GOOGLE_DRIVE_FILES_LIST_URL = "https://www.googleapis.com/drive/v3/files?q=mimeType='application/vnd.google-apps.document'&fields=files(id,name)";

    private static final Gson gson = new GsonBuilder().setPrettyPrinting().create();

    public static NounMetadata createDoc(String accessToken, String title, String content) {
        try {
            Map<String, String> headers = getBearerHeader(accessToken);

            Map<String, String> body = new HashMap<>();
            body.put(TITLE_KEY, title);

            String jsonBody = gson.toJson(body);
            String response = HttpHelperUtility.postRequestStringBody(GOOGLE_DOCS_CREATE_URL, headers, jsonBody, ContentType.APPLICATION_JSON, null, null, null );
            Map<String, Object> json = gson.fromJson(response, new TypeToken<Map<String, Object>>() {}.getType());
            String docId = (String) json.get(DOCUMENT_ID_KEY);
            updateDoc(accessToken, docId, content);
            Map<String, Object> result = new HashMap<>();
            result.put(DOCUMENT_ID_KEY, json.get("documentId"));
            result.put(TITLE_KEY, json.get("title"));
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
            String url = GOOGLE_DOCS_GET_URL + docId;
            String response = HttpHelperUtility.getRequest(url, headers, null, null, null);
            Map<String, Object> json = gson.fromJson(response, new TypeToken<Map<String, Object>>() {}.getType());
            String title = (String) json.get("title");
            StringBuilder contentText = new StringBuilder();
            Map<String, Object> body = (Map<String, Object>) json.get("body");
            if (body != null) {
                List<Map<String, Object>> contentList = (List<Map<String, Object>>) body.get("content");
                if (contentList != null) {
                    for (Map<String, Object> contentItem : contentList) {
                        Map<String, Object> paragraph = (Map<String, Object>) contentItem.get("paragraph");
                        if (paragraph != null) {
                            List<Map<String, Object>> elements = (List<Map<String, Object>>) paragraph.get("elements");
                            if (elements != null) {
                                for (Map<String, Object> element : elements) {
                                    Map<String, Object> textRun = (Map<String, Object>) element.get("textRun");
                                    if (textRun != null) {
                                        String text = (String) textRun.get("content");
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

    public static NounMetadata updateDoc(String accessToken, String docId, String newText) {
        try {
            Map<String, String> headers = getBearerHeader(accessToken);
            String updateUrl = String.format(GOOGLE_DOCS_BATCH_UPDATE_URL, docId);
            Map<String, Object> location = new HashMap<>();
            location.put("index", 1);
            Map<String, Object> insertText = new HashMap<>();
            insertText.put("location", location);
            insertText.put("text", newText);
            Map<String, Object> insertTextRequest = new HashMap<>();
            insertTextRequest.put("insertText", insertText);
            Map<String, Object> payload = new HashMap<>();
            payload.put("requests", Collections.singletonList(insertTextRequest));
            String jsonBody = gson.toJson(payload);
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
            String url = "https://www.googleapis.com/drive/v3/files/" + docId;
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
	public static NounMetadata titleExists(String accessToken, String titleToFind) {
        try {
            Map<String, String> headers = getBearerHeader(accessToken);
            String response = HttpHelperUtility.getRequest(GOOGLE_DRIVE_FILES_LIST_URL, headers, null, null, null);
            Map<String, Object> json = gson.fromJson(response, new TypeToken<Map<String, Object>>() {}.getType());
            List<Map<String, Object>> files = (List<Map<String, Object>>) json.get("files");
            for (Map<String, Object> file : files) {
                if (file.get("name") != null && file.get("name").toString().equalsIgnoreCase(titleToFind)) {
                    Map<String, Object> result = new HashMap<>();
                    result.put("exists", true);
                    result.put("docId", file.get("id"));
                    return new NounMetadata(result, PixelDataType.CUSTOM_DATA_STRUCTURE, PixelOperationType.OPERATION);
                }
            }
            Map<String, Object> notFound = new HashMap<>();
            notFound.put(STATUS_KEY, false);
            return new NounMetadata(notFound, PixelDataType.CUSTOM_DATA_STRUCTURE, PixelOperationType.OPERATION);
        } catch (Exception e) {
            classLogger.error("Error checking title existence", e);
            throw new SemossPixelException("Failed to check document title: " + e.getMessage());
        }
    }

    private static Map<String, String> getBearerHeader(String accessToken) {
        Map<String, String> headers = new HashMap<>();
        headers.put("Authorization", "Bearer " + accessToken);
        headers.put("Content-Type", "application/json");
        return headers;
    }
}
