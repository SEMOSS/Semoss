package prerna.io.connector.salesforce;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.reflect.TypeToken;

import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.execptions.SemossPixelException;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.security.HttpHelperUtility;
import org.apache.hc.core5.http.ContentType;

public class SalesforceHelper {
	
	private static final Logger classLogger = LogManager.getLogger(SalesforceHelper.class);

	private static final Gson gson = new GsonBuilder().setPrettyPrinting().create();
	
	private static final String BEARER = "Bearer ";
	private static final String CONTENT_TYPE_JSON = "application/json";
	private static final String ERRORS_KEY = "errors";
    private static final String FIELDS_KEY = "fields";
	private static final String HEADER_AUTHORIZATION = "Authorization";
    private static final String HEADER_CONTENT_TYPE = "Content-Type";
    private static final String ID_KEY = "id";
    private static final String RECORDS_KEY = "records";
    private static final String SEARCH_RECORDS_KEY = "searchRecords";
    private static final String SUCCESS_KEY = "success";
    private static final String TOTAL_SIZE_KEY = "totalSize";

    private static final String FIELDS_DESCRIBE_SUFFIX = "/describe";
    private static final String SOBJECTS_ENDPOINT_SUFFIX = "/services/data/v64.0/sobjects/";
    private static final String QUERY_ENDPOINT_SUFFIX = "/services/data/v64.0/query/?q=";
    private static final String SEARCH_ENDPOINT_SUFFIX = "/services/data/v64.0/search/?q=";
    
    public static NounMetadata fetchObjectSchema(String accessToken, String instanceUrl, String sObjectName) {
    	Map<String, Object> result = new HashMap<>();
    	
    	try {
    		Map<String, String> headers = getBearerHeader(accessToken);
    		
    		//fetch all sObjects
    		String fullUrl = instanceUrl + SOBJECTS_ENDPOINT_SUFFIX;
    		classLogger.info("Fetching Salesforce Objects list from: " + fullUrl);
    		
    		String objectsResponse = HttpHelperUtility.getRequest(fullUrl, headers, null, null, null);
    		Map<String, Object> objectsList = gson.fromJson(objectsResponse, new TypeToken<Map<String, Object>>() {}.getType());
            result.put("objects", objectsList);
    		
            // fetch field metadata for a specific sObject
            if (sObjectName != null && !sObjectName.trim().isEmpty()) {
            	String describeUrl = instanceUrl + SOBJECTS_ENDPOINT_SUFFIX + sObjectName + FIELDS_DESCRIBE_SUFFIX;
            	classLogger.info("Fetching Salesforce fields metadata for object: " + sObjectName + " from: " + describeUrl);
            	
            	String fieldsResponse = HttpHelperUtility.getRequest(describeUrl, headers, null, null, null);
            	Map<String, Object> fieldsMetadata = gson.fromJson(fieldsResponse, new TypeToken<Map<String, Object>>(){}.getType());
            	result.put("fieldsMetadata", fieldsMetadata);
            } else {
            	result.put("fieldsMetadata", null);
            }
            
            return new NounMetadata(result, PixelDataType.CUSTOM_DATA_STRUCTURE, PixelOperationType.OPERATION);
            
    	} catch(Exception e) {
    		classLogger.error("Failed to fetch combined Salesforce metadata ", e);
            throw new SemossPixelException("Failed to fetch combined Salesforce metadata: " + e.getMessage(), e);
    	}
    }
    
    public static NounMetadata runSoqlQuery(String accessToken, String instanceUrl, String soqlQuery) {
    	Map<String, Object> result = new HashMap<>();
    	
    	try {
    		if (soqlQuery == null || soqlQuery.trim().isEmpty()) {
    			classLogger.error("SOQL query string must not be empty.");
                throw new IllegalArgumentException("SOQL query string must not be empty.");
    		}
    		
    		Map<String, String> headers = getBearerHeader(accessToken);
    		
    		String endpoint = instanceUrl + QUERY_ENDPOINT_SUFFIX;
    		String encodedSoql = java.net.URLEncoder.encode(soqlQuery, "UTF-8");
    		String fullUrl = endpoint + encodedSoql;
    		classLogger.info("Running SOQL: " + soqlQuery + " via " + fullUrl);
    		
    		String response = HttpHelperUtility.getRequest(fullUrl, headers, null, null, null);
    		Map<String, Object> responseMap = gson.fromJson(response, new TypeToken<Map<String, Object>>(){}.getType());
    		
    		result.put(TOTAL_SIZE_KEY, responseMap.get(TOTAL_SIZE_KEY));
    		result.put(RECORDS_KEY, responseMap.get(RECORDS_KEY));
    		
    		return new NounMetadata(result, PixelDataType.CUSTOM_DATA_STRUCTURE, PixelOperationType.OPERATION);
    		
    	} catch(Exception e) {
    		classLogger.error("Failed to run SOQL query on Salesforce ", e);
    		throw new SemossPixelException("Failed to run SOQL query: " + e.getMessage(), e);
    	}
    }
    
    public static NounMetadata fetchRecordById(String accessToken, String instanceUrl, String sObjectName, String recordId) {
    	Map<String, Object> result = new HashMap<>();
    	
    	try {
    		if (sObjectName == null || sObjectName.trim().isEmpty()) {
    			classLogger.error("Salesforce oject name must not be empty.");
    			throw new IllegalArgumentException("Salesforce object name must not be empty.");
    		}
    		if (recordId == null || recordId.trim().isEmpty()) {
    			classLogger.error("Record Id must not be empty.");
    			throw new IllegalArgumentException("Record Id must not be empty.");
    		}

    		Map<String, String> headers = getBearerHeader(accessToken);
    		
    		String fullUrl = instanceUrl + SOBJECTS_ENDPOINT_SUFFIX + sObjectName + "/" + recordId;
    		classLogger.info("Fetching Salesforce record for: " + sObjectName + "/" + recordId + " using URL: " + fullUrl);
    		
    		String response = HttpHelperUtility.getRequest(fullUrl, headers, null, null, null);
    		Map<String, Object> responseMap = gson.fromJson(response, new TypeToken<Map<String, Object>>(){}.getType());
    		
            result.put(FIELDS_KEY, responseMap);
    		
            return new NounMetadata(result, PixelDataType.CUSTOM_DATA_STRUCTURE, PixelOperationType.OPERATION);
            
    	} catch(Exception e) {
    		classLogger.error("Failed to fetch Salesforce record by Id ", e);
    		throw new SemossPixelException("Failed to fetch Salesforce record by Id: " + e.getMessage(), e);
    	}
    }
    
    public static NounMetadata runSoslQuery(String accessToken, String instanceUrl, String soslQuery) {
    	Map<String, Object> result = new HashMap<>();
    	
    	try {
    		if (soslQuery == null || soslQuery.trim().isEmpty()) {
    			classLogger.error("SOSL query string must not be empty.");
                throw new IllegalArgumentException("SOSL query string must not be empty.");
    		}
    		
    		Map<String, String> headers = getBearerHeader(accessToken);
    		
    		String endpoint = instanceUrl + SEARCH_ENDPOINT_SUFFIX;
    		String encodedSosl = java.net.URLEncoder.encode(soslQuery, "UTF-8");
    		String fullUrl = endpoint + encodedSosl;
    		classLogger.info("Running SOSL: " + soslQuery + " via " + fullUrl);
    		
    		String response = HttpHelperUtility.getRequest(fullUrl, headers, null, null, null);
    		Map<String, Object> responseMap = gson.fromJson(response, new TypeToken<Map<String, Object>>(){}.getType());
    		
    		result.put(SEARCH_RECORDS_KEY, responseMap.get(SEARCH_RECORDS_KEY));
    		
    		return new NounMetadata(result, PixelDataType.CUSTOM_DATA_STRUCTURE, PixelOperationType.OPERATION);
    		
    	} catch(Exception e) {
    		classLogger.error("Failed to run SOSL query on Salesforce ", e);
    		throw new SemossPixelException("Failed to run SOSL query: " + e.getMessage(), e);
    	}
    }
    
    public static NounMetadata createRecord(String accessToken, String instanceUrl, String sObjectName, Map<String, Object> fieldValues) {
    	Map<String, Object> result = new HashMap<>();
    	
    	try {
    		if (sObjectName == null || sObjectName.trim().isEmpty()) {
    			classLogger.error("Salesforce oject name must not be empty.");
    			throw new IllegalArgumentException("Salesforce object name must not be empty.");
    		}
    		if (fieldValues == null || fieldValues.isEmpty()) {
    			classLogger.error("Field values map must not be empty.");
    			throw new IllegalArgumentException("Field values map must not be empty.");
    		}
    		
    		Map<String, String> headers = getBearerHeader(accessToken);
    		
    		String fullUrl = instanceUrl + SOBJECTS_ENDPOINT_SUFFIX + sObjectName;
    		// converting the field values map to JSON for POST body
    		String jsonBody = gson.toJson(fieldValues);
    		classLogger.info("Creating Salesforce record in " + sObjectName + " via: " + fullUrl + " with body: " + jsonBody);
    		
    		String response = HttpHelperUtility.postRequestStringBody(fullUrl, headers, jsonBody, ContentType.APPLICATION_JSON, null, null, null );
            Map<String, Object> responseMap = gson.fromJson(response, new TypeToken<Map<String, Object>>() {}.getType());
    		
            String recordId = (String) responseMap.get(ID_KEY);
            boolean success = Boolean.TRUE.equals(responseMap.get(SUCCESS_KEY));
            Object errors = responseMap.get(ERRORS_KEY);
            
            result.put(ID_KEY, recordId);
            result.put(SUCCESS_KEY, success);
            
            if (errors != null && !((List<?>) errors).isEmpty()) {
            	result.put(ERRORS_KEY, errors);
            }
            
            return new NounMetadata(result, PixelDataType.CUSTOM_DATA_STRUCTURE, PixelOperationType.OPERATION);
            
    	} catch(Exception e) {
    		classLogger.error("Error creating Salesforce record ", e);
    		throw new SemossPixelException("Error creating Salesforce record: " + e.getMessage(), e);
    	}	
    }
    
    public static NounMetadata deleteRecord(String accessToken, String instanceUrl, String sObjectName, String recordId) {
    	Map<String, Object> result = new HashMap<>();
    	
    	try {
    		if (sObjectName == null || sObjectName.trim().isEmpty()) {
    			classLogger.error("Salesforce oject name must not be empty.");
    			throw new IllegalArgumentException("Salesforce object name must not be empty.");
    		}
    		if (recordId == null || recordId.trim().isEmpty()) {
    			classLogger.error("Record Id must not be empty.");
    			throw new IllegalArgumentException("Record Id must not be empty.");
    		}
    		
    		Map<String, String> headers = getBearerHeader(accessToken);
    		
    		String fullUrl = instanceUrl + SOBJECTS_ENDPOINT_SUFFIX + sObjectName + "/" + recordId;
    		classLogger.info("Deleting Salesforce record: " + sObjectName + " Id: " + recordId + " via " + fullUrl);
    		
    		String response = HttpHelperUtility.deleteRequestStringBody(fullUrl, headers, null, null, null);
    		
    		// if delete is successful, response is empty (204 No Content)
            boolean success = (response == null || response.trim().isEmpty());
            result.put(SUCCESS_KEY, success);
    		
            return new NounMetadata(result, PixelDataType.CUSTOM_DATA_STRUCTURE, PixelOperationType.OPERATION);
            
    	} catch(Exception e) {
    		classLogger.error("Error deleting Salesforce record ", e);
    		throw new SemossPixelException("Error deleting Salesforce record: " + e.getMessage(), e);
    	}
    }
    
    public static NounMetadata updateRecord(String accessToken, String instanceUrl, String sObjectName, String recordId, Map<String, Object> fieldValues) {
    	Map<String, Object> result = new HashMap<>();
    	
    	try {
    		if (sObjectName == null || sObjectName.trim().isEmpty()) {
    			classLogger.error("Salesforce oject name must not be empty.");
    			throw new IllegalArgumentException("Salesforce object name must not be empty.");
    		}
    		if (recordId == null || recordId.trim().isEmpty()) {
    			classLogger.error("Record Id must not be empty.");
    			throw new IllegalArgumentException("Record Id must not be empty.");
    		}
    		if (fieldValues == null || fieldValues.isEmpty()) {
    			classLogger.error("Field values map must not be empty.");
    			throw new IllegalArgumentException("Field values map must not be empty.");
    		}
    		
    		Map<String, String> headers = getBearerHeader(accessToken);
    		
    		String fullUrl = instanceUrl + SOBJECTS_ENDPOINT_SUFFIX + sObjectName + "/" + recordId;
    		// converting the field values map to JSON for PATCH body
    		String jsonBody = gson.toJson(fieldValues);
    		classLogger.info("Updating Salesforce record: " + sObjectName + " Id: " + recordId + " via " + fullUrl);
    		
    		String response = HttpHelperUtility.patchRequestStringBody(fullUrl, headers, jsonBody, ContentType.APPLICATION_JSON, null, null, null );
    		
    		// if patch/update is successful, response is empty (204 No Content)
            boolean success = (response == null || response.trim().isEmpty());
            result.put(SUCCESS_KEY, success);
    		
            return new NounMetadata(result, PixelDataType.CUSTOM_DATA_STRUCTURE, PixelOperationType.OPERATION);
            
    	} catch(Exception e) {
    		classLogger.error("Error updating Salesforce record ", e);
    		throw new SemossPixelException("Error updating Salesforce record: " + e.getMessage(), e);
    	}
    }
    
    public static Map<String, String> getBearerHeader(String accessToken) {
        Map<String, String> headers = new HashMap<>();
        headers.put(HEADER_AUTHORIZATION, BEARER + accessToken);
        headers.put(HEADER_CONTENT_TYPE, CONTENT_TYPE_JSON);
        return headers;
    }
	
}
