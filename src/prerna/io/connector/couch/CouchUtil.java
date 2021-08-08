package prerna.io.connector.couch;

import java.awt.image.BufferedImage;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Iterator;
import java.util.Map;
import java.util.UUID;

import javax.imageio.ImageIO;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.ResponseBuilder;

import org.apache.commons.codec.binary.Base64;
import org.apache.commons.fileupload.FileItem;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.http.Header;
import org.apache.http.HttpEntity;
import org.apache.http.HttpHeaders;
import org.apache.http.HttpResponse;
import org.apache.http.HttpStatus;
import org.apache.http.StatusLine;
import org.apache.http.client.methods.HttpDelete;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpHead;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.util.EntityUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

import prerna.auth.utils.AbstractSecurityUtils;
import prerna.auth.utils.SecurityQueryUtils;
import prerna.cluster.util.ClusterUtil;
import prerna.engine.impl.SmssUtilities;
import prerna.nameserver.utility.MasterDatabaseUtility;
import prerna.util.AssetUtility;
import prerna.util.Constants;
import prerna.util.DIHelper;
import prerna.util.insight.InsightUtility;
import prerna.util.insight.TextToGraphic;

public class CouchUtil {
	
	public static final String DATABASE = "database";
	public static final String INSIGHT = "insight";
	public static final String PROJECT = "project";
	
	private static final Logger LOGGER = LogManager.getLogger(CouchUtil.class);
	
	private static final String DIR_SEPARATOR = java.nio.file.FileSystems.getDefault().getSeparator();
	
	private static final String COUCH_ENABLED_KEY = "COUCH_ENABLED";
	public static final boolean COUCH_ENABLED = 
		StringUtils.isEmpty(DIHelper.getInstance().getProperty(COUCH_ENABLED_KEY))
		?
		(
			System.getenv().containsKey(COUCH_ENABLED_KEY)
			?
			Boolean.parseBoolean(System.getenv(COUCH_ENABLED_KEY))
			:
			false
		)
		:
		Boolean.parseBoolean(DIHelper.getInstance().getProperty(COUCH_ENABLED_KEY))
		;
	
	private static final String COUCH_ENDPOINT_KEY = "COUCH_ENDPOINT";
	private static final String COUCH_ENDPOINT = 
		StringUtils.isEmpty(DIHelper.getInstance().getProperty(COUCH_ENDPOINT_KEY))
		?
		(
			System.getenv().containsKey(COUCH_ENDPOINT_KEY)
			?
			System.getenv(COUCH_ENDPOINT_KEY)
			:
			""
		)
		:
		DIHelper.getInstance().getProperty(COUCH_ENDPOINT_KEY)
		;
	
	private static final String COUCH_CREDS_KEY = "COUCH_CREDS";
	private static final String COUCH_CREDS = 
		StringUtils.isEmpty(DIHelper.getInstance().getProperty(COUCH_CREDS_KEY))
		?
		(
			System.getenv().containsKey(COUCH_CREDS_KEY)
			?
			System.getenv(COUCH_CREDS_KEY)
			:
			""
		)
		:
		DIHelper.getInstance().getProperty(COUCH_CREDS_KEY)
		;
	private static final String COUCH_AUTH = "Basic " 
		+ new String(Base64.encodeBase64(COUCH_CREDS.getBytes(StandardCharsets.ISO_8859_1)));
	
	private static final ObjectMapper MAPPER = new ObjectMapper();
	
	public static Response download(String partitionId, Map<String, String> referenceData) throws CouchException {
		if(referenceData.isEmpty()) {
			throw new IllegalArgumentException("Selector list is empty");
		}
		
		String selector = getSelectorString(referenceData);
		
		String documentId = null;
		ObjectNode docJson = null;
		String attachmentId = null;
		try {
			CouchResponse findResponse = retrieveDocumentsInPartitionForSelector(partitionId, selector);
			JsonNode findRespJson = MAPPER.readTree(findResponse.getResponseBody());
			docJson = (ObjectNode) findRespJson.path("docs").get(0);
			if(docJson != null) {
				documentId = docJson.path("_id").textValue();
				JsonNode attachmentsJson = docJson.get("_attachments");
				if(attachmentsJson != null) {
					Iterator<String> fields = attachmentsJson.fieldNames();
					if(fields.hasNext()) {
						attachmentId = fields.next();
					}
				}
			}
		} catch (JsonProcessingException e) {
			throw new CouchException("Error parsing document search response", e);
		}
		
		byte[] attachmentBytes = null;
		if(documentId != null && attachmentId != null) {
			CouchResponse documentResponse = retrieveDocument(documentId, true);
			try {
				JsonNode docAttJson = MAPPER.readTree(documentResponse.getResponseBody());
				String attachmentData = docAttJson.path("_attachments").path(attachmentId).path("data").textValue();
				attachmentBytes = Base64.decodeBase64(attachmentData);
			} catch (JsonProcessingException e) {
				throw new CouchException("Error parsing document search response", e);
			}
		} else {
			if(docJson == null) {
				docJson = MAPPER.createObjectNode();
				for(String key : referenceData.keySet()) {
					docJson.put(key, referenceData.get(key));
				}
			}
			attachmentBytes = createDefault(partitionId, docJson);
		}
		
		String eTag = null;
		try {
			eTag = new String(Base64.encodeBase64(MessageDigest.getInstance("MD5").digest(attachmentBytes)));
		} catch (NoSuchAlgorithmException e) {
			LOGGER.error("Error building byte digest", e);
		}
		
		ResponseBuilder builder = Response.ok(attachmentBytes)
				.header("Content-Disposition", "attachment; filename=\"" + attachmentId + "\"");
		if(eTag != null) {
			builder = builder.tag(eTag);
		}
		return builder.build();
	}
	
	public static void upload(String partitionId, Map<String, String> referenceData, File imageFile) throws CouchException {
		if(!referenceData.containsKey(partitionId)) {
			throw new IllegalArgumentException("Upload data is missing required value for key: " + partitionId);
		}
		
		try {
			String selector = getSelectorString(referenceData);
			
			String documentId;
			String revisionId;
			ObjectNode docJson = null;
			
			CouchResponse findResponse = retrieveDocumentsInPartitionForSelector(partitionId, selector);
			JsonNode findRespJson = MAPPER.readTree(findResponse.getResponseBody());
			docJson = (ObjectNode) findRespJson.path("docs").get(0);
			if(docJson != null) {
				documentId = docJson.path("_id").textValue();
				revisionId = docJson.path("_rev").textValue();
			} else {
				documentId = partitionId + ":" + UUID.randomUUID().toString();
				revisionId = null;
				docJson = MAPPER.createObjectNode();
				for(String key : referenceData.keySet()) {
					docJson.put(key, referenceData.get(key));
				}
			}
			
			String contentType;
			String fileExtension = FilenameUtils.getExtension(imageFile.getName()).trim();
			if(fileExtension == null || fileExtension.isEmpty()) {
				contentType = "application/octet-stream";
				
			} else {
				contentType = "image/"+fileExtension;
			}
			String attachmentName = "image." + fileExtension;
			updateDocument(documentId, revisionId, docJson, attachmentName, 
					contentType, FileUtils.readFileToByteArray(imageFile));
		} catch (IOException e) {
			LOGGER.error(Constants.STACKTRACE, e);
			throw new CouchException("Error processing upload", e);
		}
	}
	
	public static void upload(String partitionId, Map<String, String> referenceData, FileItem imageFile) throws CouchException {
		if(!referenceData.containsKey(partitionId)) {
			throw new IllegalArgumentException("Upload data is missing required value for key: " + partitionId);
		}
		
		try {
			String selector = getSelectorString(referenceData);
			
			String documentId;
			String revisionId;
			ObjectNode docJson = null;
			
			CouchResponse findResponse = retrieveDocumentsInPartitionForSelector(partitionId, selector);
			JsonNode findRespJson = MAPPER.readTree(findResponse.getResponseBody());
			docJson = (ObjectNode) findRespJson.path("docs").get(0);
			if(docJson != null) {
				documentId = docJson.path("_id").textValue();
				revisionId = docJson.path("_rev").textValue();
			} else {
				documentId = partitionId + ":" + UUID.randomUUID().toString();
				revisionId = null;
				docJson = MAPPER.createObjectNode();
				for(String key : referenceData.keySet()) {
					docJson.put(key, referenceData.get(key));
				}
			}
			
			String attachmentName = "image." + imageFile.getContentType().split("/")[1];
			updateDocument(documentId, revisionId, docJson, attachmentName, 
					imageFile.getContentType(), IOUtils.toByteArray(imageFile.getInputStream()));
		} catch (IOException e) {
			LOGGER.error(Constants.STACKTRACE, e);
			throw new CouchException("Error processing upload", e);
		}
	}
	
	public static void delete(String partitionId, Map<String, String> referenceData) throws CouchException {
		if(referenceData.isEmpty()) {
			throw new IllegalArgumentException("Selector list is empty");
		}
		
		try {
			String selector = getSelectorString(referenceData);
			
			String documentId;
			String revisionId;
			
			CouchResponse findResponse = retrieveDocumentsInPartitionForSelector(partitionId, selector);
			JsonNode findRespJson;
				findRespJson = MAPPER.readTree(findResponse.getResponseBody());
			JsonNode docJson = findRespJson.path("docs").get(0);
			if(docJson == null) {
				// if it isn't found then deletion is unnecessary. return as if successful.
				LOGGER.warn("Couch deletion call on missing document: " + selector);
				return;
			}
			documentId = docJson.path("_id").textValue();
			revisionId = docJson.path("_rev").textValue();
			deleteDocument(documentId, revisionId);
		} catch (JsonProcessingException e) {
			LOGGER.error(Constants.STACKTRACE, e);
			throw new CouchException("Error processing delete", e);
		}
	}
	
	private static String getSelectorString(Map<String, String> referenceData) {
		StringBuilder selectorBuilder = new StringBuilder("{\"selector\": {");
		for(String key : referenceData.keySet()) {
			String searchValue = referenceData.get(key);
			selectorBuilder.append("\"").append(key).append("\":{\"$eq\":");
			if(searchValue == null) {
				selectorBuilder.append("null");
			} else {
				selectorBuilder.append("\"").append(searchValue).append("\"");
			}
			selectorBuilder.append("},");
		}
		selectorBuilder.replace(selectorBuilder.length()-1, selectorBuilder.length(), "");
		selectorBuilder.append("}}");
		return selectorBuilder.toString();
	}
	
	private static byte[] createDefault(String partitionId, ObjectNode documentData) throws CouchException {
		String documentId = null;
		String revisionId = null;
		if(documentData.has("_id")) {
			documentId = documentData.get("_id").textValue();
			if(documentData.has("_rev")) {
				revisionId = documentData.get("_rev").textValue();
			} else {
				try {
					CouchResponse infoCheckResponse = retrieveDocumentInfo(documentId);
					revisionId = infoCheckResponse.getRevision();
				} catch (CouchException e) {
					if(e.getStatusCode() != HttpStatus.SC_NOT_FOUND) {
						throw e;
					}
					revisionId = null;
				}
			}
		} else {
			documentId = partitionId + ":" + UUID.randomUUID().toString();
		}
		
		String databaseId = documentData.path(DATABASE).textValue();
		String projectId = documentData.path(PROJECT).textValue();
		String insightId = documentData.path(INSIGHT).textValue();
		
		try {
			String attachmentName;
			String contentType;
			byte[] fileContent;
			
			if(DATABASE.equals(partitionId)) {
				String databaseName = MasterDatabaseUtility.getDatabaseAliasForId(databaseId);
				
				File[] images;
				if(ClusterUtil.IS_CLUSTER) {
					String imagePath = ClusterUtil.IMAGES_FOLDER_PATH
							+ DIR_SEPARATOR + "databases";
					images = InsightUtility.findImageFile(imagePath, databaseId);
				} else {
					String imagePath = DIHelper.getInstance().getProperty(Constants.BASE_FOLDER) 
							+ DIR_SEPARATOR + Constants.DB_FOLDER 
							+ DIR_SEPARATOR + SmssUtilities.getUniqueName(databaseName, databaseId) 
							+ DIR_SEPARATOR + "app_root"
							+ DIR_SEPARATOR + "version";
					images = InsightUtility.findImageFile(imagePath);
				}
				
				if(images != null && images.length > 0) {
					File insightImageFile = images[0];
					String extension = FilenameUtils.getExtension(insightImageFile.getName());
					attachmentName = "image." + extension;
					contentType = "image/" + extension;
					fileContent = FileUtils.readFileToByteArray(insightImageFile);
				} else {
					attachmentName = "image.png";
					contentType = "image/png";
					BufferedImage img = TextToGraphic.buildBufferedImage(databaseName);
					ByteArrayOutputStream baos = new ByteArrayOutputStream();
					ImageIO.write(img, "png", baos);
					fileContent = baos.toByteArray();
				}
			} else if(PROJECT.equals(partitionId)) {
				String projectName = SecurityQueryUtils.getProjectAliasForId(projectId);
				
				File[] images;
				if(ClusterUtil.IS_CLUSTER) {
					String imagePath = ClusterUtil.IMAGES_FOLDER_PATH
							+ DIR_SEPARATOR + "projects";
					images = InsightUtility.findImageFile(imagePath, projectId);
				} else {
					String imagePath = AssetUtility.getProjectAssetVersionFolder(projectName, projectId);
					images = InsightUtility.findImageFile(imagePath);
				}
				
				if(images != null && images.length > 0) {
					File insightImageFile = images[0];
					String extension = FilenameUtils.getExtension(insightImageFile.getName());
					attachmentName = "image." + extension;
					contentType = "image/" + extension;
					fileContent = FileUtils.readFileToByteArray(insightImageFile);
				} else {
					attachmentName = "image.png";
					contentType = "image/png";
					BufferedImage img = TextToGraphic.buildBufferedImage(projectName);
					ByteArrayOutputStream baos = new ByteArrayOutputStream();
					ImageIO.write(img, "png", baos);
					fileContent = baos.toByteArray();
				}
			} else {
				String projectName = SecurityQueryUtils.getProjectAliasForId(projectId);
				String imagePath = AssetUtility.getProjectAssetVersionFolder(projectName, projectId)
						+ DIR_SEPARATOR + insightId;
				File[] images = InsightUtility.findImageFile(imagePath);
				
				File insightImageFile = null;
				if(images != null && images.length > 0) {
					insightImageFile = images[0];
					attachmentName = insightImageFile.getName();
					String extension = FilenameUtils.getExtension(insightImageFile.getName());
					contentType = "image/" + extension;
				} else {
					insightImageFile = AbstractSecurityUtils.getStockImage(projectId, insightId);
					attachmentName = "image.png";
					contentType = "image/png";
				}
				fileContent = FileUtils.readFileToByteArray(insightImageFile);
			}
			updateDocument(documentId, revisionId, documentData, attachmentName, contentType, fileContent);
			return fileContent;
		} catch (IOException e) {
			LOGGER.error(Constants.STACKTRACE, e);
			throw new CouchException("Error processing image creation", e);
		}
	}
	
	private static CouchResponse retrieveDocumentInfo(String documentId) throws CouchException {
		HttpHead documentInfoGet = new HttpHead(COUCH_ENDPOINT + documentId);
		CouchResponse response = executeRequest(documentInfoGet);
		LOGGER.debug("Successfully retrieved info: " + response.toString());
		return response;
	}
	
	private static CouchResponse updateDocument(String documentId, String revisionId, ObjectNode documentData, String attachmentName, 
			String attachmentContentType, byte[] attachmentBytes) throws CouchException {
		String attachmentData = new String(Base64.encodeBase64(attachmentBytes));
		
		ObjectNode attachmentsNode = null;
		if(documentData.has("_attachments")) {
			attachmentsNode = (ObjectNode) documentData.get("_attachments");
		} else {
			attachmentsNode = documentData.putObject("_attachments");
		}
		
		if(attachmentsNode.has(attachmentName)) {
			attachmentsNode.remove(attachmentName);
		}
		
		attachmentsNode = attachmentsNode
				.putObject(attachmentName)
				.put("content_type", attachmentContentType)
				.put("data", attachmentData);
		
		try {
			HttpPut docCreate;
			if(revisionId == null) {
				docCreate = new HttpPut(COUCH_ENDPOINT + documentId);
			} else {
				docCreate = new HttpPut(COUCH_ENDPOINT + documentId + "?rev=" + revisionId);
			}
			docCreate.setEntity(new StringEntity(documentData.toString()));
			CouchResponse response = executeRequest(docCreate);
			LOGGER.debug("Successful document creation: " + response.toString());
			return response;
		} catch (UnsupportedEncodingException e) {
			throw new IllegalArgumentException("The document data encoding isn't supported", e);
		}
	}
	
	private static CouchResponse retrieveDocumentsInPartitionForSelector(String partitionId, String selector) throws CouchException {
		try {
			HttpPost findPost = new HttpPost(COUCH_ENDPOINT + "_partition/" + partitionId + "/_find");
			findPost.setEntity(new StringEntity(selector));
			findPost.setHeader(HttpHeaders.CONTENT_TYPE, "application/json");
			CouchResponse response = executeRequest(findPost);
			LOGGER.debug("Successfully retrieved documents for selector: " + response.toString());
			return response;
		} catch (UnsupportedEncodingException e) {
			throw new CouchException("Failed to form query", e);
		}
	}
	
	private static CouchResponse retrieveDocument(String documentId, boolean withAttachments) throws CouchException {
		HttpGet documentGet = new HttpGet(COUCH_ENDPOINT + documentId + "?attachments=" + withAttachments);
		documentGet.setHeader(HttpHeaders.ACCEPT, "application/json");
		CouchResponse response = executeRequest(documentGet);
		LOGGER.debug("Successful document retrieval: " + response.toString());
		return response;
	}
	
	private static CouchResponse deleteDocument(String documentId, String revisionId) throws CouchException {
		HttpDelete documentDelete = new HttpDelete(COUCH_ENDPOINT + documentId + "?rev=" + revisionId);
		CouchResponse response = executeRequest(documentDelete);
		LOGGER.debug("Successful document deletion: " + response.toString());
		return response;
	}
	
	private static CouchResponse executeRequest(HttpUriRequest request) throws CouchException {
		try (CloseableHttpClient client = HttpClientBuilder.create().build()) {
			request.setHeader(HttpHeaders.AUTHORIZATION, COUCH_AUTH);
			
			HttpResponse response = client.execute(request);
			
			String responseBody = null;
			HttpEntity entity = response.getEntity();
			if (entity != null) {
				responseBody = EntityUtils.toString(entity);
			}
			
			StatusLine statusLine = response.getStatusLine();
			if(statusLine == null) {
				throw new CouchException(String.format("Unsuccessful CouchDB request. Request URI: %s. Status: null.", request.getURI().toString()));
			} else if(statusLine.getStatusCode() / 100 != 2) {
				throw new CouchException(Integer.valueOf(statusLine.getStatusCode()), String.format("Unsuccessful CouchDB request. Request URI: %s. Status: %s.", request.getURI().toString(), statusLine.toString()));
			}
			
			String revision = null;
			Header eTagHeader = response.getFirstHeader("ETag");
			if(eTagHeader != null) {
				revision = eTagHeader.getValue().replace("\"", "");
			}
			
			return new CouchResponse(statusLine.getStatusCode(), responseBody, revision);
		} catch (IOException e) {
			throw new CouchException(String.format("Error during CouchDB request. Request URI: %s. Message: %s", request.getURI().toString(), e.getMessage()), e);
		}
	}
	
	/* Run this to init a new database in couch: */
	public static void main(String[] args) throws Exception {
		// (re-)initialize a DB on localhost
		
		String couchEndpoint = "http://localhost:5984/";
		String dbName = "userfiles";
		String creds = "Basic " 
				+ new String(Base64.encodeBase64("admin:admin".getBytes(StandardCharsets.ISO_8859_1)));
		
		// strong arm the credentials into the class
		Field field = CouchUtil.class.getDeclaredField("COUCH_AUTH");
		Field modifiers = Field.class.getDeclaredField("modifiers");
		boolean modifiersAccessible = modifiers.isAccessible();
		modifiers.setAccessible(true);
		modifiers.setInt(field, field.getModifiers() & ~Modifier.FINAL);
		field.setAccessible(true);
		field.set(null, creds);
		field.setAccessible(false);
		modifiers.setAccessible(modifiersAccessible);
		
		// delete if exists
		HttpDelete dbDelete = new HttpDelete(couchEndpoint + dbName);
		try {
			executeRequest(dbDelete);
			System.out.println("Successfully deleted database " + dbName);
		} catch (CouchException e) {
			if(e.getStatusCode() != HttpStatus.SC_NOT_FOUND) {
				System.out.println("Failed to delete database: " + e);
				throw e;
			}
			System.out.println("Database doesn't exist to delete");
		}
		
		// create a partitioned DB
		HttpPut dbCreate = new HttpPut(couchEndpoint + dbName + "?partitioned=true");
		try {
			executeRequest(dbCreate);
			System.out.println("Successfully created database " + dbName);
		} catch (CouchException e) {
			System.out.println("Failed to create database: " + e);
			throw e;
		}
		
		// add indexes for database, project, and insight & project field selectors
		String indexEndpoint = couchEndpoint + dbName + "/_index";
		String indexBodyTemplate = "{\"index\": {\"fields\": [%s]}, \"name\": \"%s-index\", \"type\": \"json\", \"partitioned\": true}";
		try {
			HttpPost indexCreate = new HttpPost(indexEndpoint);
			indexCreate.addHeader("Content-Type", "application/json");
			
			indexCreate.setEntity(new StringEntity(String.format(indexBodyTemplate, "\"database\"", "database")));
			executeRequest(indexCreate);
			System.out.println("Successfully created database index");
			
			indexCreate.setEntity(new StringEntity(String.format(indexBodyTemplate, "\"project\"", "project")));
			executeRequest(indexCreate);
			System.out.println("Successfully created project index");
			
			indexCreate.setEntity(new StringEntity(String.format(indexBodyTemplate, "\"project\", \"insight\"", "insight")));
			executeRequest(indexCreate);
			System.out.println("Successfully created insight index");
		} catch (CouchException e) {
			System.out.println("Failed to create all indexes: "+ e);
			throw e;
		}
	}
	/**/
}
