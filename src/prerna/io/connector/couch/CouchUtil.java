package prerna.io.connector.couch;

import java.awt.image.BufferedImage;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
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
import prerna.auth.utils.SecurityProjectUtils;
import prerna.cluster.util.ClusterUtil;
import prerna.engine.impl.SmssUtilities;
import prerna.nameserver.utility.MasterDatabaseUtility;
import prerna.util.AssetUtility;
import prerna.util.Constants;
import prerna.util.DIHelper;
import prerna.util.insight.InsightUtility;
import prerna.util.insight.TextToGraphic;

/**
 * Utility class to handle image operations in a partitioned CouchDB database.
 * This allows downloads, uploads, and deletion of images in the partitions for
 * databases, projects, and insights. One should verify that CouchDB is enabled
 * before using this class via the
 * <a href="#{@link}">{@link CouchUtil#COUCH_ENABLED}</a> flag.
 */
public class CouchUtil {
	
	public static final String DATABASE = "database";
	public static final String STORAGE = "storage";
	public static final String MODEL = "model";
	public static final String VECTOR = "vector";
	public static final String FUNCTION = "function";
	public static final String INSIGHT = "insight";
	public static final String PROJECT = "project";
	
	private static final Logger classLogger = LogManager.getLogger(CouchUtil.class);
	
	private static final String DIR_SEPARATOR = java.nio.file.FileSystems.getDefault().getSeparator();
	
	private static final String COUCH_ENABLED_KEY = "COUCH_ENABLED";
	/**
	 * boolean indicator that CouchDB is configured to be enabled
	 */
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
	
	/**
	 * Retrieve the image attachment data from a CouchDB document in the given
	 * partition with matching field data. The entries of the map are used to form a
	 * document selector used to query CouchDB for matching documents in the
	 * partition. If a document is found, the attachment data is retrieved.
	 * Otherwise, a new document with a default image attachment is created. The
	 * retrieved or created image data is used to build a JAX-RS Response object to
	 * download it.
	 * 
	 * @param partitionId   The partition of the database to query for document
	 *                      attachments
	 * @param referenceData A map whose key-value pairs are used to build a document
	 *                      selector
	 * @return A download <a href="#{@link}">{@link Response}</a> containing the
	 *         bytes of the attachment
	 * @see CouchUtil#getSelectorString
	 * @see CouchUtil#retrieveDocumentsInPartitionForSelector
	 * @see CouchUtil#retrieveDocument
	 * @see CouchUtil#createDefault
	 * @throws IllegalArgumentException If the referenceData map is null or empty
	 * @throws CouchException           If another exception is encountered
	 */
	public static Response download(String partitionId, Map<String, String> referenceData) throws CouchException {
		if(referenceData == null || referenceData.isEmpty()) {
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
			classLogger.error("Error building byte digest", e);
		}
		
		ResponseBuilder builder = Response.ok(attachmentBytes)
				.header("Content-Disposition", "attachment; filename=\"" + attachmentId + "\"");
		if(eTag != null) {
			builder = builder.tag(eTag);
		}
		return builder.build();
	}
	
	/**
	 * Upload image data from a File to CouchDB with the given partition and
	 * document field data. The entries of the map are used to form a document
	 * selector used to query CouchDB for matching documents in the partition. If a
	 * document is found, it is updated to contain the fields and attachment
	 * provided. Otherwise, a new document with an attachment of the imageFile
	 * contents is created.
	 * 
	 * @param partitionId   The partition of the database that will contain the
	 *                      document
	 * @param referenceData A map whose key-value pairs are placed in the document's
	 *                      fields
	 * @param imageFile     A <a href="#{@link}">{@link File}</a> whose contents
	 *                      will be attached to the document
	 * 
	 * @see CouchUtil#getSelectorString
	 * @see CouchUtil#retrieveDocumentsInPartitionForSelector
	 * @see CouchUtil#updateDocument
	 * @throws IllegalArgumentException If the referenceData map is null or invalid
	 *                                  for the partition
	 * @throws CouchException           If another exception is encountered
	 */
	public static void upload(String partitionId, Map<String, String> referenceData, File imageFile) throws CouchException {
		if(referenceData == null || !referenceData.containsKey(partitionId)) {
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
					contentType, FileUtils.readFileToByteArray(imageFile), true);
		} catch (IOException e) {
			classLogger.error(Constants.STACKTRACE, e);
			throw new CouchException("Error processing upload", e);
		}
	}
	
	/**
	 * Upload image data from a FileItem to CouchDB with the given partition and
	 * document field data. The entries of the map are used to form a document
	 * selector used to query CouchDB for matching documents in the partition. If a
	 * document is found, it is updated to contain the fields and attachment
	 * provided. Otherwise, a new document with an attachment of the imageFile
	 * contents is created.
	 * 
	 * @param partitionId   The partition of the database that will contain the
	 *                      document
	 * @param referenceData A map whose key-value pairs are placed in the document's
	 *                      fields
	 * @param imageFile     A <a href="#{@link}">{@link FileItem}</a> whose contents
	 *                      will be attached to the document
	 * 
	 * @see CouchUtil#getSelectorString
	 * @see CouchUtil#retrieveDocumentsInPartitionForSelector
	 * @see CouchUtil#updateDocument
	 * @throws IllegalArgumentException If the referenceData map is null or invalid
	 *                                  for the partition
	 * @throws CouchException           If another exception is encountered
	 */
	public static void upload(String partitionId, Map<String, String> referenceData, FileItem imageFile) throws CouchException {
		if(referenceData == null || !referenceData.containsKey(partitionId)) {
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
					imageFile.getContentType(), IOUtils.toByteArray(imageFile.getInputStream()), true);
		} catch (IOException e) {
			classLogger.error(Constants.STACKTRACE, e);
			throw new CouchException("Error processing upload", e);
		}
	}
	
	/**
	 * Delete a document from CouchDB in the given partition with the matching
	 * document field data. The entries of the map are used to form a document
	 * selector used to query CouchDB for matching documents in the partition. If a
	 * document is found, it is deleted.
	 * 
	 * @param partitionId   The partition of the database that will contain the
	 *                      document
	 * @param referenceData A map whose key-value pairs are placed in the document's
	 *                      fields
	 * 
	 * @see CouchUtil#getSelectorString
	 * @see CouchUtil#retrieveDocumentsInPartitionForSelector
	 * @see CouchUtil#deleteDocument
	 * @throws IllegalArgumentException If the referenceData map is null or empty
	 * @throws CouchException           If another exception is encountered
	 */
	public static void delete(String partitionId, Map<String, String> referenceData) throws CouchException {
		if(referenceData == null || referenceData.isEmpty()) {
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
				classLogger.warn("Couch deletion call on missing document: " + selector);
				return;
			}
			documentId = docJson.path("_id").textValue();
			revisionId = docJson.path("_rev").textValue();
			deleteDocument(documentId, revisionId);
		} catch (JsonProcessingException e) {
			classLogger.error(Constants.STACKTRACE, e);
			throw new CouchException("Error processing delete", e);
		}
	}
	
	/**
	 * Verify if a document with the given selector exists in the CouchDB partition.
	 * 
	 * @param partitionId   The partition of the database to query for the document
	 * @param searchSelector The JSON string of the search selector
	 * @return A boolean representing if the document exists or not
	 * @see CouchUtil#retrieveDocumentsInPartitionForSelector
	 * @throws IllegalArgumentException If the searchSelector is null or empty
	 * @throws CouchException           If another exception is encountered
	 */
	public static boolean documentExists(String partitionId, String searchSelector) throws CouchException {
		if(searchSelector == null || searchSelector.isEmpty()) {
			throw new IllegalArgumentException("Invalid searchSelector");
		}
		try {
			CouchResponse searchResponse = CouchUtil.retrieveDocumentsInPartitionForSelector(partitionId,
					searchSelector);
			JsonNode searchRespJson;
				searchRespJson = MAPPER.readTree(searchResponse.getResponseBody());
			ObjectNode docJson = (ObjectNode) searchRespJson.path("docs").get(0);
			return docJson != null;
		} catch (JsonProcessingException e) {
			classLogger.error(Constants.STACKTRACE, e);
			throw new CouchException("Error processing search", e);
		}
	}
	
	/**
	 * Build a JSON selector string for the given map. The entries of the map are
	 * used to form a document selector with key = value over all key-value pairs in
	 * the map. The JSON created is of the form: {"selector": { "key1": {"$eq":
	 * "value1"}, "key2": {"$eq": "value2"}, ... }}
	 * 
	 * @param referenceData A map whose key-value pairs are used to build the
	 *                      selector
	 * @return The selector JSON String
	 */
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
	
	/**
	 * Build a default image for the given partition and data. If the provided
	 * documentData includes an _id, indicating a document already exists, a
	 * revision tag is sought for the eventual CouchDB update of the image
	 * attachment. The default image is created by first searching for a local image
	 * in the associated DB, project, and insight image locations. If found, the
	 * byte array contents are returned. Otherwise, an image is formed based on the
	 * database/project name or the insight layout type as appropriate. Before
	 * returning, the image is also uploaded to CouchDB for later use.
	 * 
	 * @param partitionId  The partition of the database that will contain the image
	 * @param documentData A <a href="#{@link}">{@link ObjectNode}</a> with contents
	 *                     needed for the partition choice
	 * @return The byte[] of image contents
	 * @see CouchUtil#retrieveDocumentInfo
	 * @see CouchUtil#updateDocument
	 * @see InsightUtility#findImageFile
	 * @see TextToGraphic#buildBufferedImage
	 * @see AbstractSecurityUtils#getStockImage
	 * @throws CouchException If an exception is encountered
	 */
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
							+ DIR_SEPARATOR + Constants.DATABASE_FOLDER 
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
				String projectName = SecurityProjectUtils.getProjectAliasForId(projectId);
				
				File[] images;
				if(ClusterUtil.IS_CLUSTER) {
					String imagePath = ClusterUtil.IMAGES_FOLDER_PATH
							+ DIR_SEPARATOR + "projects";
					images = InsightUtility.findImageFile(imagePath, projectId);
				} else {
					String imagePath = AssetUtility.getProjectVersionFolder(projectName, projectId);
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
				String projectName = SecurityProjectUtils.getProjectAliasForId(projectId);
				String imagePath = AssetUtility.getProjectVersionFolder(projectName, projectId)
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
			updateDocument(documentId, revisionId, documentData, attachmentName, contentType, fileContent, false);
			return fileContent;
		} catch (IOException e) {
			classLogger.error(Constants.STACKTRACE, e);
			throw new CouchException("Error processing image creation", e);
		}
	}
	
	/**
	 * Call CouchDB with an HTTP HEAD request to /{db}/{docid} to lookup document
	 * information.
	 * 
	 * @param documentId The document identifier used by CouchDB
	 * @return A <a href="#{@link}">{@link CouchResponse}</a> wrapper for the HTTP
	 *         response
	 * @see CouchUtil#executeRequest
	 * @see <a href=
	 *      "https://docs.couchdb.org/en/stable/api/document/common.html#head--db-docid">Couch
	 *      API Reference on Document Info Lookup</a>
	 * @throws CouchException If an exception is encountered during the request
	 */
	private static CouchResponse retrieveDocumentInfo(String documentId) throws CouchException {
		HttpHead documentInfoGet = new HttpHead(COUCH_ENDPOINT + documentId);
		CouchResponse response = executeRequest(documentInfoGet);
		classLogger.debug("Successfully retrieved info: " + response.toString());
		return response;
	}
	
	/**
	 * Call CouchDB with an HTTP PUT request to /{db}/{docid} to update the
	 * document. The provided revisionId, if non-null, is used as a query parameter
	 * in the PUT. The provided attachment data is used to update the _attachments
	 * portion of the documentData before the request is performed.
	 * 
	 * @param documentId            The document identifier for use by CouchDB
	 * @param revisionId            Revision identifier used in the PUT when the
	 *                              document exists. Null otherwise
	 * @param documentData          A <a href="#{@link}">{@link ObjectNode}</a>
	 *                              whose contents are mapped to the CouchDB
	 *                              document
	 * @param attachmentName        The name of the attachment as it will appear in
	 *                              the _attachments object. For example, image.png
	 * @param attachmentContentType The content type of the attachment. For example,
	 *                              image/png
	 * @param attachmentBytes       The byte array contents of the attachment. These
	 *                              will be Base64 encoded before being added to the
	 *                              document data
	 * @param ignoreExtension		Boolean if true, to ignore the extension and remove
	 * 								all attachments that contains the same file name
	 * @return A <a href="#{@link}">{@link CouchResponse}</a> wrapper for the HTTP
	 *         response
	 * @see CouchUtil#executeRequest
	 * @see <a href=
	 *      "https://docs.couchdb.org/en/stable/api/document/common.html#put--db-docid">Couch
	 *      API Reference on Document Updates</a>
	 * @throws IllegalArgumentException If the documentData fails to be encoded into
	 *                                  the request
	 * @throws CouchException           If another exception is encountered
	 */
	private static CouchResponse updateDocument(String documentId, String revisionId, ObjectNode documentData, String attachmentName, 
			String attachmentContentType, byte[] attachmentBytes, boolean ignoreExtension) throws CouchException {
		String attachmentData = new String(Base64.encodeBase64(attachmentBytes));
		
		ObjectNode attachmentsNode = null;
		if(documentData.has("_attachments")) {
			attachmentsNode = (ObjectNode) documentData.get("_attachments");
		} else {
			attachmentsNode = documentData.putObject("_attachments");
		}
		
		if(!ignoreExtension) {
			if(attachmentsNode.has(attachmentName)) {
				attachmentsNode.remove(attachmentName);
			}
		} else {
			// loop through and remove all attachments with the same name (minus extension)
			// as what we are uploading
			String attachmentNameNoExt = attachmentName.substring(0, attachmentName.lastIndexOf('.'));
			Set<String> removeSet = new HashSet<>();
			Iterator<String> existingAttachmentsIt = attachmentsNode.fieldNames();
			while(existingAttachmentsIt.hasNext()) {
				String existingAttachment = existingAttachmentsIt.next();
				String existingAttachmentNameNoExt = existingAttachment.substring(0, existingAttachment.lastIndexOf('.'));
				if(existingAttachmentNameNoExt.equals(attachmentNameNoExt)) {
					removeSet.add(existingAttachment);
				}
			}
			// remove all the existing keys
			if(!removeSet.isEmpty()) {
				attachmentsNode.remove(removeSet);
			}
		}
		
		// add the new attachment to the node
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
			classLogger.debug("Successful document creation: " + response.toString());
			return response;
		} catch (UnsupportedEncodingException e) {
			throw new IllegalArgumentException("The document data encoding isn't supported", e);
		}
	}
	
	/**
	 * Call CouchDB with an HTTP POST request to /_partition/{partitionId}/_find to
	 * search for documents in the partition matching the given selector string.
	 * 
	 * @param partitionId The partition of the database to search for documents in
	 * @param selector    A JSON selector string with the search criteria
	 * @return A <a href="#{@link}">{@link CouchResponse}</a> wrapper for the HTTP
	 *         response
	 * @see CouchUtil#executeRequest
	 * @see <a href=
	 *      "https://docs.couchdb.org/en/stable/api/partitioned-dbs.html#db-partition-partition-id-find">Couch
	 *      API Reference on Document Search in Partition</a>
	 * @see <a href=
	 *      "https://docs.couchdb.org/en/stable/api/database/find.html#selector-syntax">Couch
	 *      API Reference on Selector Syntax</a>
	 * @throws IllegalArgumentException If the selector fails to be encoded into the
	 *                                  request
	 * @throws CouchException           If another exception is encountered
	 */
	private static CouchResponse retrieveDocumentsInPartitionForSelector(String partitionId, String selector) throws CouchException {
		try {
			HttpPost findPost = new HttpPost(COUCH_ENDPOINT + "_partition/" + partitionId + "/_find");
			findPost.setEntity(new StringEntity(selector));
			// Explicitly tell CouchDB to expect a JSON to avoid 415 errors
			findPost.setHeader(HttpHeaders.CONTENT_TYPE, "application/json");
			CouchResponse response = executeRequest(findPost);
			classLogger.debug("Successfully retrieved documents for selector: " + response.toString());
			return response;
		} catch (UnsupportedEncodingException e) {
			throw new IllegalArgumentException("The selector encoding isn't supported", e);
		}
	}
	
	/**
	 * Call CouchDB with an HTTP GET request to /{db}/{docId} to retrieve a document
	 * with the given identifier and the option to include attachment data.
	 * 
	 * @param documentId      The document identifier as used by CouchDB
	 * @param withAttachments A boolean flagging if the document attachment contents
	 *                        should be requested as well
	 * @return A <a href="#{@link}">{@link CouchResponse}</a> wrapper for the HTTP
	 *         response
	 * @see CouchUtil#executeRequest
	 * @see <a href=
	 *      "https://docs.couchdb.org/en/stable/api/document/common.html#get--db-docid">Couch
	 *      API Reference on Document Retrieval</a>
	 * @throws CouchException If an exception is encountered
	 */
	private static CouchResponse retrieveDocument(String documentId, boolean withAttachments) throws CouchException {
		HttpGet documentGet = new HttpGet(COUCH_ENDPOINT + documentId + "?attachments=" + withAttachments);
		// add accepts application/json to get the attachment data in the JSON structure instead of as multipart
		documentGet.setHeader(HttpHeaders.ACCEPT, "application/json");
		CouchResponse response = executeRequest(documentGet);
		classLogger.debug("Successful document retrieval: " + response.toString());
		return response;
	}
	
	/**
	 * Call CouchDB with an HTTP DELETE request to /{db}/{docId} to delete a
	 * document with the given identifier and revision.
	 * 
	 * @param documentId The document identifier as used by CouchDB
	 * @param revisionId The current revision code used by CouchDB to validate the
	 *                   deletion request
	 * @return A <a href="#{@link}">{@link CouchResponse}</a> wrapper for the HTTP
	 *         response
	 * @see CouchUtil#executeRequest
	 * @see <a href=
	 *      "https://docs.couchdb.org/en/stable/api/document/common.html#delete--db-docid">Couch
	 *      API Reference on Document Deletes</a>
	 * @throws CouchException If an exception is encountered
	 */
	private static CouchResponse deleteDocument(String documentId, String revisionId) throws CouchException {
		HttpDelete documentDelete = new HttpDelete(COUCH_ENDPOINT + documentId + "?rev=" + revisionId);
		CouchResponse response = executeRequest(documentDelete);
		classLogger.debug("Successful document deletion: " + response.toString());
		return response;
	}
	
	/**
	 * A generalized helper method to send a variety of HTTP requests to CouchDB.
	 * All requests get a header added for request authorization.
	 * 
	 * @param request The <a href="#{@link}">{@link HttpUriRequest}</a> request to
	 *                execute with the default
	 *                <a href="#{@link}">{@link CloseableHttpClient}</a>
	 * @return A <a href="#{@link}">{@link CouchResponse}</a> wrapper for the HTTP
	 *         response
	 * @throws CouchException If an exception is encountered
	 */
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
	
	/* Run this to init a new database in couch:
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
		
	}*/
}
