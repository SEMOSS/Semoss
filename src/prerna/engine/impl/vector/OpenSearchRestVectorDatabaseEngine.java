package prerna.engine.impl.vector;

import java.io.File;
import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.FilenameUtils;
import org.apache.http.HttpEntity;
import org.apache.http.HttpHeaders;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.gson.JsonArray;
import com.google.gson.JsonNull;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.google.gson.internal.LinkedTreeMap;

import prerna.ds.py.PyUtils;
import prerna.engine.api.VectorDatabaseTypeEnum;
import prerna.om.Insight;
import prerna.reactor.vector.VectorDatabaseParamOptionsEnum;
import prerna.security.HttpHelperUtility;
import prerna.util.Constants;
import prerna.util.Utility;

public class OpenSearchRestVectorDatabaseEngine extends AbstractVectorDatabaseEngine {
	
	private static final Logger classLogger = LogManager.getLogger(OpenSearchRestVectorDatabaseEngine.class);

	private static final String OPEN_SEARCH_INIT_SCRIPT = "${VECTOR_SEARCHER_NAME} = vector_database.OpenSearchConnector(embedder_engine_id = '${EMBEDDER_ENGINE_ID}', index_name = '${INDEX_NAME}', tokenizer = cfg_tokenizer, distance_method = '${DISTANCE_METHOD}')";
	
	private String mapping = "{\"settings\":{\"index\":{\"knn\":true}},\"mappings\":{\"properties\":{\"my_vector1\":{\"type\":\"knn_vector\",\"dimension\":1024,\"method\":{\"name\":\"hnsw\",\"space_type\":\"l2\",\"engine\":\"lucene\",\"parameters\":{\"ef_construction\":128,\"m\":24}}}}}}";
	
	private String indexName = null;
	private String clusterUrl = null;
	private String username = null;
	private String password = null;

	@Override
	public void open(Properties smssProp) throws Exception {
		super.open(smssProp);
		// Now that the smssprops are loaded and the folders are made we should create or get index to save time later 
		getIndex();
	}

	@Override
	protected String[] getServerStartCommands() {
		return (AbstractVectorDatabaseEngine.TOKENIZER_INIT_SCRIPT+OPEN_SEARCH_INIT_SCRIPT).split(PyUtils.PY_COMMAND_SEPARATOR);
	}
	
	@Override
	public void addDocument(List<String> filePaths, Map<String, Object> parameters) {
		if(!modelPropsLoaded) {
			verifyModelProps();
		}
		getIndex();
		String indexClass = this.defaultIndexClass;
		if (parameters.containsKey(INDEX_CLASS)) {
			indexClass = (String) parameters.get(INDEX_CLASS);
		}
		int chunkMaxTokenLength = this.contentLength;
		if (parameters.containsKey(VectorDatabaseParamOptionsEnum.CONTENT_LENGTH.getKey())) {
			chunkMaxTokenLength = (int) parameters.get(VectorDatabaseParamOptionsEnum.CONTENT_LENGTH.getKey());
		}
		
		int tokenOverlapBetweenChunks = this.contentOverlap;
		if (parameters.containsKey(VectorDatabaseParamOptionsEnum.CONTENT_OVERLAP.getKey())) {
			tokenOverlapBetweenChunks = (int) parameters.get(VectorDatabaseParamOptionsEnum.CONTENT_OVERLAP.getKey());
		}
		
		String chunkUnit = this.defaultChunkUnit;
		if (parameters.containsKey(VectorDatabaseParamOptionsEnum.CHUNK_UNIT.getKey())) {
			chunkUnit = (String) parameters.get(VectorDatabaseParamOptionsEnum.CHUNK_UNIT.getKey());
		}
		
		String extractionMethod = this.defaultExtractionMethod;
		if (parameters.containsKey(VectorDatabaseParamOptionsEnum.EXTRACTION_METHOD.getKey())) {
			extractionMethod = (String) parameters.get(VectorDatabaseParamOptionsEnum.EXTRACTION_METHOD.getKey());
		}
		
		Insight insight = getInsight(parameters.get(INSIGHT));
		if (insight == null) {
			throw new IllegalArgumentException("Insight must be provided to run Model Engine Encoder");
		}
		
		checkSocketStatus();
		
		File indexDirectory = new File(this.schemaFolder, indexName);
		File documentDir = new File(indexDirectory, "documents");
		if(!documentDir.exists()) {
			documentDir.mkdirs();
		}
		
		boolean filesAppoved = VectorDatabaseUtils.verifyFileTypes(filePaths, new ArrayList<>(Arrays.asList(documentDir.list())));
		if (!filesAppoved) {
			throw new IllegalArgumentException("Currently unable to mix csv with non-csv file types.");
		}
		
		File tableIndexFolder = new File(this.schemaFolder + DIR_SEPARATOR + indexName, "indexed_files");
		if (!tableIndexFolder.exists()) {
			tableIndexFolder.mkdirs();
		}

		String columnsToIndex = "";
		List<String> extractedFiles = new ArrayList<String>();
		List<String> filesToCopyToCloud = new ArrayList<String>(); // create a list to store all the net new files so we can push them to the cloud
		String chunkingStrategy = PyUtils.determineStringType(parameters.getOrDefault("chunkingStrategy", "ALL"));
		
		// move the documents from insight into documents folder
		HashSet<File> fileToExtractFrom = new HashSet<File>();
		for (String fileName : filePaths) {
			File fileInInsightFolder = new File(Utility.normalizePath(fileName));
			
			// Double check that they are files and not directories
			if (!fileInInsightFolder.isFile()) {
				continue;
			}
			
			File destinationFile = new File(documentDir, fileInInsightFolder.getName());
			
			// Check if the destination file exists, and if so, delete it
			try {
				if (destinationFile.exists()) {
					FileUtils.forceDelete(destinationFile);
	            }
				FileUtils.moveFileToDirectory(fileInInsightFolder, documentDir, true);
			} catch (IOException e) {
				classLogger.error(Constants.STACKTRACE, e);
				throw new IllegalArgumentException("Unable to remove previously created file for " + destinationFile.getName() + " or move it to the document directory");
			}
			
			// add it to the list of files we need to extract text from
			fileToExtractFrom.add(destinationFile);
			
			// add it to the list of files that need to be pushed to the cloud in a new thread
			filesToCopyToCloud.add(destinationFile.getAbsolutePath());
		}
		
		// loop through each document and attempt to extract text
		for (File document : fileToExtractFrom) {
			String documentName = FilenameUtils.getBaseName(document.getName());
			File extractedFile = new File(tableIndexFolder.getAbsolutePath() + DIR_SEPARATOR + documentName + ".csv");
			String extractedFileName = extractedFile.getAbsolutePath().replace(FILE_SEPARATOR, DIR_SEPARATOR);
			try {
				if (extractedFile.exists()) {
					FileUtils.forceDelete(extractedFile);
				}
				if (!document.getName().toLowerCase().endsWith(".csv")) {
					
					classLogger.info("Extracting text from document " + documentName);
					// determine which text extraction method to use
					int rowsCreated;
					if (extractionMethod.equals("fitz") && document.getName().toLowerCase().endsWith(".pdf")) {
						rowsCreated = VectorDatabaseUtils.extractTextUsingPython(pyt, document, this.schemaFolder.getAbsolutePath().replace(FILE_SEPARATOR, DIR_SEPARATOR) + DIR_SEPARATOR + indexClass + DIR_SEPARATOR + "extraction_files", extractedFileName);
					} else {
						rowsCreated = VectorDatabaseUtils.convertFilesToCSV(extractedFile.getAbsolutePath(), document);
					}
					
					// check to see if the file data was extracted
					if (rowsCreated <= 1) {
						// no text was extracted so delete the file
						FileUtils.forceDelete(extractedFile); // delete the csv
						FileUtils.forceDelete(document); // delete the input file e.g pdf
						continue;
					}
					
					classLogger.info("Creating chunks from extracted text for " + documentName);
					
					VectorDatabaseUtils.createChunksFromTextInPages(pyt, extractedFileName, chunkUnit, chunkMaxTokenLength, tokenOverlapBetweenChunks, chunkingStrategy);
					
					// this needs to match the column created in the new CSV
					columnsToIndex = "['Content']"; 
				} else {
					// copy csv over
					FileUtils.copyFileToDirectory(document, tableIndexFolder);
					if (parameters.containsKey(VectorDatabaseParamOptionsEnum.COLUMNS_TO_INDEX.getKey())) {
						columnsToIndex = PyUtils.determineStringType(parameters.get(VectorDatabaseParamOptionsEnum.COLUMNS_TO_INDEX.getKey()));
					} else {
						columnsToIndex = "[]"; // this is so we pass an empty list
					}
				}
				createMappingAndMakeCall(extractedFileName, documentName, insight, columnsToIndex, parameters);
			} catch (IOException e) {
				classLogger.error(Constants.STACKTRACE, e);
				throw new IllegalArgumentException("Unable to remove old or create new text extraction file for " + documentName);
			}
		}
	}
	
	@Override
	public void addEmbeddings(VectorDatabaseCSVTable vectorCsvTable, Insight insight, Map<String, Object> parameters) throws Exception {
		// TODO Auto-generated method stub
		// TODO Auto-generated method stub
		// TODO Auto-generated method stub
		
		// to implement
		// turn the vector csv table into the payload required
		
		throw new IllegalArgumentException("This method is not yet implemented for this engine");
	}

	/**
	 * 
	 * @param extractedFileName
	 * @param documentName
	 * @param insight
	 * @param columnsToIndex
	 * @param parameters
	 */
	private void createMappingAndMakeCall(String extractedFileName, String documentName, Insight insight, String columnsToIndex, Map<String, Object> parameters) {
		Object mappings;

		System.out.println(extractedFileName);
		documentName = documentName.replaceAll("\\s+","");
		StringBuilder addDocumentPyCommand = new StringBuilder();

		// get the relevant FAISS searcher object in python
		addDocumentPyCommand.append(vectorDatabaseSearcher);

		addDocumentPyCommand.append(".addDocumetAndCreateMapping(documentFileLocation = ['")
				.append(String.join("','", extractedFileName)).append("'], insight_id = '").append(insight.getInsightId())
				.append("', columns_to_index = ").append(columnsToIndex);

		if (parameters.containsKey(VectorDatabaseParamOptionsEnum.COLUMNS_TO_REMOVE.getKey())) {
			// add the columns based in the vector db query
			addDocumentPyCommand.append(", ").append("columns_to_remove").append(" = ").append(PyUtils
					.determineStringType(parameters.get(VectorDatabaseParamOptionsEnum.COLUMNS_TO_REMOVE.getKey())));
		}
		addDocumentPyCommand.append(")");
		classLogger.info("Running >>>" + addDocumentPyCommand.toString());
		mappings = pyt.runScript(addDocumentPyCommand.toString(), insight);
		mappings = convertKeyValueToJSON(mappings);
		CloseableHttpClient httpClient = null;
		CloseableHttpResponse response = null;
		String responseData = null;
		HttpEntity entity = null;
		try {
			httpClient = HttpClients.createDefault();
			HttpPut httpPut = new HttpPut(this.clusterUrl + "/" + this.indexName + "/" + "_doc" + "/" + documentName);
			String encodedPassword = getCredsBase64Encoded();
			httpPut.setHeader(HttpHeaders.AUTHORIZATION, "Basic " + encodedPassword);
			httpPut.setEntity(new StringEntity(mappings.toString(), ContentType.APPLICATION_JSON));
			response = httpClient.execute(httpPut);
			System.out.println(response);
		} catch (IOException e) {
			classLogger.error(Constants.STACKTRACE, e);
			throw new IllegalArgumentException("Could not connect to URL at " + this.clusterUrl);
		} finally {
			if (entity != null) {
				try {
					EntityUtils.consume(entity);
				} catch (IOException e) {
					classLogger.error(Constants.STACKTRACE, e);
				}
			}
			if (response != null) {
				try {
					response.close();
				} catch (IOException e) {
					classLogger.error(Constants.STACKTRACE, e);
				}
			}
			if (httpClient != null) {
				try {
					httpClient.close();
				} catch (IOException e) {
					classLogger.error(Constants.STACKTRACE, e);
				}
			}
		}
	}

	@Override
	public void removeDocument(List<String> fileNames, Map<String, Object> parameters) {
		List<String> successfulFiles = new ArrayList<>();
		List<String> failedFiles = new ArrayList<>();
		for(String fileName: fileNames) {
			fileName = fileName.replaceAll("\\s+","");
			String fileNameSubString = fileName.substring(0, fileName.indexOf("."));
			
	        String url = this.clusterUrl + "/" + this.indexName + "/" + "_doc" + "/" + fileNameSubString;
			Map<String, String> headersMap = new HashMap<>();
			headersMap.put(HttpHeaders.AUTHORIZATION, "Basic " + getCredsBase64Encoded());
			headersMap.put(HttpHeaders.CONTENT_TYPE, ContentType.APPLICATION_JSON.getMimeType());

			try {
				HttpHelperUtility.deleteRequestStringBody(url, headersMap, null, null, null);
				successfulFiles.add(fileName);
			} catch(Exception e) {
				classLogger.error(Constants.STACKTRACE, e);
				failedFiles.add(fileName);
			}
		}
		
		if(!successfulFiles.isEmpty()) {
			removeDocumentsFromFileDir(fileNames, parameters);
		}
	}

	/**
	 * 
	 * @param filePaths
	 * @param parameters
	 */
	private void removeDocumentsFromFileDir(List<String> filePaths, Map<String, Object> parameters) {
		String indexClass = this.defaultIndexClass;
		if (parameters.containsKey(INDEX_CLASS)) {
			indexClass = (String) parameters.get(INDEX_CLASS);
		}
					
		List<String> filesToRemoveFromCloud = new ArrayList<String>();
		String indexedFilesPath = this.schemaFolder.getAbsolutePath() + DIR_SEPARATOR + this.indexName + DIR_SEPARATOR + "indexed_files";
		Path indexDirectory = Paths.get(indexedFilesPath);
		for (String document : filePaths) {
			String documentName = document.split("\\.")[0];
	        String[] fileNamesToDelete = {documentName + "_dataset.pkl", documentName + "_vectors.pkl", documentName + ".csv"};

	        // Create a filter for the file names
	        DirectoryStream.Filter<Path> fileNameFilters = entry -> {
	            String fileName = entry.getFileName().toString();
	            for (String fileNameToDelete : fileNamesToDelete) {
	                if (fileName.equals(fileNameToDelete)) {
	                    return true;
	                }
	            }
	            return false;
	        };
	        
	        DirectoryStream<Path> stream;
			try {
				stream = Files.newDirectoryStream(indexDirectory, fileNameFilters);
			} catch (IOException e) {
				classLogger.error(Constants.STACKTRACE, e);
				throw new IllegalArgumentException("Unable determine files in " + indexDirectory.getFileName());
			}
	        for (Path entry : stream) {
                // Delete each file that matches the specified file name
                try {
					Files.delete(entry);
					filesToRemoveFromCloud.add(entry.toString());
				} catch (IOException e) {
					classLogger.error(Constants.STACKTRACE, e);
					throw new IllegalArgumentException("Unable to remove file: " + entry.getFileName());
				}
                classLogger.info("Deleted: " + entry.toString());
            }
	        try {
	        	File documentFile = new File(this.schemaFolder.getAbsolutePath() + DIR_SEPARATOR + this.indexName + DIR_SEPARATOR + "documents", document);
				FileUtils.forceDelete(documentFile);
				filesToRemoveFromCloud.add(documentFile.getAbsolutePath());
			} catch (IOException e) {
				classLogger.error(Constants.STACKTRACE, e);
				throw new IllegalArgumentException("Unable to delete " + document + "from documents directory");
			}
		}
	}

	@Override
	public List<Map<String, Object>> nearestNeighbor(String searchStatement, Number limit, Map<String, Object> parameters) {
		verifyModelProps();
		Object mappings = null;
		if(parameters.containsKey("MAPPINGS")) {
			// Do rest call and pass in the mappings as a json object 
			mappings = parameters.get("MAPPINGS");
			
		} else {
			// Invoke python to create vector of the question and pass in to a rest? 
			checkSocketStatus();
			Insight insight = getInsight(parameters.get(INSIGHT));
			if (insight == null) {
				throw new IllegalArgumentException("Insight must be provided to run Model Engine Encoder");
			}
			StringBuilder callMaker = new StringBuilder();
			callMaker.append(this.vectorDatabaseSearcher).append(".createBodyForKNN(");
			// make the question arg
			callMaker.append("question=\"\"\"")
					 .append(searchStatement.replace("\"", "\\\""))
					 .append("\"\"\"");

			callMaker.append(", insight_id='")
					 .append(insight.getInsightId())
					 .append("'");
			callMaker.append(", limit='")
			 .append(limit.toString())
			 .append("'");
			callMaker.append(")");
	 		classLogger.info("Running >>>" + callMaker.toString());
			mappings = pyt.runScript(callMaker.toString(), insight);
			mappings = convertKeyValueToJSON(mappings);
			System.out.println(mappings);
			System.out.println(mappings);
			System.out.println(mappings);
			System.out.println(mappings);
		}
		
		Map<String, String> headersMap = new HashMap<>();
		String encodedPassword = getCredsBase64Encoded();
		headersMap.put(HttpHeaders.AUTHORIZATION, "Basic " + encodedPassword);
		headersMap.put(HttpHeaders.CONTENT_TYPE, "application/json");
		headersMap.put(HttpHeaders.ACCEPT, "application/json");
		String utilResponse = HttpHelperUtility.postRequestStringBody(this.clusterUrl+"/"+this.indexName+"/_search", headersMap, mappings.toString(), ContentType.APPLICATION_JSON, null, null, null);
		System.out.println(utilResponse);
		
		
		return null;
	}

	private String getCredsBase64Encoded() {
		String encoding = Base64.getEncoder().encodeToString((this.username + ":" + this.password).getBytes());
		return encoding;
	}

	public static JsonObject convertKeyValueToJSON(Object mappings) {
		return processLinkedTreeMap((LinkedTreeMap<String, Object>) mappings);
	}
	
	public static JsonObject processLinkedTreeMap(LinkedTreeMap<String, Object> input) {
		JsonObject thisObject = new JsonObject();
		Object[] objs = input.entrySet().toArray();
		for (int i=0; i<objs.length; i++) {
			Map.Entry o = (Map.Entry) objs[i];
			String thisKey = o.getKey().toString();
			Object thisValue = o.getValue();
			if(thisValue == null) {
				thisObject.add(thisKey, JsonNull.INSTANCE);
			} else if (thisValue instanceof LinkedTreeMap) {
				thisObject.add(thisKey, processLinkedTreeMap((LinkedTreeMap<String, Object>) o.getValue()));
			} else if(thisValue instanceof Collection) {
				thisObject.add(thisKey, processArray((Collection<Object>) o.getValue()));
			} else if(thisValue  instanceof Number) {
				thisObject.addProperty(thisKey, (Number) thisValue); 
			} else if(thisValue instanceof Boolean) {
				thisObject.addProperty(thisKey, (Boolean) thisValue); 
			} else {
				thisObject.addProperty(thisKey, thisValue + ""); 
			}
		}
		return thisObject;
	}
	
	public static JsonArray processArray(Collection<Object> input) {
		JsonArray thisArr = new JsonArray();
		for(Object in : input) {
			if(in == null) {
				thisArr.add(JsonNull.INSTANCE);
			} else if(in instanceof LinkedTreeMap) {
				thisArr.add(processLinkedTreeMap((LinkedTreeMap<String, Object>) in));
			} else if(in instanceof Collection) {
				thisArr.add(processArray((Collection<Object>) in));
			} else if(in instanceof Number) {
				thisArr.add((Number) in); 
			} else if(in instanceof Boolean) {
				thisArr.add((Boolean) in); 
			} else {
				thisArr.add(in + ""); 
			}
		}
		return thisArr;
	}
	
	/**
	 * 
	 */
	private void getIndex() {
		Boolean exisits = doesIndexExsist();
		if(!exisits) {
			createIndex();
		}
	}

	/**
	 * 
	 */
	private void createIndex() {
		if(!modelPropsLoaded) {
			verifyModelProps();
		}
		
        JsonObject jsonObject = JsonParser.parseString(mapping).getAsJsonObject();
        String url = this.clusterUrl + "/" + this.indexName;
		Map<String, String> headersMap = new HashMap<>();
		headersMap.put(HttpHeaders.AUTHORIZATION, "Basic " + getCredsBase64Encoded());
		headersMap.put(HttpHeaders.CONTENT_TYPE, ContentType.APPLICATION_JSON.getMimeType());
		String body = jsonObject.toString();
		
        String response = HttpHelperUtility.putRequestStringBody(url, headersMap, body, ContentType.APPLICATION_JSON, null, null, null);
        System.out.println("Create Index Response = " + response);
	}

	/**
	 * 
	 * @return
	 */
	private Boolean doesIndexExsist() {
		if(!modelPropsLoaded) {
			verifyModelProps();
		}
		
		String url = this.clusterUrl + "/" + this.indexName;
		Map<String, String> headersMap = new HashMap<>();
		headersMap.put(HttpHeaders.AUTHORIZATION, "Basic " + getCredsBase64Encoded());
		headersMap.put(HttpHeaders.CONTENT_TYPE, "application/json");
		HttpHelperUtility.headRequest(url, headersMap, null, null, null);
		return true;
	}

	@Override
	public List<Map<String, Object>> listDocuments(Map<String, Object> parameters) {
		String indexClass = this.defaultIndexClass;
		if (parameters != null && parameters.containsKey(INDEX_CLASS)) {
			indexClass = (String) parameters.get(INDEX_CLASS);
		}
		
		File documentsDir = new File(this.schemaFolder.getAbsolutePath() + DIR_SEPARATOR + this.indexName + DIR_SEPARATOR + "documents");
		
        List<Map<String, Object>> fileList = new ArrayList<>();

		File[] files = documentsDir.listFiles();
        if (files != null) {
            for (File file : files) {
                String fileName = file.getName();
                long fileSizeInBytes = file.length();
                double fileSizeInMB = (double) fileSizeInBytes / (1024);
                SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                String lastModified = dateFormat.format(new Date(file.lastModified()));
                
                Map<String, Object> fileInfo = new HashMap<>();
                fileInfo.put("fileName", fileName);
                fileInfo.put("fileSize", fileSizeInMB);
                fileInfo.put("lastModified", lastModified);
                fileList.add(fileInfo);
            }
        } 
		
		return fileList;
	}
	
	@Override
	public VectorDatabaseTypeEnum getVectorDatabaseType() {
		return VectorDatabaseTypeEnum.OPENSEARCH_REST;
	}
}