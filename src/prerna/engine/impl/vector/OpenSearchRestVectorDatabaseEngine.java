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
import org.apache.http.client.methods.HttpDelete;
import org.apache.http.client.methods.HttpHead;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.json.JSONException;
import org.json.JSONObject;

import com.bettercloud.vault.json.Json;
import com.bettercloud.vault.json.JsonObject;
import com.google.gson.internal.LinkedTreeMap;

import prerna.ds.py.PyUtils;
import prerna.ds.py.TCPPyTranslator;
import prerna.engine.api.IModelEngine;
import prerna.engine.api.VectorDatabaseTypeEnum;
import prerna.om.ClientProcessWrapper;
import prerna.om.Insight;
import prerna.reactor.vector.VectorDatabaseParamOptionsEnum;
import prerna.util.Constants;
import prerna.util.Settings;
import prerna.util.Utility;

public class OpenSearchRestVectorDatabaseEngine extends AbstractVectorDatabaseEngine {
	
	private static final Logger classLogger = LogManager.getLogger(OpenSearchRestVectorDatabaseEngine.class);

	private File schemaFolder;
	protected String vectorDatabaseSearcher = null;
	private List<String> indexClasses;
	private static final String VECTOR_SEARCHER_NAME = "VECTOR_SEARCHER_NAME";
	private static final String DIR_SEPARATOR = "/";
	private static final String FILE_SEPARATOR = java.nio.file.FileSystems.getDefault().getSeparator();
	private static final String openSearchInitScript = "import vector_database;${VECTOR_SEARCHER_NAME} = vector_database.OpenSearchConnector(embedder_engine_id = '${EMBEDDER_ENGINE_ID}', username = '${USERNAME}', password = '${PASSWORD}', index_name = '${INDEX_NAME}', hosts = ['${HOSTS}'], tokenizer = cfg_tokenizer, distance_method = '${DISTANCE_METHOD}')";
	private static final String tokenizerInitScript = "from genai_client import get_tokenizer;cfg_tokenizer = get_tokenizer(tokenizer_name = '${MODEL}', max_tokens = ${MAX_TOKENS}, tokenizer_type = '${MODEL_TYPE}');";
	private String mapping = "{\"settings\":{\"index\":{\"knn\":true}},\"mappings\":{\"properties\":{\"my_vector1\":{\"type\":\"knn_vector\",\"dimension\":1024,\"method\":{\"name\":\"hnsw\",\"space_type\":\"l2\",\"engine\":\"lucene\",\"parameters\":{\"ef_construction\":128,\"m\":24}}}}}}";
	
	// MOVE THESE TO CLASS VARIABLES??? 
	private String indexName = null;
	private String clusterUrl = null;
	private String username = null;
	private String password = null;
	
	@Override
	public void open(Properties smssProp) throws Exception {
		super.open(smssProp);
		// highest directory (first layer inside vector db base folder)
		this.pyDirectoryBasePath = this.connectionURL + "py" + DIR_SEPARATOR;
		this.cacheFolder = new File(pyDirectoryBasePath.replace(FILE_SEPARATOR, DIR_SEPARATOR));

		// second layer - This holds all the different "tables". The reason we want this
		// is to easily and quickly grab the sub folders
		this.schemaFolder = new File(this.connectionURL, "schema");
		if (!this.schemaFolder.exists()) {
			this.schemaFolder.mkdirs();
		}
		this.smssProp.put(Constants.WORKING_DIR, this.schemaFolder.getAbsolutePath());

		// third layer - All the separate tables,classes, or searchers that can be added
		// to this db
		this.indexClasses = new ArrayList<>();
		for (File file : this.schemaFolder.listFiles()) {
			if (file.isDirectory() && !file.getName().equals("temp")) {
				this.indexClasses.add(file.getName());
			}
		}

		this.vectorDatabaseSearcher = Utility.getRandomString(6);

		this.smssProp.put(VECTOR_SEARCHER_NAME, this.vectorDatabaseSearcher);
		
		// Now that the smssprops are loaded and the folders are made we should create or get index to save time later 
		getIndex();
	}

	/**
	 * 
	 */
	protected void verifyModelProps() {
		String embedderEngineId = this.smssProp.getProperty(Constants.EMBEDDER_ENGINE_ID);

		if(this.smssProp.getProperty("INDEX_NAME") != null) { this.indexName = this.smssProp.getProperty("INDEX_NAME");}
		if(this.smssProp.getProperty("HOSTS") != null) { this.clusterUrl = Utility.decodeURIComponent(this.smssProp.getProperty("HOSTS"));}
		if(this.smssProp.getProperty("USERNAME") != null) { this.username = this.smssProp.getProperty("USERNAME");}
		if(this.smssProp.getProperty("PASSWORD") != null) { this.password = Utility.decodeURIComponent(this.smssProp.getProperty("PASSWORD"));}
		
		// If embedderEngine does not exist, do not set up the following
		if(embedderEngineId != null && !embedderEngineId.isEmpty()) {
			IModelEngine modelEngine = Utility.getModel(embedderEngineId);
			if (modelEngine == null) {
				throw new IllegalArgumentException("Model Engine must be created and contain MODEL");
			}

			Properties modelProperties = modelEngine.getSmssProp();
			if (modelProperties.isEmpty() || !modelProperties.containsKey(Constants.MODEL)) {
				throw new IllegalArgumentException("Model Engine must be created and contain MODEL");
			}

			this.smssProp.put(Constants.MODEL, modelProperties.getProperty(Constants.MODEL));
			this.smssProp.put(IModelEngine.MODEL_TYPE, modelProperties.getProperty(IModelEngine.MODEL_TYPE));
		}
		
		for (Object smssKey : this.smssProp.keySet()) {
			String key = smssKey.toString();
			this.vars.put(key, this.smssProp.getProperty(key));
		}
		
		modelPropsLoaded = true;
	}
	
	protected synchronized void startServer(int port) {
		// already created by another thread
		if(this.cpw != null && this.cpw.getSocketClient() != null && this.cpw.getSocketClient().isConnected()) {
			return;
		}
				
		if(!modelPropsLoaded) {
			verifyModelProps();
		}
		
		// break the commands seperated by ;
		String [] commands = (tokenizerInitScript+openSearchInitScript).split(PyUtils.PY_COMMAND_SEPARATOR);
		
		// replace the Vars
		for(int commandIndex = 0; commandIndex < commands.length;commandIndex++) {
			commands[commandIndex] = fillVars(commands[commandIndex]);
		}
		
		// Start and connect to the open search instance
		if(!this.cacheFolder.exists()) {
			this.cacheFolder.mkdirs();
		}
		
		// check if we have already created a process wrapper
		if(this.cpw == null) {
			this.cpw = new ClientProcessWrapper();
		}
		
		String timeout = "30";
		if(this.smssProp.containsKey(Constants.IDLE_TIMEOUT)) {
			timeout = this.smssProp.getProperty(Constants.IDLE_TIMEOUT);
		}
		
		if (this.cpw.getSocketClient() == null) {
			boolean debug = false;

			// pull the relevant values from the smss
			String forcePort = this.smssProp.getProperty(Settings.FORCE_PORT);
			String customClassPath = this.smssProp.getProperty("TCP_WORKER_CP");
			String loggerLevel = this.smssProp.getProperty(Settings.LOGGER_LEVEL, "WARNING");
			String venvEngineId = this.smssProp.getProperty(Constants.VIRTUAL_ENV_ENGINE, null);
			String venvPath = venvEngineId != null ? Utility.getVenvEngine(venvEngineId).pathToExecutable() : null;
					
			if (port < 0) {
				// port has not been forced
				if (forcePort != null && !(forcePort = forcePort.trim()).isEmpty()) {
					try {
						port = Integer.parseInt(forcePort);
						debug = true;
					} catch (NumberFormatException e) {
						// ignore
						classLogger
								.warn("OpenSearch connection " + this.engineName + " has an invalid FORCE_PORT value");
					}
				}
			}
			String serverDirectory = this.cacheFolder.getAbsolutePath();
			boolean nativePyServer = true; // it has to be -- don't change this unless you can send engine calls from
											// python
			try {
				this.cpw.createProcessAndClient(nativePyServer, null, port, venvPath, serverDirectory, customClassPath,
						debug, timeout, loggerLevel);
			} catch (Exception e) {
				classLogger.error(Constants.STACKTRACE, e);
				throw new IllegalArgumentException("Unable to connect to server for faiss databse.");
			}
		} else if (!this.cpw.getSocketClient().isConnected()) {
			this.cpw.shutdown(false);
			try {
				this.cpw.reconnect();
			} catch (Exception e) {
				classLogger.error(Constants.STACKTRACE, e);
				throw new IllegalArgumentException(
						"Failed to start TCP Server for OpenSearch Connection  = " + this.engineName);
			}
		}
		
		// create the py translator
		pyt = new TCPPyTranslator();
		pyt.setSocketClient(this.cpw.getSocketClient());
	
		// TODO remove once bug is caught / fixed
		StringBuilder intitPyCommands = new StringBuilder("\n");
		for (String command : commands) {
			intitPyCommands.append(command).append("\n");
		}		
		classLogger.info("Initializing OpenSearch Connection with the following py commands >>>" + intitPyCommands.toString());
		pyt.runEmptyPy(commands);
	}
	
	@Override
	public VectorDatabaseTypeEnum getVectorDatabaseType() {
		return VectorDatabaseTypeEnum.OPENSEARCH_REST;
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
			String encodedPassword = getEncoding();
			httpPut.setHeader(HttpHeaders.AUTHORIZATION, "Basic " + encodedPassword);
//			httpPut.setHeader(HttpHeaders.CONTENT_TYPE, "application/json");
			httpPut.setEntity(new StringEntity(mappings.toString(), ContentType.APPLICATION_JSON));
			response = httpClient.execute(httpPut);
			System.out.println(response);
//			JsonObject jsonResponse = Json.parse(responseData).asObject();
//			System.out.println(jsonResponse.toString());
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
		for(String fileName: fileNames) {
			fileName = fileName.replaceAll("\\s+","");
			String fileNameSubString = fileName.substring(0, fileName.indexOf("."));
			CloseableHttpClient httpClient = null;
			CloseableHttpResponse response = null;
			String responseData = null;
			HttpEntity entity = null;
			try {
				httpClient = HttpClients.createDefault();
				HttpDelete httpDelete = new HttpDelete(
						this.clusterUrl + "/" + this.indexName + "/" + "_doc" + "/" + fileNameSubString);
				String encodedPassword = getEncoding();
				httpDelete.setHeader(HttpHeaders.AUTHORIZATION, "Basic " + encodedPassword);
				httpDelete.setHeader(HttpHeaders.CONTENT_TYPE, "application/json");
				response = httpClient.execute(httpDelete);
				int statusCode = response.getStatusLine().getStatusCode();
				if(statusCode == 200) {
					removeDocumentsFromFileDir(fileNames, parameters);
				} else {
					throw new IllegalArgumentException("Unable to delete from open search cluster");
				}
				System.out.println(response);
//				JsonObject jsonResponse = Json.parse(responseData).asObject();
//				System.out.println(jsonResponse.toString());
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
		
//		if (!this.indexClasses.contains(indexClass)) {
//			throw new IllegalArgumentException("Unable to remove documents from a directory that does not exist");
//		}
			
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
		Object mappings;
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
		}
		CloseableHttpClient httpClient = null;
	    CloseableHttpResponse response = null;
	    String responseData = null;
	    HttpEntity entity = null;
		try {
			httpClient = HttpClients.createDefault();
			HttpPost httpPost = new HttpPost(this.clusterUrl+"/_search");
			String encodedPassword = getEncoding();
			httpPost.setHeader(HttpHeaders.AUTHORIZATION, "Basic " + encodedPassword);
			httpPost.setHeader(HttpHeaders.CONTENT_TYPE, "application/json");
			httpPost.setEntity(new StringEntity(mappings.toString()));
			response = httpClient.execute(httpPost);
			System.out.println(response);
			JsonObject jsonResponse = Json.parse(responseData).asObject();
			System.out.println(jsonResponse.toString());
		} catch (IOException e) {
			classLogger.error(Constants.STACKTRACE, e);
			throw new IllegalArgumentException("Could not connect to URL at " + this.clusterUrl);
		} finally {
			if(entity != null) {
				try {
					EntityUtils.consume(entity);
				} catch (IOException e) {
					classLogger.error(Constants.STACKTRACE, e);
				}
			}
			if(response != null) {
				try {
					response.close();
				} catch (IOException e) {
					classLogger.error(Constants.STACKTRACE, e);
				}
			}
			if(httpClient != null) {
				try {
					httpClient.close();
				} catch (IOException e) {
					classLogger.error(Constants.STACKTRACE, e);
				}
			}
		}
		return null;
	}

	private String getEncoding() {
		String encoding = Base64.getEncoder().encodeToString((this.username + ":" + this.password).getBytes());
		return encoding;
	}

	public static JSONObject convertKeyValueToJSON(Object mappings) {
        JSONObject jo=new JSONObject();
        Object[] objs = ((LinkedTreeMap<String, Object>) mappings).entrySet().toArray();
        for (int l=0;l<objs.length;l++)
        {
            Map.Entry o= (Map.Entry) objs[l];
            try {
                if (o.getValue() instanceof LinkedTreeMap)
                    jo.put(o.getKey().toString(),convertKeyValueToJSON((LinkedTreeMap<String, Object>) o.getValue()));
                else
                    jo.put(o.getKey().toString(),o.getValue());
            } catch (JSONException e) {
                e.printStackTrace();
            }
        }
        return jo;
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
		
        JSONObject jsonObject = new JSONObject(mapping);
        CloseableHttpClient httpClient = null;
		CloseableHttpResponse response = null;
		String responseData = null;
		HttpEntity entity = null;
		try {
			httpClient = HttpClients.createDefault();
			HttpPut httpPut = new HttpPut(this.clusterUrl + "/" + this.indexName);
			String encodedPassword = getEncoding();
			httpPut.setHeader(HttpHeaders.AUTHORIZATION, "Basic " + encodedPassword);
			httpPut.setHeader(HttpHeaders.CONTENT_TYPE, "application/json");
			httpPut.setEntity(new StringEntity(jsonObject.toString(), ContentType.APPLICATION_JSON));
			response = httpClient.execute(httpPut);
			System.out.println(response);
//			JsonObject jsonResponse = Json.parse(responseData).asObject();
//			System.out.println(jsonResponse.toString());
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

	/**
	 * 
	 * @return
	 */
	private Boolean doesIndexExsist() {
		if(!modelPropsLoaded) {
			verifyModelProps();
		}
		CloseableHttpClient httpClient = null;
		CloseableHttpResponse response = null;
		String responseData = null;
		HttpEntity entity = null;
		try {
			httpClient = HttpClients.createDefault();
			HttpHead httphead = new HttpHead(this.clusterUrl + "/" + this.indexName);
			String encodedPassword = getEncoding();
			httphead.setHeader(HttpHeaders.AUTHORIZATION, "Basic " + encodedPassword);
			httphead.setHeader(HttpHeaders.CONTENT_TYPE, "application/json");
			response = httpClient.execute(httphead);
			int statusCode = response.getStatusLine().getStatusCode();
			if(statusCode == 200) {
				return true;
			} else {
				return false;
			}
//			System.out.println(response);
//			JsonObject jsonResponse = Json.parse(responseData).asObject();
//			System.out.println(jsonResponse.toString());
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
	
}