package prerna.engine.impl.vector;

import java.io.File;
import java.io.IOException;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.text.StringSubstitutor;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import prerna.auth.User;
import prerna.auth.utils.SecurityEngineUtils;
import prerna.cluster.util.ClusterUtil;
import prerna.cluster.util.CopyFilesToEngineRunner;
import prerna.ds.py.PyTranslator;
import prerna.ds.py.PyUtils;
import prerna.engine.api.ICustomEmbeddingsFunctionEngine;
import prerna.engine.api.IEngine;
import prerna.engine.api.IFunctionEngine;
import prerna.engine.api.IModelEngine;
import prerna.engine.api.IVectorDatabaseEngine;
import prerna.engine.impl.AbstractEngine;
import prerna.engine.impl.SmssUtilities;
import prerna.engine.impl.model.workers.ModelEngineInferenceLogsWorker;
import prerna.engine.impl.vector.metadata.VectorDatabaseMetadataCSVTable;
import prerna.om.ClientProcessWrapper;
import prerna.om.Insight;
import prerna.om.InsightStore;
import prerna.om.ThreadStore;
import prerna.reactor.vector.VectorDatabaseParamOptionsEnum;
import prerna.util.Constants;
import prerna.util.EngineUtility;
import prerna.util.Settings;
import prerna.util.Utility;

public abstract class AbstractVectorDatabaseEngine extends AbstractEngine implements IVectorDatabaseEngine {

	private static final Logger classLogger = LogManager.getLogger(AbstractVectorDatabaseEngine.class);
	
	protected static final String TOKENIZER_INIT_SCRIPT = "from genai_client import get_tokenizer;"
			+ "cfg_tokenizer = get_tokenizer(tokenizer_name = '${MODEL}', max_tokens = ${MAX_TOKENS}, tokenizer_type = '${MODEL_TYPE}');"
			+ "import vector_database;";
	
	public static final String LATEST_VECTOR_SEARCH_STATEMENT = "LATEST_VECTOR_SEARCH_STATEMENT";
	
	public static final String INDEX_CLASS = "indexClass";

	public static final String DOCUMENTS_FOLDER_NAME = "documents";
	public static final String INDEXED_FOLDER_NAME = "indexed_files";
	
	public static final String METADATA = "metadata";
	public static final String FILTERS_KEY = "filters";
	public static final String METADATA_FILTERS_KEY = "metaFilters";
	
	public static final String CSVPATH = "csvPath";
	public static final String DOCUMENT = "document";
	public static final String PARAMETERS = "parameters";
	
	protected String encoderName = null;
	protected String encoderType = null;

	protected int contentLength = 512;
	protected int contentOverlap = 0;
	
	protected boolean modelPropsLoaded = false;
	protected String embedderEngineId = null;
	protected String keywordGeneratorEngineId = null;
	
	protected String defaultChunkUnit;
	protected String defaultExtractionMethod;
	protected String defaultChunkingMethod;
	
    protected boolean customDocumentProcessor = false;
    protected String customDocumentProcessorFunctionID = null;

    protected String imageEngineId;
    
	// our paradigm for how we store files
	protected String defaultIndexClass;
	protected List<String> indexClasses;
	
	protected String distanceMethod;
	
	// maintain details in the log database
	protected boolean keepInputOutput = false;
	protected boolean inferenceLogsEnbaled = Utility.isModelInferenceLogsEnabled();

	protected ClientProcessWrapper cpw = null;
	// python server
	protected PyTranslator pyTranslator = null;
	protected File pyDirectoryBasePath = null;
	
	protected File schemaFolder;

	// string substitute vars
	protected Map<String, String> vars = new HashMap<>();
	
	@Override
	public void open(Properties smssProp) throws Exception {
		super.open(smssProp);
		
		if (this.smssProp.containsKey(Constants.CONTENT_LENGTH)) {
			this.contentLength = Integer.parseInt(this.smssProp.getProperty(Constants.CONTENT_LENGTH));
		}
		if (this.smssProp.containsKey(Constants.CONTENT_OVERLAP)) {
			this.contentOverlap = Integer.parseInt(this.smssProp.getProperty(Constants.CONTENT_OVERLAP));
		}

		this.keepInputOutput = Boolean.parseBoolean(this.smssProp.getProperty(Constants.KEEP_INPUT_OUTPUT));

		this.defaultChunkUnit = "tokens";
		if (this.smssProp.containsKey(Constants.DEFAULT_CHUNK_UNIT)) {
			this.defaultChunkUnit = this.smssProp.getProperty(Constants.DEFAULT_CHUNK_UNIT).toLowerCase().trim();
			if (!this.defaultChunkUnit.equals("tokens") && !this.defaultChunkUnit.equals("characters")){
	            throw new IllegalArgumentException("DEFAULT_CHUNK_UNIT should be either 'tokens' or 'characters'");
			}
		}
		
		this.defaultExtractionMethod = this.smssProp.getProperty(Constants.EXTRACTION_METHOD, "None");
		this.distanceMethod = this.smssProp.getProperty(Constants.DISTANCE_METHOD, getDefaultDistanceMethod());
		this.defaultChunkingMethod = this.smssProp.getProperty(Constants.DEFAULT_CHUNKING_METHOD, "recursive");
		
		this.defaultIndexClass = "default";
		if (this.smssProp.containsKey(Constants.INDEX_CLASSES)) {
			this.defaultIndexClass = this.smssProp.getProperty(Constants.INDEX_CLASSES);
		}
		
        // smss properties for custom document processing
        if (this.smssProp.containsKey(Constants.CUSTOM_DOCUMENT_PROCESSOR)) {
        	this.customDocumentProcessor =  Boolean.parseBoolean(this.smssProp.getProperty(Constants.CUSTOM_DOCUMENT_PROCESSOR));
        }
        if (this.smssProp.containsKey(Constants.CUSTOM_DOCUMENT_PROCESSOR_FUNCTION_ID)) {
        	this.customDocumentProcessorFunctionID = this.smssProp.getProperty(Constants.CUSTOM_DOCUMENT_PROCESSOR_FUNCTION_ID);
        }
        
		// highest directory (first layer inside vector db base folder)
		String engineDir = EngineUtility.getSpecificEngineAssetsFolder(IEngine.CATALOG_TYPE.VECTOR, this.engineId, this.engineName);
		this.pyDirectoryBasePath = new File(Utility.normalizePath(engineDir + FILE_SEPARATOR + "py" + FILE_SEPARATOR));
		
		// second layer - This holds all the different "tables". The reason we want this is to easily and quickly grab the sub folders
		this.schemaFolder = new File(engineDir, "schema");
		if(!this.schemaFolder.exists()) {
			this.schemaFolder.mkdirs();
		}
		
		// third layer - All the separate tables,classes, or searchers that can be added to this db
		this.indexClasses = new ArrayList<>();
        for (File file : this.schemaFolder.listFiles()) {
            if (file.isDirectory() && !file.getName().equals("temp")) {
            	this.indexClasses.add(file.getName());
            }
        }
	}
	
	protected abstract String getDefaultDistanceMethod();
	
	@Override
	public List<FileEmbeddingStatus> addDocument(List<String> filePaths, Map<String, Object> parameters) throws Exception {
		List<FileEmbeddingStatus> resultList = new ArrayList<>();
		if (!modelPropsLoaded) {
			verifyModelProps();
		}
		
		checkSocketStatus();
		
		try {
			this.removeDocument(filePaths, parameters);
		} catch(Exception ignore) {
			// we are only removing just in case
			// if something doesn't exist, just ignore the exception
		}
		String indexClass = this.defaultIndexClass;
		if (parameters.containsKey("indexClass")) {
			indexClass = (String) parameters.get("indexClass");
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
		
		String chunkingMethod = this.defaultChunkingMethod;
		if (parameters.containsKey(VectorDatabaseParamOptionsEnum.CHUNKING_METHOD.getKey())) {
			chunkingMethod = (String) parameters.get(VectorDatabaseParamOptionsEnum.CHUNKING_METHOD.getKey());
		}
        
		Insight insight = getInsight(parameters.get(Constants.INSIGHT));
		if (insight == null) {
			throw new IllegalArgumentException("Insight must be provided to run Model Engine Encoder");
		}
		
		File indexFilesDir = new File(this.schemaFolder + FILE_SEPARATOR + indexClass, INDEXED_FOLDER_NAME);
		
		// store the actual files we are extracting from
		// since we move this into the vector folder
		// we need to delete them if they fail
		// TODO: potentially look at loading these from insight and only pushing to the 
		// vector db catalog on success
		Set<File> fileToExtractFrom = new HashSet<File>();
		try {
			// first we need to extract the text from the document
			// TODO change this to json so we never have an encoding issue

			File indexDirectory = new File(this.schemaFolder, indexClass);
			File documentDir = new File(indexDirectory, DOCUMENTS_FOLDER_NAME);
			if(!documentDir.exists()) {
				documentDir.mkdirs();
			}

			if (!indexFilesDir.exists()) {
				indexFilesDir.mkdirs();
			}
			if (!this.indexClasses.contains(indexClass)) {
				addIndexClass(indexClass);
			}

			List<File> extractedFiles = new ArrayList<>();
			List<String> filesToCopyToCloud = new ArrayList<>(); // create a list to store all the net new files so we can push them to the cloud
			String chunkingStrategy = PyUtils.determineStringType(parameters.getOrDefault("chunkingStrategy", "ALL"));

			// move the documents from insight into documents folder
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
					FileUtils.copyFileToDirectory(fileInInsightFolder, documentDir, true);
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
				File extractedFile = new File(indexFilesDir.getAbsolutePath() + FILE_SEPARATOR + documentName + ".csv");
				String extractedFileName = extractedFile.getAbsolutePath().replace("\\", FILE_SEPARATOR);
				try {
					if (extractedFile.exists()) {
						FileUtils.forceDelete(extractedFile);
					}
					String docLower = document.getName().toLowerCase();
					
					if(docLower.endsWith(".csv")) {
						classLogger.info("You are attempting to load in a structured table for " + documentName + ". Hopefully the structure is the right format we expect...");
						// validate the file is a proper csv
						boolean validCsv = true;
						try {
							validCsv = VectorDatabaseCSVTable.validateCSVTable(document);
						} catch(Exception e) {
							classLogger.error(Constants.STACKTRACE, e);
							validCsv = false;
						}
						
						if(!validCsv) {
							StringBuilder headerBuilder = new StringBuilder();
							headerBuilder.append("'").append(VectorDatabaseCSVTable.SOURCE).append("', ")
								.append("'").append(VectorDatabaseCSVTable.SOURCE).append("', ")
								.append("'").append(VectorDatabaseCSVTable.MODALITY).append("', ")
								.append("'").append(VectorDatabaseCSVTable.DIVIDER).append("', ")
								.append("'").append(VectorDatabaseCSVTable.PART).append("', ")
								.append("'").append(VectorDatabaseCSVTable.TOKENS).append("', ")
								.append("'").append(VectorDatabaseCSVTable.CONTENT).append("'")
								;
							throw new IllegalArgumentException("The CSV must be the proper format with the following headers: " + headerBuilder.toString());
						}
						FileUtils.copyFileToDirectory(document, indexFilesDir);
					} else {
						classLogger.info("Extracting text from document " + documentName);
						boolean processed = false;
						int rowsCreated = -1;
						if (extractionMethod.equals("fitz") && document.getName().toLowerCase().endsWith(".pdf")) {
							StringBuilder extractTextFromDocScript = new StringBuilder();
							extractTextFromDocScript.append("vector_database.extract_text(source_file_name = '")
								.append(document.getAbsolutePath().replace("\\", FILE_SEPARATOR))
								.append("', target_folder = '")
								.append(this.schemaFolder.getAbsolutePath().replace("\\", FILE_SEPARATOR) 
										+ FILE_SEPARATOR + indexClass + FILE_SEPARATOR + "extraction_files")
								.append("', output_file_name = '")
								.append(extractedFileName)
								.append("')");
							setVectorFolderPermissions();
							Number rows = (Number) pyTranslator.runDirectPy(extractTextFromDocScript.toString());
							rowsCreated = rows.intValue();
							processed = true;
						} else if(this.customDocumentProcessor) {
							if(this.customDocumentProcessorFunctionID == null || this.customDocumentProcessorFunctionID.isEmpty()) {
								throw new IllegalArgumentException("Must define custom document processing function engine id in the SMSS");
							}
							IFunctionEngine functionEngine = Utility.getFunctionEngine(this.customDocumentProcessorFunctionID);
							if(!(functionEngine instanceof ICustomEmbeddingsFunctionEngine)) {
								throw new IllegalArgumentException("Vector Database owner has incorrectly setup a custom embeddings function that is not an ICustomEmbeddingsFunctionEngine");
							}
							ICustomEmbeddingsFunctionEngine customEmbeddings = (ICustomEmbeddingsFunctionEngine) functionEngine;
							if(customEmbeddings.canProcessDocument(document)) {
								rowsCreated = customEmbeddings.processDocument(extractedFile.getAbsolutePath(), document, parameters);
								processed = true;
							}
						} 
						
						// default processing if haven't processed with above logic
						if(!processed) {
							rowsCreated = VectorDatabaseUtils.convertFilesToCSV(extractedFile.getAbsolutePath(), document);
						}

						// check to see if the file data was extracted
						if (rowsCreated < 1) {
							// no text was extracted so delete the file
							FileUtils.forceDelete(extractedFile); // delete the csv
							FileUtils.forceDelete(document); // delete the input file e.g pdf
							continue;
						}                    
						setVectorFolderPermissions();
						classLogger.info("Creating chunks from extracted text for " + documentName);

						StringBuilder splitTextCommand = new StringBuilder();
						splitTextCommand.append("vector_database.split_text(csv_file_location = '")
							.append(extractedFileName)
							.append("', chunk_unit = '")
							.append(chunkUnit)
							.append("', chunk_size = ")
							.append(chunkMaxTokenLength)
							.append(", chunk_overlap = ")
							.append(tokenOverlapBetweenChunks)
							.append(", chunking_strategy = ")
							.append(chunkingStrategy)
							.append(", chunking_method = '")
							.append(chunkingMethod)
							.append("', cfg_tokenizer = cfg_tokenizer)");
						
						pyTranslator.runScript(splitTextCommand.toString());
					}

					// add it to the list of files that need to be pushed to the cloud in a new thread
					filesToCopyToCloud.add(document.getAbsolutePath());
					extractedFiles.add(extractedFile);
				} catch (IOException e) {
					classLogger.error(Constants.STACKTRACE, e);
					throw new IllegalArgumentException("Unable to remove old or create new text extraction file for " + documentName);
				}
			}
			
			if(extractedFiles.size() == 0) {
				StringBuilder fileNamesAttemptedUpload = new StringBuilder("[");
				boolean first = true;
				for (File document : fileToExtractFrom) {
					if(!first) {
						fileNamesAttemptedUpload.append(",");
					}
					fileNamesAttemptedUpload.append(FilenameUtils.getName(document.getName()));
				}
				fileNamesAttemptedUpload.append("]");
				throw new IllegalArgumentException("Unable to extract any text from " + fileNamesAttemptedUpload);
			}
			
			resultList = addEmbeddingFiles(extractedFiles, insight, parameters);
			
			if (ClusterUtil.IS_CLUSTER) {
				// push the actual documents over to the cloud
				Thread copyFilesToCloudThread = new Thread(new CopyFilesToEngineRunner(this.engineId, this.getCatalogType(), filesToCopyToCloud.stream().toArray(String[]::new)));
				copyFilesToCloudThread.start();
			}
		} catch(Exception e) {
			classLogger.error(Constants.STACKTRACE, e);
			// delete files moved into vector db documents folder
			for (File document : fileToExtractFrom) {
				document.delete();
			}
			throw e;
		} finally {
			cleanUpAddDocument(indexFilesDir);
		}
		return resultList;
	}
	
	protected void addIndexClass(String indexClass) {
		this.indexClasses.add(indexClass);
	}
	
	protected void cleanUpAddDocument(File file) {
		try {
			FileUtils.forceDelete(file);
		} catch (IOException e) {
			classLogger.error(Constants.STACKTRACE, e);
		}
	}
	
	
	/**
	 * 
	 * @param indexClass
	 * @return
	 */
	@Override
	public String getIndexFilesPath(String indexClass) {
		if(indexClass == null || (indexClass=indexClass.trim()).isEmpty()) {
			indexClass = this.defaultIndexClass;
		}
		if (!this.indexClasses.contains(indexClass)) {
			throw new IllegalArgumentException("Unable to retieve document csv from a directory that does not exist");
		}
		return Utility.normalizePath(this.schemaFolder.getAbsolutePath() + FILE_SEPARATOR + indexClass + FILE_SEPARATOR + INDEXED_FOLDER_NAME);
	}
	
	/**
	 * 
	 * @param indexClass
	 * @return
	 */
	@Override
	public String getDocumentsFilesPath(String indexClass) {
		if(indexClass == null || (indexClass=indexClass.trim()).isEmpty()) {
			indexClass = this.defaultIndexClass;
		}
		if (!this.indexClasses.contains(indexClass)) {
			throw new IllegalArgumentException("Unable to retieve document csv from a directory that does not exist");
		}
		return Utility.normalizePath(this.schemaFolder.getAbsolutePath() + FILE_SEPARATOR + indexClass + FILE_SEPARATOR + DOCUMENTS_FOLDER_NAME);
	}
	
	@Override
	public List<FileEmbeddingStatus> addEmbeddings(List<String> vectorCsvFiles, Insight insight, Map<String, Object> parameters) throws Exception {
		List<FileEmbeddingStatus> fileStatusList = new ArrayList<>();
		for(String vectorCsvFile : vectorCsvFiles) {
			VectorDatabaseCSVTable vectorCsvTable = VectorDatabaseCSVTable.initCSVTable(new File(vectorCsvFile));
			fileStatusList = addEmbeddings(vectorCsvTable, insight, parameters);
		}
		
		return fileStatusList;
	}
	
	@Override
	public List<FileEmbeddingStatus> addEmbeddings(String vectorCsvFile, Insight insight, Map<String, Object> parameters) throws Exception {
		VectorDatabaseCSVTable vectorCsvTable = VectorDatabaseCSVTable.initCSVTable(new File(vectorCsvFile));
		return addEmbeddings(vectorCsvTable, insight, parameters);
	}
	
	@Override
	public List<FileEmbeddingStatus> addEmbeddingFiles(List<File> vectorCsvFiles, Insight insight, Map<String, Object> parameters) throws Exception {
		List<FileEmbeddingStatus> fileStatusList = new ArrayList<>();
		for (File vectorCsvFile : vectorCsvFiles) {
			try {
				VectorDatabaseCSVTable vectorCsvTable = VectorDatabaseCSVTable.initCSVTable(vectorCsvFile);
				List<FileEmbeddingStatus> resultList = addEmbeddings(vectorCsvTable, insight, parameters);
	            fileStatusList.addAll(resultList);
			} catch (Exception e) {
				// File failed completely
				FileEmbeddingStatus failedStatus = new FileEmbeddingStatus(vectorCsvFile.getName(), "FAILED", 0, 0, 0);
	            fileStatusList.add(failedStatus);
			}
		}
		return fileStatusList;
	}
	
	@Override
	public List<FileEmbeddingStatus> addEmbeddingFile(File vectorCsvFile, Insight insight, Map<String, Object> parameters) throws Exception {
		VectorDatabaseCSVTable vectorCsvTable = VectorDatabaseCSVTable.initCSVTable(vectorCsvFile);
		return addEmbeddings(vectorCsvTable, insight, parameters);
	}
	
	@Override
	public void addEmbedding(List<? extends Number> embedding, String source, String modality, String divider,
			String part, int tokens, String content, Map<String, Object> additionalMetadata) throws Exception {
		// TODO Auto-generated method stub
	}
	
	@Override
	public void addMetadata(VectorDatabaseMetadataCSVTable vectorCsvTable) {
		// TODO Auto-generated method stub
		
	}

	/**
	 * This is an abstract method for the implementation class such that tracking occurs
	 * 
	 * @param insight
	 * @param searchStatement
	 * @param limit
	 * @param parameters
	 * @return
	 */
	protected abstract List<Map<String, Object>> nearestNeighborCall(Insight insight, String searchStatement, Number limit, Map <String, Object> parameters);
	
	@Override
	public List<Map<String, Object>> nearestNeighbor(Insight insight, String searchStatement, Number limit, Map <String, Object> parameters) {
		if(parameters == null) {
			parameters = new HashMap<String, Object>();
		}

		ZonedDateTime inputTime = ZonedDateTime.now();
		List<Map<String, Object>> vectorSearchResponse = nearestNeighborCall(insight, searchStatement, limit, parameters);
		ZonedDateTime outputTime = ZonedDateTime.now();

		if (inferenceLogsEnbaled && this.keepInputOutput) {
			Gson gson = new GsonBuilder().disableHtmlEscaping().create();
			Thread inferenceRecorder = new Thread(new ModelEngineInferenceLogsWorker (
					/*messageId*/UUID.randomUUID().toString(), 
					/*messageMethod*/"nearestNeighbor", 
					/*engine*/this, 
					/*insightId*/insight.getInsightId(),
					/*projectContextId*/insight.getContextProjectId(),
					/*projectId*/insight.getProjectId(),
					/*user*/insight.getUser(),
					/*sessionId*/ThreadStore.getSessionId(),
					/*roomId*/ThreadStore.getInsightId(),
					/*context*/null, 
					/*prompt*/searchStatement,
					/*fullPrompt*/null,
					/*promptTokens*/null,
					/*inputTime*/inputTime, 
					/*response*/gson.toJson(vectorSearchResponse),
					/*responseTokens*/null,
					/*outputTime*/outputTime
					));
			inferenceRecorder.start();
		}

		return vectorSearchResponse;
	}
	
	/**
	 * 
	 */
	protected void verifyModelProps() {
		// This could get moved depending on other vector db needs
		// This is to get the Model Name and Max Token for an encoder -- we need this to verify chunks aren't getting truncated
		this.embedderEngineId = this.smssProp.getProperty(Constants.EMBEDDER_ENGINE_ID);
		if (this.embedderEngineId == null || (this.embedderEngineId=this.embedderEngineId.trim()).isEmpty()) {
			
			// check legacy key....
			this.embedderEngineId = this.smssProp.getProperty("ENCODER_ID");
			if (this.embedderEngineId == null || (this.embedderEngineId=this.embedderEngineId.trim()).isEmpty()) {
				throw new IllegalArgumentException("Must define the embedder engine id for this vector database using " + Constants.EMBEDDER_ENGINE_ID);
			}
			
			this.smssProp.put(Constants.EMBEDDER_ENGINE_ID, embedderEngineId);
		}
		
		IModelEngine modelEngine = Utility.getModel(embedderEngineId);
		if (modelEngine == null) {
			throw new NullPointerException("Could not find the defined embedder engine id for this vector database with value = " + this.embedderEngineId);
		}
		
		Properties modelProperties = modelEngine.getSmssProp();
		if (modelProperties.isEmpty() || !modelProperties.containsKey(Constants.MODEL)) {
			throw new IllegalArgumentException("Embedder engine exists but does not contain key " + Constants.MODEL);
		}
		
		this.smssProp.put(Constants.MODEL, modelProperties.getProperty(Constants.MODEL));
		this.smssProp.put(IModelEngine.MODEL_TYPE, modelProperties.getProperty(IModelEngine.MODEL_TYPE));
		if (!modelProperties.containsKey(Constants.MAX_TOKENS)) {
			this.smssProp.put(Constants.MAX_TOKENS, "None");
		} else {
			String modelMaxTokens = modelProperties.getProperty(Constants.MAX_TOKENS);
			if(modelMaxTokens == null || (modelMaxTokens=modelMaxTokens.trim()).isEmpty()) {
				this.smssProp.put(Constants.MAX_TOKENS, "None");
			} else {
				this.smssProp.put(Constants.MAX_TOKENS, modelMaxTokens);
			}
		}

		// model engine responsible for creating keywords
		this.keywordGeneratorEngineId = this.smssProp.getProperty(Constants.KEYWORD_ENGINE_ID);
		if (this.keywordGeneratorEngineId != null && !(this.keywordGeneratorEngineId=this.keywordGeneratorEngineId.trim()).isEmpty()) {
			// pull the model smss if needed
			Utility.getModel(this.keywordGeneratorEngineId);
			this.smssProp.put(Constants.KEYWORD_ENGINE_ID, this.keywordGeneratorEngineId);
		} else {
			// add it to the smss prop so the string substitution does not fail
			this.smssProp.put(Constants.KEYWORD_ENGINE_ID, "");
		}
		
		for (Object smssKey : this.smssProp.keySet()) {
			String key = smssKey.toString();
			this.vars.put(key, this.smssProp.getProperty(key));
		}
		
		this.modelPropsLoaded = true;
	}
	
	@Override
	public boolean userCanAccessEmbeddingModels(User user) {
		if (!modelPropsLoaded) {
			verifyModelProps();
		}
		
		if(this.embedderEngineId != null) {
			if(!SecurityEngineUtils.userCanViewEngine(user, this.embedderEngineId)) {
				throw new IllegalArgumentException("Embeddings model " + this.embedderEngineId + " does not exist or user does not have access to this model");
			}
		}
		
		if(this.keywordGeneratorEngineId != null && !this.keywordGeneratorEngineId.isEmpty()) {
			if(!SecurityEngineUtils.userCanViewEngine(user, this.keywordGeneratorEngineId)) {
				throw new IllegalArgumentException("Keyword model " + this.keywordGeneratorEngineId + " does not exist or user does not have access to this model");
			}
		}
		
		return true;
	}

	/**
	 * 
	 */
	protected void checkSocketStatus() {
		if(this.cpw == null || this.cpw.getSocketClient() == null || !this.cpw.getSocketClient().isConnected()) {
			this.startServer(-1);
		}
	}
	
	/**
	 * 
	 * @param input
	 * @return
	 */
	protected String fillVars(String input) {
		StringSubstitutor sub = new StringSubstitutor(vars);
		String resolvedString = sub.replace(input);
		return resolvedString;
	}
	
	/**
	 * This method is meant to be overriden so that we dont need to copy/paste the startServer code for every implementation
	 * @return
	 */
	protected String[] getServerStartCommands() {
		return (AbstractVectorDatabaseEngine.TOKENIZER_INIT_SCRIPT).split(PyUtils.PY_COMMAND_SEPARATOR);
	}
	
	/**
	 * 
	 * @param port
	 */
	protected synchronized void startServer(int port) {
		// already created by another thread
		if(this.cpw != null && this.cpw.getSocketClient() != null && this.cpw.getSocketClient().isConnected()) {
			return;
		}
		if (!modelPropsLoaded) {
			verifyModelProps();
		}
		if(!this.pyDirectoryBasePath.exists()) {
			this.pyDirectoryBasePath.mkdirs();
		}

		// check if we have already created a process wrapper
		ClientProcessWrapper cpwToInit = new ClientProcessWrapper();
		if(this.cpw != null) {
			this.cpw.shutdown(false);
		}
		
		String timeout = "30";
		if(this.smssProp.containsKey(Constants.IDLE_TIMEOUT)) {
			timeout = this.smssProp.getProperty(Constants.IDLE_TIMEOUT);
		}
	
		boolean debug = false;
		
		// pull the relevant values from the smss
		String forcePort = this.smssProp.getProperty(Settings.FORCE_PORT);
		String customClassPath = this.smssProp.getProperty("TCP_WORKER_CP");
		String loggerLevel = this.smssProp.getProperty(Settings.LOGGER_LEVEL, "WARNING");
		String venvEngineId = this.smssProp.getProperty(Constants.VIRTUAL_ENV_ENGINE, null);
		String venvPath = venvEngineId != null ? Utility.getVenvEngine(venvEngineId).pathToExecutable() : null;
		
		if(port < 0) {
			// port has not been forced
			if(forcePort != null && !(forcePort=forcePort.trim()).isEmpty()) {
				try {
					port = Integer.parseInt(forcePort);
					debug = true;
				} catch(NumberFormatException e) {
					// ignore
					classLogger.warn("Vector Database " + this.engineName + " has an invalid FORCE_PORT value");
				}
			}
		}
		
		//if we have a python specific user, make sure that user can access the schema folder
		setVectorFolderPermissions();
		
		String serverDirectory = this.pyDirectoryBasePath.getAbsolutePath();
		boolean nativePyServer = true; // it has to be -- don't change this unless you can send engine calls from python
		try {
			cpwToInit.createProcessAndClient(nativePyServer, null, port, venvPath, serverDirectory, customClassPath, debug, timeout, loggerLevel);
		} catch (Exception e) {
			classLogger.error(Constants.STACKTRACE, e);
			throw new IllegalArgumentException("Unable to connect to server for vector databse.");
		}

		// create the py translator
		Insight processInsight = new Insight();
		InsightStore.getInstance().put(processInsight);
		this.pyTranslator = new PyTranslator(cpwToInit.getSocketClient(), processInsight);
		
		try {
			// this is engine specific... or can be
			String[] commands = getServerStartCommands();
			// replace the vars
			StringSubstitutor substitutor = new StringSubstitutor(this.vars);
			for(int commandIndex = 0; commandIndex < commands.length;commandIndex++) {
				String resolvedString = substitutor.replace(commands[commandIndex]);
				commands[commandIndex] = resolvedString;
			}
			
			// for debugging...
			classLogger.info("Initializing " + SmssUtilities.getUniqueName(this.engineName, this.engineId) 
								+ " python process with commands >>> " + String.join("\n", commands));
			
			this.pyTranslator.runEmptyPy(commands);
			
			// finally set the cpw in the class
			this.cpw = cpwToInit;
		} catch(Exception e) {
			// set the model props to false
			// incase those values were incorrect
			modelPropsLoaded = false;
			classLogger.error(Constants.STACKTRACE, e);
			if(cpwToInit != null) {
				classLogger.warn("Able to start the python process for the vector database " 
						+ SmssUtilities.getUniqueName(this.engineName, this.engineId) 
						+ " but the start script failed.");
				cpwToInit.shutdown(false);
			}
			throw e;
		}
	}
	
	/**
	 * 
	 * @param insightObj
	 * @return
	 */
	protected Insight getInsight(Object insightObj) {
		if (insightObj instanceof String) {
			return InsightStore.getInstance().get((String) insightObj);
		} else {
			return (Insight) insightObj;
		}
	}
	
	/**
	 * 
	 * @param content
	 * @param insight
	 * @return
	 */
	protected Float[] getEmbeddingsFloat(String content, Insight insight) {
		if (!modelPropsLoaded) {
			verifyModelProps();
		}
		IModelEngine embeddingsEngine = Utility.getModel(this.embedderEngineId);
		List <Double> embeddingsResponse = embeddingsEngine.embeddings(Arrays.asList(new String[] {content}), getInsight(insight), null).getResponse().get(0);
		Float [] retFloat = new Float[embeddingsResponse.size()];
		for(int vecIndex = 0; vecIndex < retFloat.length; vecIndex++) {
			retFloat[vecIndex] = (Float)embeddingsResponse.get(vecIndex).floatValue();
		}
		
		return retFloat;
	}
	
	/**
	 * 
	 * @param content
	 * @param insight
	 * @return
	 */
	protected List<Double> getEmbeddingsDouble(String content, Insight insight) {
		if (!modelPropsLoaded) {
			verifyModelProps();
		}
		IModelEngine embeddingsEngine = Utility.getModel(this.embedderEngineId);
		List<Double> embeddingsResponse = embeddingsEngine.embeddings(Arrays.asList(new String[] {content}), getInsight(insight), null).getResponse().get(0);
		return embeddingsResponse;
	}
	
	/**
	 * 
	 * @return
	 */
	public boolean keepInputOutput() {
		return this.keepInputOutput;
	}

	@Override
	public IEngine.CATALOG_TYPE getCatalogType() {
		return IEngine.CATALOG_TYPE.VECTOR;
	}

	@Override
	public String getCatalogSubType(Properties smssProp) {
		return this.getVectorDatabaseType().toString();
	}
	
	@Override
	public void close() throws IOException {
		this.modelPropsLoaded = false;
		if(this.cpw != null) {
			this.cpw.shutdown(true);
		}
	}
	
	@Override
	public boolean holdsFileLocks() {
		return false;
	}
	
	/**
	 * 
	 */
	private void setVectorFolderPermissions() {
		//if we have a python specific user, make sure that user can access the schema folder
		String pythonUser = Utility.getDIHelperProperty(Settings.PY_SERVER_USER);
		if (pythonUser != null && !pythonUser.trim().isEmpty()) {
			try {
				Utility.setOwnerAndGroupPermissionsRecursively(this.schemaFolder);
			} catch (IOException e) {
				classLogger.error(Constants.STACKTRACE, e);
			} catch (InterruptedException e) {
				classLogger.error(Constants.STACKTRACE, e);
			}
		}
	}
	
}