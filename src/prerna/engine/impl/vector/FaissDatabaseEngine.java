package prerna.engine.impl.vector;

import java.io.File;
import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.FilenameUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.cluster.util.ClusterUtil;
import prerna.cluster.util.CopyFilesToEngineRunner;
import prerna.cluster.util.DeleteFilesFromEngineRunner;
import prerna.ds.py.PyUtils;
import prerna.ds.py.TCPPyTranslator;
import prerna.engine.api.IEngine;
import prerna.engine.api.VectorDatabaseTypeEnum;
import prerna.om.ClientProcessWrapper;
import prerna.om.Insight;
import prerna.query.querystruct.filters.AndQueryFilter;
import prerna.query.querystruct.filters.BetweenQueryFilter;
import prerna.query.querystruct.filters.IQueryFilter;
import prerna.query.querystruct.filters.OrQueryFilter;
import prerna.query.querystruct.filters.SimpleQueryFilter;
import prerna.query.querystruct.filters.SimpleQueryFilter.FILTER_TYPE;
import prerna.query.querystruct.selectors.IQuerySelector;
import prerna.query.querystruct.selectors.QueryColumnSelector;
import prerna.query.querystruct.selectors.QueryConstantSelector;
import prerna.reactor.qs.SubQueryExpression;
import prerna.reactor.vector.VectorDatabaseParamOptionsEnum;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Constants;
import prerna.util.EngineUtility;
import prerna.util.Settings;
import prerna.util.Utility;
import prerna.util.sql.AbstractSqlQueryUtil;

public class FaissDatabaseEngine extends AbstractVectorDatabaseEngine {

	private static final Logger classLogger = LogManager.getLogger(FaissDatabaseEngine.class);
	
	public static final String VECTOR_SEARCHER_NAME = "VECTOR_SEARCHER_NAME";
	
	private static final String TOKENIZER_INIT_SCRIPT = "from genai_client import get_tokenizer;"
			+ "cfg_tokenizer = get_tokenizer(tokenizer_name = '${MODEL}', max_tokens = ${MAX_TOKENS}, tokenizer_type = '${MODEL_TYPE}');";
	private static final String FAISS_INIT_SCRIPT = "import vector_database;"
			+ "${VECTOR_SEARCHER_NAME} = vector_database.FAISSDatabase(embedder_engine_id = '${EMBEDDER_ENGINE_ID}', tokenizer = cfg_tokenizer, keyword_engine_id = '${KEYWORD_ENGINE_ID}', distance_method = '${DISTANCE_METHOD}')";
	
	protected String vectorDatabaseSearcher = null;
	
	private List<String> indexClasses;
	private HashMap<String, Boolean> indexClassHasDatasetLoaded = new HashMap<String, Boolean>();
	
	@Override
	public void open(Properties smssProp) throws Exception {
		super.open(smssProp);
		
		// highest directory (first layer inside vector db base folder)
		String engineDir = EngineUtility.getSpecificEngineBaseFolder(IEngine.CATALOG_TYPE.VECTOR, this.engineId, this.engineName);
		this.pyDirectoryBasePath = new File(Utility.normalizePath(engineDir + DIR_SEPARATOR + "py" + DIR_SEPARATOR));
		
		// second layer - This holds all the different "tables". The reason we want this is to easily and quickly grab the sub folders
		this.schemaFolder = new File(engineDir, "schema");
		if(!this.schemaFolder.exists()) {
			this.schemaFolder.mkdirs();
		}
		this.smssProp.put(Constants.WORKING_DIR, this.schemaFolder.getAbsolutePath());
		
		// third layer - All the separate tables,classes, or searchers that can be added to this db
		this.indexClasses = new ArrayList<>();
        for (File file : this.schemaFolder.listFiles()) {
            if (file.isDirectory() && !file.getName().equals("temp")) {
            	this.indexClasses.add(file.getName());
            }
        }
		
		this.vectorDatabaseSearcher = Utility.getRandomString(6);
		this.smssProp.put(VECTOR_SEARCHER_NAME, this.vectorDatabaseSearcher);
	}

	@Override
	protected synchronized void startServer(int port) {
		// already created by another thread
		if(this.cpw != null && this.cpw.getSocketClient() != null && this.cpw.getSocketClient().isConnected()) {
			return;
		}
	
		if (!modelPropsLoaded) {
			verifyModelProps();
		}
		
		// spin the server
		// start the client
		// get the startup command and parameters - at some point we need a better way than the command
		
		// execute all the basic commands		
		
		// break the commands seperated by ;
		String [] commands = (TOKENIZER_INIT_SCRIPT+FAISS_INIT_SCRIPT).split(PyUtils.PY_COMMAND_SEPARATOR);
		
		// need to iterate through and potential spin up tables themselves
		if (this.indexClasses.size() > 0) {
	        ArrayList<String> modifiedCommands = new ArrayList<>(Arrays.asList(commands));
			for (String indexClass : this.indexClasses) {
				File fileToCheck = new File(this.schemaFolder.getAbsolutePath() + DIR_SEPARATOR + indexClass, "dataset.pkl");
				modifiedCommands.add("${"+VECTOR_SEARCHER_NAME+"}.create_searcher(searcher_name = '"+indexClass+"', base_path = '"+fileToCheck.getParent().replace(FILE_SEPARATOR, DIR_SEPARATOR) + DIR_SEPARATOR +"')");
				if (fileToCheck.exists()) {
			        modifiedCommands.add("${"+VECTOR_SEARCHER_NAME+"}.searchers['"+indexClass+"'].load_dataset('"+fileToCheck.getParent().replace(FILE_SEPARATOR, DIR_SEPARATOR) + DIR_SEPARATOR +"' + 'dataset.pkl')");
			        modifiedCommands.add("${"+VECTOR_SEARCHER_NAME+"}.searchers['"+indexClass+"'].load_encoded_vectors('"+fileToCheck.getParent().replace(FILE_SEPARATOR, DIR_SEPARATOR) + DIR_SEPARATOR +"' + 'vectors.pkl')");
		        }
			}
            commands = modifiedCommands.stream().toArray(String[]::new);
		}

		// replace the Vars
		for(int commandIndex = 0; commandIndex < commands.length;commandIndex++) {
			commands[commandIndex] = fillVars(commands[commandIndex]);
		}
		
		if(!this.pyDirectoryBasePath.exists()) {
			this.pyDirectoryBasePath.mkdirs();
		}
		
		// check if we have already created a process wrapper
		if(this.cpw == null) {
			this.cpw = new ClientProcessWrapper();
		}
		
		String timeout = "30";
		if(this.smssProp.containsKey(Constants.IDLE_TIMEOUT)) {
			timeout = this.smssProp.getProperty(Constants.IDLE_TIMEOUT);
		}
		
		if(this.cpw.getSocketClient() == null) {
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
						classLogger.warn("Faiss Database " + this.engineName + " has an invalid FORCE_PORT value");
					}
				}
			}
			
			String serverDirectory = this.pyDirectoryBasePath.getAbsolutePath();
			boolean nativePyServer = true; // it has to be -- don't change this unless you can send engine calls from python
			try {
				this.cpw.createProcessAndClient(nativePyServer, null, port, venvPath, serverDirectory, customClassPath, debug, timeout, loggerLevel);
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
				throw new IllegalArgumentException("Failed to start TCP Server for Faiss Database = " + this.engineName);
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
		
		classLogger.info("Initializing FAISS db with the following py commands >>>" + intitPyCommands.toString());
		pyt.runEmptyPy(commands);
		
		// check if the index class was able to load in its documents
		
		if (this.indexClasses.size() > 0) {
			for (String indexClass : this.indexClasses) {
				StringBuilder checkForEmptyDatabase = new StringBuilder();
				checkForEmptyDatabase.append(this.vectorDatabaseSearcher)
									 .append(".searchers['")
									 .append(indexClass)
									 .append("']")
									 .append(".datasetsLoaded()");
				boolean datasetsLoaded = (boolean) pyt.runScript(checkForEmptyDatabase.toString());
				this.indexClassHasDatasetLoaded.put(indexClass, datasetsLoaded);
			}
		}
	}
		
	@Override
	public void addDocument(List<String> filePaths, Map <String, Object> parameters) {
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

		// File temporaryFileDirectory = (File) parameters.get("temporaryFileDirectory");
		
		
		// first we need to extract the text from the document
		// TODO change this to json so we never have an encoding issue
		checkSocketStatus();
		
		File indexDirectory = new File(this.schemaFolder, indexClass);
		File documentDir = new File(indexDirectory, "documents");
		if(!documentDir.exists()) {
			documentDir.mkdirs();
		}
		
		boolean filesAppoved = VectorDatabaseUtils.verifyFileTypes(filePaths, new ArrayList<>(Arrays.asList(documentDir.list())));
		if (!filesAppoved) {
			throw new IllegalArgumentException("Currently unable to mix csv with non-csv file types.");
		}
		
		File tableIndexFolder = new File(this.schemaFolder + DIR_SEPARATOR + indexClass, "indexed_files");
		if (!tableIndexFolder.exists()) {
			tableIndexFolder.mkdirs();
		}
		if (!this.indexClasses.contains(indexClass)) {
			this.indexClasses.add(indexClass);
			this.pyt.runScript(this.vectorDatabaseSearcher + ".create_searcher(searcher_name = '"+indexClass+"', base_path = '"+tableIndexFolder.getParent().replace(FILE_SEPARATOR, DIR_SEPARATOR) + DIR_SEPARATOR +"')");
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
				extractedFiles.add(extractedFileName);
			} catch (IOException e) {
				classLogger.error(Constants.STACKTRACE, e);
				throw new IllegalArgumentException("Unable to remove old or create new text extraction file for " + documentName);
			}
		}
		
		// if we were able to extract files, begin embeddings process
		if (extractedFiles.size() > 0) {
			// create dataset
			StringBuilder addDocumentPyCommand = new StringBuilder();
			
			// get the relevant FAISS searcher object in python
			addDocumentPyCommand.append(vectorDatabaseSearcher)
								.append(".searchers['")
								.append(indexClass)
								.append("']");
			
			addDocumentPyCommand.append(".addDocumet(documentFileLocation = ['")
								.append(String.join("','", extractedFiles))
								.append("'], insight_id = '")
								.append(insight.getInsightId())
								.append("', columns_to_index = ")
								.append(columnsToIndex);
			
			if (parameters.containsKey(VectorDatabaseParamOptionsEnum.COLUMNS_TO_REMOVE.getKey())) {
				// add the columns based in the vector db query
				addDocumentPyCommand.append(", ")
						 			.append("columns_to_remove")
						 			.append(" = ")
						 			.append(PyUtils.determineStringType(
									 parameters.get(
											 VectorDatabaseParamOptionsEnum.COLUMNS_TO_REMOVE.getKey()
											 )
									 ));
			}
			
			if (parameters.containsKey(VectorDatabaseParamOptionsEnum.KEYWORD_SEARCH_PARAM.getKey())) {
				// add the columns based in the vector db query
				addDocumentPyCommand.append(", ")
						 			.append("keyword_search_params")
						 			.append(" = ")
						 			.append(PyUtils.determineStringType(
									 parameters.get(
											 VectorDatabaseParamOptionsEnum.KEYWORD_SEARCH_PARAM.getKey()
											 )
									 ));
			}
												
			addDocumentPyCommand.append(")");
			
			String script = addDocumentPyCommand.toString();
			
			classLogger.info("Running >>>" + script);
			Map<String, Object> pythonResponseAfterCreatingFiles = (Map<String, Object>) this.pyt.runScript(script, insight);

			if (ClusterUtil.IS_CLUSTER) {
				// and the newly created csvs
				filesToCopyToCloud.addAll(extractedFiles);
				// add all the embeddings files and the datasets
				filesToCopyToCloud.addAll((List<String>) pythonResponseAfterCreatingFiles.get("createdDocuments"));
				
				Thread copyFilesToCloudThread = new Thread(new CopyFilesToEngineRunner(engineId, this.getCatalogType(), filesToCopyToCloud.stream().toArray(String[]::new)));
				copyFilesToCloudThread.start();
			}
			
			// verify the index class loaded the dataset
			StringBuilder checkForEmptyDatabase = new StringBuilder();
			checkForEmptyDatabase.append(this.vectorDatabaseSearcher)
								 .append(".searchers['")
								 .append(indexClass)
								 .append("']")
								 .append(".datasetsLoaded()");
			boolean datasetsLoaded = (boolean) pyt.runScript(checkForEmptyDatabase.toString());
			this.indexClassHasDatasetLoaded.put(indexClass, datasetsLoaded);
		}
		
		// inform the user that some chunks are too large and they might loose semantic value
		// Map<String, List<Integer>> needToReturnForWarnings = (Map<String, List<Integer>>) pythonResponseAfterCreatingFiles.get("documentsWithLargerChunks");
	}

	@Override
	public void removeDocument(List<String> filePaths, Map <String, Object> parameters) {
		String indexClass = this.defaultIndexClass;
		if (parameters.containsKey(INDEX_CLASS)) {
			indexClass = (String) parameters.get(INDEX_CLASS);
		}
		
		if (!this.indexClasses.contains(indexClass)) {
			throw new IllegalArgumentException("Unable to remove documents from a directory that does not exist");
		}
		
		checkSocketStatus();
		
		List<String> filesToRemoveFromCloud = new ArrayList<String>();
		String indexedFilesPath = this.schemaFolder.getAbsolutePath() + DIR_SEPARATOR + indexClass + DIR_SEPARATOR + "indexed_files";
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
	        	File documentFile = new File(this.schemaFolder.getAbsolutePath() + DIR_SEPARATOR + indexClass + DIR_SEPARATOR + "documents", document);
				FileUtils.forceDelete(documentFile);
				filesToRemoveFromCloud.add(documentFile.getAbsolutePath());
			} catch (IOException e) {
				classLogger.error(Constants.STACKTRACE, e);
				throw new IllegalArgumentException("Unable to delete " + document + "from documents directory");
			}
		}
		
		// this would mean the indexClass is now empty, we should delete it
		File indexedFolder = new File(indexedFilesPath);
		if (indexedFolder.list().length == 0) {
			try {
				File indexClassDirectory = new File(indexedFolder.getParent());
				
				// remove the master dataset and vector files
				filesToRemoveFromCloud.add(new File(indexClassDirectory, "dataset.pkl").getAbsolutePath());
				filesToRemoveFromCloud.add(new File(indexClassDirectory, "vectors.pkl").getAbsolutePath());
				
				// delete the entire folder
				FileUtils.forceDelete(indexClassDirectory);
			} catch (IOException e) {
				classLogger.error(Constants.STACKTRACE, e);
				throw new IllegalArgumentException("Unable to delete remove the index class folder");
			}
			this.pyt.runScript(this.vectorDatabaseSearcher + ".delete_searcher(searcher_name = '"+indexClass+"')");
			this.indexClasses.remove(indexClass);
			this.indexClassHasDatasetLoaded.remove(indexClass);
		} else {
			// Regenerate the master "dataset.pkl" and "vectors.pkl" files
	        StringBuilder updateMasterFilesCommand = new StringBuilder();
	        updateMasterFilesCommand.append(this.vectorDatabaseSearcher)
	                                .append(".searchers['")
	                                .append(indexClass)
	                                .append("']")
	                                .append(".createMasterFiles(path_to_files = '")
	                                .append(indexDirectory.getParent().toString().replace(FILE_SEPARATOR, DIR_SEPARATOR))
	                                .append("')");

	        String script = updateMasterFilesCommand.toString();
	        classLogger.info("Running >>>" + script);
	        this.pyt.runScript(script);

	        // Verify index class loaded the dataset
	        StringBuilder checkForEmptyDatabase = new StringBuilder();
	        checkForEmptyDatabase.append(this.vectorDatabaseSearcher)
	                             .append(".searchers['")
	                             .append(indexClass)
	                             .append("']")
	                             .append(".datasetsLoaded()");
	        boolean datasetsLoaded = (boolean) pyt.runScript(checkForEmptyDatabase.toString());
	        this.indexClassHasDatasetLoaded.put(indexClass, datasetsLoaded);
		}
		
		if (ClusterUtil.IS_CLUSTER) {
			Thread deleteFilesFromCloudThread = new Thread(new DeleteFilesFromEngineRunner(engineId, this.getCatalogType(), filesToRemoveFromCloud.stream().toArray(String[]::new)));
			deleteFilesFromCloudThread.start();
		}
	}

	@SuppressWarnings("unchecked")
	@Override
	public List<Map<String, Object>> nearestNeighbor(String question, Number limit, Map <String, Object> parameters) {
		
		checkSocketStatus();
				
		Insight insight = getInsight(parameters.get(INSIGHT));
		if (insight == null) {
			throw new IllegalArgumentException("Insight must be provided to run Model Engine Encoder");
		}
		
		insight.getVarStore().put(LATEST_VECTOR_SEARCH_STATEMENT, new NounMetadata(question, PixelDataType.CONST_STRING));
		
		StringBuilder callMaker = new StringBuilder();
		
		String indexClass = this.defaultIndexClass;
		if (parameters.containsKey(INDEX_CLASS)) {
			Object indexClassObj = parameters.get(INDEX_CLASS);
			if (indexClassObj instanceof String) {
				indexClass = (String) indexClassObj;
				// make the python method
				callMaker.append(this.vectorDatabaseSearcher)
						 .append(".searchers['")
						 .append(indexClass)
						 .append("']")
						 .append(".nearestNeighbor(");
			} else if (indexClassObj instanceof Collection) {
				indexClass = PyUtils.determineStringType(indexClassObj);
				// make the python method
				callMaker.append(this.vectorDatabaseSearcher)
						 .append(".nearestNeighbor(")
						 .append("indexClasses = ")
						 .append(indexClass)
						 .append(", ");
			}
		} else {
			// make the python method
			callMaker.append(this.vectorDatabaseSearcher)
					 .append(".searchers['")
					 .append(indexClass)
					 .append("']")
					 .append(".nearestNeighbor(");
		}
		
		// make sure the database has docuemnts loaded / added
		if (!this.indexClassHasDatasetLoaded.containsKey(indexClass) || this.indexClassHasDatasetLoaded.get(indexClass) == false) {
			throw new IllegalArgumentException("There are no documents loaded in the index class of the vector database.");
		}
	
		// make the question arg
		callMaker.append("question=\"\"\"")
				 .append(question.replace("\"", "\\\""))
				 .append("\"\"\"");
		
		callMaker.append(", insight_id='")
				 .append(insight.getInsightId())
				 .append("'");
				
		String searchFilters = "None";
		if (parameters.containsKey("filters")) {
			// TODO modify so query can come from py world
			List<IQueryFilter> filters = (List<IQueryFilter>) parameters.remove("filters");
			searchFilters = addFilters(filters);
			// make the filter arg
			callMaker.append(", ")
					 .append("filter=\"\"\"")
					 .append(searchFilters)
					 .append("\"\"\"");
		}
		
		// make the limit, i.e. the number of responses we want
		callMaker.append(", ")
				 .append("results = ")
				 .append(limit);
		
		if (parameters.containsKey(VectorDatabaseParamOptionsEnum.COLUMNS_TO_RETURN.getKey())) {
			// add the columns based in the vector db query
			callMaker.append(", ")
					 .append("columns_to_return")
					 .append(" = ")
					 .append(PyUtils.determineStringType(
							 parameters.get(
									 VectorDatabaseParamOptionsEnum.COLUMNS_TO_RETURN.getKey()
									 )
							 ));
		}
		
		if (parameters.containsKey(VectorDatabaseParamOptionsEnum.RETURN_THRESHOLD.getKey())) {
			// add the return_threshold, it should be a long or double value
			Object thresholdValue =  parameters.get(VectorDatabaseParamOptionsEnum.RETURN_THRESHOLD.getKey());
			Double returnThreshold;
			
			if (thresholdValue instanceof Long) {
				Long y = (Long) thresholdValue;
				returnThreshold = y.doubleValue();
			} else if ((thresholdValue instanceof Double)){
				returnThreshold = (Double) thresholdValue;
			} else {
				throw new IllegalArgumentException("Please make sure the the return threshold is of type Double");
			}
			
			callMaker.append(", ")
					 .append("return_threshold = ")
					 .append(returnThreshold);
		}
		
		if (parameters.containsKey(VectorDatabaseParamOptionsEnum.ASCENDING.getKey())) {
			// This should be a True or False value
			String trueFalseString = (String) parameters.get(VectorDatabaseParamOptionsEnum.ASCENDING.getKey());
			String pythonTrueFalse = Character.toUpperCase(trueFalseString.charAt(0)) + trueFalseString.substring(1);
			
			callMaker.append(",")
					 .append("ascending = ")
					 .append(pythonTrueFalse);
		}
		
		// close the method
 		callMaker.append(")");
 		classLogger.info("Running >>>" + callMaker.toString());
		Object output = pyt.runScript(callMaker.toString(), insight);
		
		return (List<Map<String, Object>>) output;
	}
	
	public Map<String, String> removeCorruptedFiles(String indexClass){
		if (indexClass == null || indexClass.isEmpty()) {
			indexClass = this.defaultIndexClass;
		}
		
		File indexClassDirectory = new File(this.schemaFolder, indexClass);
		
		if (!indexClassDirectory.exists()) {
			throw new IllegalArgumentException("The FAISS Index Class called " + indexClass + " does not exist.");
		}
		
		StringBuilder executionScript = new StringBuilder();
		executionScript.append(vectorDatabaseSearcher)
					   .append(".searchers['")
					   .append(indexClass)
					   .append("']");
		
		executionScript.append(".removeCorruptedFiles(")
					   .append("path_to_files = '")
					   .append(indexClassDirectory.getAbsolutePath().replace(FILE_SEPARATOR, DIR_SEPARATOR))
					   .append("')");
		
		@SuppressWarnings("unchecked")
		Map<String, String> corruptedFilesToReason = (Map<String, String>) this.pyt.runScript(executionScript.toString());
		
		if (ClusterUtil.IS_CLUSTER) {
			Thread deleteFilesFromCloudThread = new Thread(new DeleteFilesFromEngineRunner(engineId, this.getCatalogType(), corruptedFilesToReason.keySet().stream().toArray(String[]::new)));
			deleteFilesFromCloudThread.start();
		}
		
		// verify the index class loaded the dataset
		StringBuilder checkForEmptyDatabase = new StringBuilder();
		checkForEmptyDatabase.append(this.vectorDatabaseSearcher)
							 .append(".searchers['")
							 .append(indexClass)
							 .append("']")
							 .append(".datasetsLoaded()");
		boolean datasetsLoaded = (boolean) pyt.runScript(checkForEmptyDatabase.toString());
		this.indexClassHasDatasetLoaded.put(indexClass, datasetsLoaded);
		
		return corruptedFilesToReason;
	}

	@Override
	public VectorDatabaseTypeEnum getVectorDatabaseType() {
		return VectorDatabaseTypeEnum.FAISS;
	}

	/**
	 * 
	 * @param filters
	 * @return
	 */
	private String addFilters(List<IQueryFilter> filters) {
		List<String> filterStatements = new ArrayList<>();
		
		for(IQueryFilter filter : filters) {
			StringBuilder filterSyntax = processFilter(filter);
			if(filterSyntax != null) {
				filterStatements.add(filterSyntax.toString());
			}
		}
		if (filterStatements.size() == 0) {
			throw new IllegalArgumentException("Unable to generate filter");
		}
		return String.join(" and ", filterStatements);
	}
	
	/**
	 * 
	 * @param filter
	 * @return
	 */
	private StringBuilder processFilter(IQueryFilter filter) {
		// logic taken from SqlInterpreter.processFilter
		IQueryFilter.QUERY_FILTER_TYPE filterType = filter.getQueryFilterType();
		if(filterType == IQueryFilter.QUERY_FILTER_TYPE.SIMPLE) {
			return processSimpleQueryFilter((SimpleQueryFilter) filter);
		} else if(filterType == IQueryFilter.QUERY_FILTER_TYPE.AND) {
			return processAndQueryFilter((AndQueryFilter) filter);
		} else if(filterType == IQueryFilter.QUERY_FILTER_TYPE.OR) {
			return processOrQueryFilter((OrQueryFilter) filter);
		} else if(filterType == IQueryFilter.QUERY_FILTER_TYPE.FUNCTION) {
			throw new IllegalArgumentException("Filters with a Query Filter Type of Function are not supported for FAISS vector databases");
		}else if(filterType == IQueryFilter.QUERY_FILTER_TYPE.BETWEEN) {
			throw new IllegalArgumentException("Filters with a Query Filter Type of Between are not supported for FAISS vector databases");
		}
		return null;
	}
	
	/**
	 * 
	 * @param filter
	 * @return
	 */
	protected StringBuilder processOrQueryFilter(OrQueryFilter filter) {
		StringBuilder filterBuilder = new StringBuilder();
		List<IQueryFilter> filterList = filter.getFilterList();
		int numAnds = filterList.size();
		for(int i = 0; i < numAnds; i++) {
			if(i == 0) {
				filterBuilder.append("(");
			} else {
				filterBuilder.append(" or ");
			}
			filterBuilder.append(processFilter(filterList.get(i)));
		}
		filterBuilder.append(")");
		return filterBuilder;
	}

	/**
	 * 
	 * @param filter
	 * @return
	 */
	protected StringBuilder processAndQueryFilter(AndQueryFilter filter) {
		StringBuilder filterBuilder = new StringBuilder();
		List<IQueryFilter> filterList = filter.getFilterList();
		int numAnds = filterList.size();
		for(int i = 0; i < numAnds; i++) {
			if(i == 0) {
				filterBuilder.append("(");
			} else {
				filterBuilder.append(" and ");
			}
			filterBuilder.append(processFilter(filterList.get(i)));
		}
		filterBuilder.append(")");
		return filterBuilder;
	}
	
	/**
	 * 
	 * @param filter
	 * @return
	 */
	protected StringBuilder processBetweenQueryFilter(BetweenQueryFilter filter)
	{
		StringBuilder retBuilder = new StringBuilder();
		retBuilder.append(processSelector(filter.getColumn(), true));
		retBuilder.append("  BETWEEN  ");
		retBuilder.append(filter.getStart());
		retBuilder.append("  AND  ");
		retBuilder.append(filter.getEnd());
		return retBuilder;
	}
	
	/**
	 * 
	 * @param filter
	 * @return
	 */
	protected StringBuilder processSimpleQueryFilter(SimpleQueryFilter filter) {
		NounMetadata leftComp = filter.getLComparison();
		NounMetadata rightComp = filter.getRComparison();
		String thisComparator = filter.getComparator();
		
		FILTER_TYPE fType = filter.getSimpleFilterType();
		if(fType == FILTER_TYPE.COL_TO_COL) {
			return addSelectorToSelectorFilter(leftComp, rightComp, thisComparator);
		} else if(fType == FILTER_TYPE.COL_TO_VALUES) {
			return addSelectorToValuesFilter(leftComp, rightComp, thisComparator);
		} else if(fType == FILTER_TYPE.VALUES_TO_COL) {
			// same logic as above, just switch the order and reverse the comparator if it is numeric
			return addSelectorToValuesFilter(rightComp, leftComp, IQueryFilter.getReverseNumericalComparator(thisComparator));
		} else if(fType == FILTER_TYPE.COL_TO_QUERY) {
			throw new IllegalArgumentException("Filter of with a Filter Type of COL_TO_QUERY are not supported for FAISS vector databases");
		} else if(fType == FILTER_TYPE.QUERY_TO_COL) {
			throw new IllegalArgumentException("Filter of with a Filter Type of QUERY_TO_COL are not supported for FAISS vector databases");
		} else if(fType == FILTER_TYPE.COL_TO_LAMBDA) {
			throw new IllegalArgumentException("Filter of with a Filter Type of COL_TO_LAMBDA are not supported for FAISS vector databases");
		} else if(fType == FILTER_TYPE.LAMBDA_TO_COL) {
			// same logic as above, just switch the order and reverse the comparator if it is numeric
			throw new IllegalArgumentException("Filter of with a Filter Type of LAMBDA_TO_COL are not supported for FAISS vector databases");
		} else if(fType == FILTER_TYPE.VALUE_TO_VALUE) {
			// WHY WOULD YOU DO THIS!!!
			throw new IllegalArgumentException("Filter of with a Filter Type of VALUE_TO_VALUE are not supported for FAISS vector databases");
		} 
		return null;
	}
	
	/**
	 * Add filter for column to column
	 * @param leftComp
	 * @param rightComp
	 * @param thisComparator
	 */
	protected StringBuilder addSelectorToSelectorFilter(NounMetadata leftComp, NounMetadata rightComp, String thisComparator) {
		// get the left side
		IQuerySelector leftSelector = (IQuerySelector) leftComp.getValue();
		IQuerySelector rightSelector = (IQuerySelector) rightComp.getValue();

		/*
		 * Add the filter syntax here once we have the correct physical names
		 */
		
		StringBuilder filterBuilder = new StringBuilder();
		filterBuilder.append(leftSelector.getQueryStructName());
		if(thisComparator.equals("<>")) {
			thisComparator = "!=";
		}
		filterBuilder.append(" ").append(thisComparator).append(" ").append(rightSelector.getQueryStructName());

		return filterBuilder;
	}
	
	/**
	 * Add filter for a column to values
	 * @param filters 
	 * @param leftComp
	 * @param rightComp
	 * @param thisComparator
	 */
	protected StringBuilder addSelectorToValuesFilter(NounMetadata leftComp, NounMetadata rightComp, String thisComparator) {
		StringBuilder filterBuilder = new StringBuilder();
		
		// get the left side
		IQuerySelector leftSelector = (IQuerySelector) leftComp.getValue();
		String leftDataType = leftSelector.getDataType();		
		if(leftDataType == null) {
			String leftConceptProperty = leftSelector.getQueryStructName();
			filterBuilder.append(leftConceptProperty);
		}
		
		boolean needToClose = false;
		thisComparator = thisComparator.trim();
		switch(thisComparator) {
			case "==":
			case "!=":
			case ">":
			case "<":
				break;
			case "?like":
				thisComparator = ".str.contains(";
				needToClose = true;
				break;
			case "?begins":
				thisComparator = ".str.startswith(";
				needToClose = true;
				break;
			case "?ends":
				thisComparator = ".str.endswith(";
				needToClose = true;
				break;
			default:
				throw new IllegalArgumentException("Comparator is not defined");
		}
		
		filterBuilder.append(thisComparator);
		
		// ugh... this is gross
		if(rightComp.getValue() instanceof Collection && !needToClose) {
			filterBuilder.append("isin(").append(PyUtils.determineStringType(rightComp.getValue())).append(")");
		} else {
			filterBuilder.append(PyUtils.determineStringType(rightComp.getValue()));
		}
		
		if (needToClose) {
			filterBuilder.append(")");
		}
		
		return filterBuilder;
	}
	
	/**
	 * Method is used to generate the appropriate syntax for each type of selector
	 * Note, this returns everything without the alias since this is called again from
	 * the base methods it calls to allow for complex math expressions
	 * @param selector
	 * @return
	 */
	protected String processSelector(IQuerySelector selector, boolean addProcessedColumn) {
		IQuerySelector.SELECTOR_TYPE selectorType = selector.getSelectorType();
		if(selectorType == IQuerySelector.SELECTOR_TYPE.CONSTANT) {
			return processConstantSelector((QueryConstantSelector) selector);
		} else if(selectorType == IQuerySelector.SELECTOR_TYPE.COLUMN) {
			return processColumnSelector((QueryColumnSelector) selector, addProcessedColumn);
		} else if(selectorType == IQuerySelector.SELECTOR_TYPE.ARITHMETIC) {
			throw new IllegalArgumentException("Not supported.");
		} else if(selectorType == IQuerySelector.SELECTOR_TYPE.OPAQUE) {
			throw new IllegalArgumentException("Filter of with an Opaque Selector Type are unsupported for FAISS vector databases");
		}else if(selectorType == IQuerySelector.SELECTOR_TYPE.IF_ELSE) {
			throw new IllegalArgumentException("Filter of with an If Else Selector Type are unsupported for FAISS vector databases");
		}
		return null;
	}
	
	protected String processConstantSelector(QueryConstantSelector selector) {
		Object constant = selector.getConstant();
		if(constant instanceof SubQueryExpression) {
			throw new IllegalArgumentException("Sub Query Expressions are not supported");
		} else if(constant instanceof Number) {
			return constant.toString();
		} else if(constant instanceof Boolean){
			String boolString = constant.toString();
			String pythonTrueFalse = Character.toUpperCase(boolString.charAt(0)) + boolString.substring(1);
			return pythonTrueFalse;
		} else { 
			return "'" + AbstractSqlQueryUtil.escapeForSQLStatement(constant + "") + "'";
		}
	}
	
	/**
	 * The second
	 * @param selector
	 * @param isTrueColumn
	 * @return
	 */
	protected String processColumnSelector(QueryColumnSelector selector, boolean notEmbeddedColumn) {
		String colName = selector.getColumn();
		return colName;
	}
	
//	public static void main(String[] args) throws Exception {
//		Properties tempSmss = new Properties();
//		tempSmss.put("CONNECTION_URL", "Semoss_Dev/vector/");
//		tempSmss.put("VECTOR_TYPE", "FAISS");
//		tempSmss.put("INDEX_CLASSES", "default");
//		tempSmss.put("ENCODER_TYPE", "huggingface");
//		tempSmss.put("ENCODER_NAME", "sentence-transformers/paraphrase-mpnet-base-v2");
//		
//		FaissDatabaseEngine engine = new FaissDatabaseEngine();
//		engine.open(tempSmss);
//		
//		engine.close();
//	}
}
