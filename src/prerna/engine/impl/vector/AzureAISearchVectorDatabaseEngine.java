package prerna.engine.impl.vector;

import java.io.File;
import java.io.IOException;
import java.io.Reader;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.text.StringSubstitutor;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import au.com.bytecode.opencsv.CSVReader;
import prerna.cluster.util.ClusterUtil;
import prerna.cluster.util.DeleteFilesFromEngineRunner;
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
import prerna.util.sql.RDBMSUtility;

public class AzureAISearchVectorDatabaseEngine extends AbstractVectorDatabaseEngine {

	private static final Logger classLogger = LogManager.getLogger(AzureAISearchVectorDatabaseEngine.class);

	private String host = null;
	private String protocol = "https";
	private String apiKey = null;
	
	private String embedderEngineId = null;
	private IModelEngine embeddingsEngine = null;
	
	private static final String tokenizerInitScript = "from genai_client import get_tokenizer;cfg_tokenizer = get_tokenizer(tokenizer_name = '${MODEL}', max_tokens = ${MAX_TOKENS}, tokenizer_type = '${MODEL_TYPE}');import vector_database;";

	@Override
	public void open(Properties smssProp) throws Exception {
		super.open(smssProp);
		
		this.host = smssProp.getProperty(Constants.HOSTNAME);
		if(this.host == null || (this.host=this.host.trim()).isEmpty()) {
			throw new IllegalArgumentException("Must define the host");
		}
		
		if(this.host.startsWith("https://")) {
			this.host = this.host.substring("https://".length(), host.length());
		} else if(this.host.startsWith("http://")) {
			this.protocol = "http";
			this.host = this.host.substring("http://".length(), host.length());
		}
		
		this.apiKey = smssProp.getProperty(Constants.API_KEY);
		if(this.apiKey == null || (this.apiKey=this.apiKey.trim()).isEmpty()) {
			throw new IllegalArgumentException("Must define the api key");
		}
		
		this.pyDirectoryBasePath = Utility.normalizePath(this.engineDirectoryPath + "py" + DIR_SEPARATOR);
		this.cacheFolder = new File(this.pyDirectoryBasePath);

		this.connectionURL = RDBMSUtility.fillParameterizedFileConnectionUrl(this.connectionURL, this.engineId, this.engineName);

		//this.cacheFolder = new File(pyDirectoryBasePath.replace(FILE_SEPARATOR, DIR_SEPARATOR));

		// second layer - This holds all the different "tables". The reason we want this is to easily and quickly grab the sub folders
		this.schemaFolder = new File(this.connectionURL, "schema");
		if(!this.schemaFolder.exists()) {
			this.schemaFolder.mkdirs();
		}
		this.smssProp.put(Constants.WORKING_DIR, this.schemaFolder.getAbsolutePath());
		
		this.embedderEngineId = this.smssProp.getProperty(Constants.EMBEDDER_ENGINE_ID);
	}

	
	@Override
	public VectorDatabaseTypeEnum getVectorDatabaseType() {
		return VectorDatabaseTypeEnum.WEAVIATE;
		
	}

	@Override
	public void addDocument(List<String> filePaths, Map<String, Object> parameters) {
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
		if (extractedFiles.size() > 0) 
		{
			
			// TODO: implement
			// TODO: implement

			
			// there are a couple of columns that get created
			// Source	Modality	Divider	Part	Tokens	Content
			// what we are trying to index is the content column
			for(int extractedFileIndex = 0; extractedFileIndex < extractedFiles.size(); extractedFileIndex++) 
			{
				File extractedFile = new File(extractedFiles.get(extractedFileIndex));
				CSVTable dataForTable = null;
				
				try {
					dataForTable = readCsv(extractedFile);
				} catch (IOException e) {
					classLogger.error(Constants.STACKTRACE, e);
				}
				
				//dataForTable.generateAndAssignEmbeddings(embeddingsEngine, this.getInsight(insight));

				// time to pump it into weaviate
				for(int rowIndex = 0;rowIndex < dataForTable.rows.size();rowIndex++)
				{
					CSVRow row = dataForTable.getRows().get(rowIndex);
					Map<String, Object> properties = new HashMap<>();
					  properties.put("Source", row.getSource());  
					  properties.put("Modality", row.getModality());  
					  properties.put("Divider", row.getDivider());  
					  properties.put("Part", row.getPart());  
					  properties.put("Tokens", row.getTokens());  
					  properties.put("Content", row.getContent());
					  
					  Float [] vector = getEmbeddings(row.getContent(), insight);

						// TODO: implement

				}
				// TODO: implement
												
			}

			// move things to cloud
			// nothing to move.. weaviate is sitting in the cloud			
		}
		// inform the user that some chunks are too large and they might loose semantic value
		// Map<String, List<Integer>> needToReturnForWarnings = (Map<String, List<Integer>>) pythonResponseAfterCreatingFiles.get("documentsWithLargerChunks");
		
		// once all is done.. close the socket.
		stopServer();
	}
	
	private Float[] getEmbeddings(String content, Insight insight)
	{
		if (this.embeddingsEngine == null)
			this.embeddingsEngine = Utility.getModel(this.embedderEngineId);
		List <Double> embeddingsResponse = embeddingsEngine.embeddings(Arrays.asList(new String[] {content}), getInsight(insight), null).getResponse().get(0);
		Float [] retFloat = new Float[embeddingsResponse.size()];
		for(int vecIndex = 0;vecIndex < retFloat.length;vecIndex++)
			retFloat[vecIndex] = (Float)embeddingsResponse.get(vecIndex).floatValue();
		
		return retFloat;
	
	}
	
	protected CSVTable readCsv(File file) throws IOException 
	{
		CSVTable pgVectorTable = new CSVTable();
		try (Reader reader = Files.newBufferedReader(Paths.get(file.getAbsolutePath()))) {
			try (CSVReader csvReader = new CSVReader(reader)) {
				String[] line;
				boolean start = true;
				Map<String,Integer> headersMap = new HashMap<String, Integer>();
				while ((line = csvReader.readNext()) != null) {
					if(start) {
						for(int i=0;i<line.length;i++) {
							headersMap.put(line[i],i);
						}
						start = false;
					} else {
						pgVectorTable.addRow(line[headersMap.get("Source")], line[headersMap.get("Modality")], line[headersMap.get("Divider")], line[headersMap.get("Part")], line[headersMap.get("Tokens")], line[headersMap.get("Content")]);
					}
				}
			}
		}

		return pgVectorTable;
	}

	@Override
	public void removeDocument(List<String> fileNames, Map<String, Object> parameters) 
	{
		// TODO: implement
		
		if (ClusterUtil.IS_CLUSTER) {
//			Thread deleteFilesFromCloudThread = new Thread(new DeleteFilesFromEngineRunner(engineId, this.getCatalogType(), filesToRemoveFromCloud.stream().toArray(String[]::new)));
//			deleteFilesFromCloudThread.start();
		}
	}

	@Override
	public List<Map<String, Object>> nearestNeighbor(String searchStatement, Number limit, Map<String, Object> parameters) {
		// TODO: implement

		return null;
	}


	@Override
	public void close() throws IOException {
		// TODO Auto-generated method stub
		stopServer();
	}
	
	
	//// private methods
	
	public synchronized void startServer(int port) {
		
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
		String [] commands = (tokenizerInitScript).split(PyUtils.PY_COMMAND_SEPARATOR);

		// need to iterate through and potential spin up tables themselves

		// replace the Vars
		StringSubstitutor substitutor = new StringSubstitutor(this.vars);
		for(int commandIndex = 0; commandIndex < commands.length;commandIndex++) {
			String resolvedString = substitutor.replace(commands[commandIndex]);
			commands[commandIndex] = resolvedString;
		}
		
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
						classLogger.warn("Vector Database " + this.engineName + " has an invalid FORCE_PORT value");
					}
				}
			}
			
			String serverDirectory = this.cacheFolder.getAbsolutePath();
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
		
		pyt.runEmptyPy(commands);
	}
	
	protected void stopServer()
	{
		// give the port back if you are not using anything from python
		try
		{
			if(cpw != null)
			{
				cpw.shutdown(true);
				cpw = null;
			}
		}catch(Exception ignore)
		{
			
		}
	}
	

}
