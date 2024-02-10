package prerna.engine.impl.vector;

import java.io.File;
import java.io.IOException;
import java.io.Reader;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.Map.Entry;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.StringEscapeUtils;
import org.apache.commons.text.StringSubstitutor;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.json.JSONArray;
import org.json.JSONObject;

import com.google.gson.Gson;
import com.pgvector.PGvector;

import au.com.bytecode.opencsv.CSVReader;
import prerna.cluster.util.ClusterUtil;
import prerna.cluster.util.DeleteFilesFromEngineRunner;
import prerna.ds.py.PyUtils;
import prerna.ds.py.TCPPyTranslator;
import prerna.engine.api.IEngine;
import prerna.engine.api.IModelEngine;
import prerna.engine.api.VectorDatabaseTypeEnum;
import prerna.reactor.vector.VectorDatabaseParamOptionsEnum;
import prerna.engine.impl.model.ModelEngineConstants;
import prerna.om.Insight;
import prerna.om.InsightStore;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.execptions.SemossPixelException;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.tcp.client.NativePySocketClient;
import prerna.util.Constants;
import prerna.util.PortAllocator;
import prerna.util.Settings;
import prerna.util.Utility;
import prerna.util.sql.AbstractSqlQueryUtil;
import prerna.util.sql.PGVectorQueryUtil;
import prerna.util.sql.RdbmsTypeEnum;
import prerna.util.sql.SqlQueryUtilFactory;

public class PGVectorDatabaseEngine extends AbstractVectorDatabaseEngine {

	private static final Logger classLogger = LogManager.getLogger(PGVectorDatabaseEngine.class);

	private static final String VECTOR_SEARCHER_NAME = "VECTOR_SEARCHER_NAME";
	public static final String KEYWORD_ENGINE_ID = "KEYWORD_ENGINE_ID";

	private static final String DIR_SEPARATOR = "/";
	private static final String FILE_SEPARATOR = java.nio.file.FileSystems.getDefault().getSeparator();
	private static final String tokenizerInitScript = "from genai_client import get_tokenizer;cfg_tokenizer = get_tokenizer(tokenizer_name = '${MODEL}', max_tokens = ${MAX_TOKENS}, tokenizer_type = '${MODEL_TYPE}');import vector_database;";

	File vectorDbFolder;
	File schemaFolder;

	List<String> indexClasses;

	// python server
	TCPPyTranslator pyt = null;
	NativePySocketClient socketClient = null;
	Process p = null;
	String port = null;
	String prefix = null;
	String pyDirectoryBasePath = null;
	File cacheFolder;
	String vectorTableName = null;
	//SMSS props
	Properties smss = null;

	@Override
	public void open(Properties smssProp) throws Exception {
		super.open(smssProp);

		this.smss = smssProp;

		Connection con = PGVectorDatabaseUtils.getDatabaseConnection(this.smss);

		vectorTableName=smssProp.getProperty(Constants.PGVECTOR_TABLE_NAME);

		// highest directory (first layer inside vector db base folder)
		this.pyDirectoryBasePath = this.connectionURL + "py" + DIR_SEPARATOR;
		this.vectorDbFolder = new File(this.connectionURL);
		this.cacheFolder = new File(pyDirectoryBasePath.replace(FILE_SEPARATOR, DIR_SEPARATOR));

		// second layer - This holds all the different "tables". The reason we want this is to easily and quickly grab the sub folders
		this.schemaFolder = new File(this.connectionURL, "schema");
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

		// This could get moved depending on other vector db needs
		// This is to get the Model Name and Max Token for an encoder -- we need this to verify chunks aren't getting truncated
		String embedderEngineId = this.smssProp.getProperty(Constants.EMBEDDER_ENGINE_ID);
		if (embedderEngineId == null) {
			embedderEngineId = this.smssProp.getProperty("ENCODER_ID");
			if (embedderEngineId == null) {
				throw new IllegalArgumentException("Embedder Engine ID is not provided.");
			}

			this.smssProp.put(Constants.EMBEDDER_ENGINE_ID, embedderEngineId);
		}

		IModelEngine modelEngine = Utility.getModel(embedderEngineId);
		Properties modelProperties = modelEngine.getSmssProp();
		if (modelProperties.isEmpty() || !modelProperties.containsKey(Constants.MODEL)) {
			throw new IllegalArgumentException("Model Engine must be created and contain MODEL");
		}

		this.smssProp.put(Constants.MODEL, modelProperties.getProperty(Constants.MODEL));
		this.smssProp.put(IModelEngine.MODEL_TYPE, modelProperties.getProperty(IModelEngine.MODEL_TYPE));
		if (!modelProperties.containsKey(Constants.MAX_TOKENS)) {
			this.smssProp.put(Constants.MAX_TOKENS, "None");	
		} else {
			this.smssProp.put(Constants.MAX_TOKENS, modelProperties.getProperty(Constants.MAX_TOKENS));
		}

		// model engine responsible for creating keywords
		String keywordGeneratorEngineId = this.smssProp.getProperty(KEYWORD_ENGINE_ID);
		if (keywordGeneratorEngineId != null) {
			// pull the model smss if needed
			Utility.getModel(keywordGeneratorEngineId);
			this.smssProp.put(KEYWORD_ENGINE_ID, keywordGeneratorEngineId);
		} else {
			// add it to the smss prop so the string substitution does not fail
			this.smssProp.put(KEYWORD_ENGINE_ID, "");
		}

		// vars for string substitution
		this.vars = new HashMap<>(this.smssProp);
	}

	public void startServer() {
		// spin the server
		// start the client
		// get the startup command and parameters - at some point we need a better way than the command

		// execute all the basic commands		

		// break the commands seperated by ;
		String [] commands = (tokenizerInitScript).split(ModelEngineConstants.PY_COMMAND_SEPARATOR);

		// need to iterate through and potential spin up tables themselves
		if (this.indexClasses.size() > 0) {
			ArrayList<String> modifiedCommands = new ArrayList<>(Arrays.asList(commands));
			commands = modifiedCommands.stream().toArray(String[]::new);
		}

		// replace the Vars
		for(int commandIndex = 0; commandIndex < commands.length;commandIndex++) {
			commands[commandIndex] = fillVars(commands[commandIndex]);
		}

    	port=PortAllocator.getInstance().getNextAvailablePort()+"";

		String timeout = "30";
		if(smssProp.containsKey(Constants.IDLE_TIMEOUT))
			timeout = smssProp.getProperty(Constants.IDLE_TIMEOUT);

		if(!this.cacheFolder.exists()) {
			this.cacheFolder.mkdirs();
		}

		String venvEngineId = this.smssProp.getProperty(Constants.VIRTUAL_ENV_ENGINE, null);
		String venvPath = venvEngineId != null ? Utility.getVenvEngine(venvEngineId).pathToExecutable() : null;

		String loggerLevel = this.smssProp.getProperty(Settings.LOGGER_LEVEL, "INFO");
		Object [] outputs = Utility.startTCPServerNativePy(this.cacheFolder.getAbsolutePath(), port, venvPath, timeout, loggerLevel);

		this.p = (Process) outputs[0];
		this.prefix = (String) outputs[1];

		socketClient = new NativePySocketClient();
		socketClient.connect("127.0.0.1", Integer.parseInt(port), false);

		// connect the client
		connectClient();

		// create the py translator
		pyt = new TCPPyTranslator();
		pyt.setSocketClient(socketClient);

		// TODO remove once bug is caught / fixed
		StringBuilder intitPyCommands = new StringBuilder("\n");
		for (String command : commands) {
			intitPyCommands.append(command).append("\n");
		}
		classLogger.info("Initializing PG Vector db with the following py commands >>>" + intitPyCommands.toString());
		pyt.runEmptyPy(commands);
	}

	public void initSQL(Connection con, String schema, String table) throws SQLException {
		//creating embeddings table
		Statement statement = con.createStatement();
		PGVectorQueryUtil pgVectorQueryUtil = new PGVectorQueryUtil();
		String sqlQueryString = pgVectorQueryUtil.createEmbeddingsTable(schema,table);
		classLogger.info(">>>>> " + sqlQueryString);
		statement.execute(sqlQueryString);
	}

	@SuppressWarnings("unchecked")
	private String fillVars(String input) {
		StringSubstitutor sub = new StringSubstitutor(vars);
		String resolvedString = sub.replace(input);
		return resolvedString;
	}

	public boolean connectClient() {
		Thread t = new Thread(socketClient);
		t.start();
		while(!socketClient.isReady())
		{
			synchronized(socketClient)
			{
				try 
				{
					socketClient.wait();
					classLogger.info("Setting the socket client ");
				} catch (InterruptedException e) {
					classLogger.error(Constants.STACKTRACE, e);
				}								
			}
		}
		return false;
	}

	@Override
	public VectorDatabaseTypeEnum getVectorDatabaseType() {
		return VectorDatabaseTypeEnum.PGVECTOR;
	}

	@Override
	public void addDocument(List<String> filePaths, Map<String, Object> parameters) {
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
			chunkUnit = (String) parameters.get(VectorDatabaseParamOptionsEnum.EXTRACTION_METHOD.getKey());
		}

		Insight insight = getInsight(parameters.get("insight"));
		if (insight == null) {
			throw new IllegalArgumentException("Insight must be provided to run Model Engine Encoder");
		}


		Connection con = null;
		//check if vectors already present for documents
		try {
			//set the connection
			con = PGVectorDatabaseUtils.getDatabaseConnection(this.smss);
			PGvector.addVectorType(con);
			
			//iterate over all files
			for (String documentName : filePaths) {
				PGVectorQueryUtil pgVectorQueryUtil = new PGVectorQueryUtil();
				String sql = pgVectorQueryUtil.removeEmbeddings(this.smssProp.getProperty(Constants.PGVECTOR_TABLE_NAME),documentName);
				Statement statement = null;
				try {
					statement = con.createStatement();
					statement.execute(sql);
				} catch (SQLException e) {
					e.printStackTrace();
				}finally {
					closeAutoClosable(statement, classLogger);
				}

			}


			// first we need to extract the text from the document
			// TODO change this to json so we never have an encoding issue
			checkSocketStatus();

			File indexDirectory = new File(this.schemaFolder, indexClass);
			File documentDir = new File(indexDirectory, "documents");
			if(!documentDir.exists()) {
				documentDir.mkdirs();
			}

			boolean filesAppoved = FaissDatabaseUtils.verifyFileTypes(filePaths, new ArrayList<>(Arrays.asList(documentDir.list())));
			if (!filesAppoved) {
				throw new IllegalArgumentException("Currently unable to mix csv with non-csv file types.");
			}

			File tableIndexFolder = new File(this.schemaFolder + DIR_SEPARATOR + indexClass, "indexed_files");
			if (!tableIndexFolder.exists()) {
				tableIndexFolder.mkdirs();
			}
			if (!this.indexClasses.contains(indexClass)) {
				this.indexClasses.add(indexClass);
			}

			String columnsToIndex = "";
			List<String> extractedFiles = new ArrayList<String>();
			List<String> filesToCopyToCloud = new ArrayList<String>(); // create a list to store all the net new files so we can push them to the cloud
			String chunkingStrategy = PyUtils.determineStringType(parameters.getOrDefault("chunkingStrategy", "ALL"));

			// move the documents from insight into documents folder
			HashSet<File> fileToExtractFrom = new HashSet<File>();
			for (String fileName : filePaths) {
				File fileInInsightFolder = new File(fileName);

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
				String documentName = document.getName().split("\\.")[0];
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
							StringBuilder extractTextFromDocScript = new StringBuilder();
							extractTextFromDocScript.append("vector_database.extract_text(source_file_name = '")
							.append(document.getAbsolutePath().replace(FILE_SEPARATOR, DIR_SEPARATOR))
							.append("', target_folder = '")
							.append(this.schemaFolder.getAbsolutePath().replace(FILE_SEPARATOR, DIR_SEPARATOR) + DIR_SEPARATOR + indexClass + DIR_SEPARATOR + "extraction_files")
							.append("', output_file_name = '")
							.append(extractedFileName)
							.append("')");
							Number rows = (Number) pyt.runScript(extractTextFromDocScript.toString());

							rowsCreated = rows.intValue();
						} else {
							rowsCreated = PGVectorDatabaseUtils.convertFilesToCSV(extractedFile.getAbsolutePath(), chunkMaxTokenLength, tokenOverlapBetweenChunks, document, this.pyt);

						}

						// check to see if the file data was extracted
						if (rowsCreated <= 1) {
							// no text was extracted so delete the file
							FileUtils.forceDelete(extractedFile); // delete the csv
							FileUtils.forceDelete(document); // delete the input file e.g pdf
							continue;
						}

						classLogger.info("Creating chunks from extracted text for " + documentName);

						// TODO ADD LOGIC to split text
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
						.append(", cfg_tokenizer = cfg_tokenizer)");
						pyt.runScript(splitTextCommand.toString());

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
				for(int i = 0; i<extractedFiles.size();i++) {
					try {
						Map<String, Object> resultMap = readCsv(Paths.get(extractedFiles.get(i)));

						List<String> contentList = (List<String>) resultMap.get("textToEmbed");
						Map<Integer, Map<String, String>> csvMap = (Map<Integer, Map<String, String>>) resultMap.get("csvToMap");

						String embeederEngineIdInput = "${EMBEDDER_ENGINE_ID}";
						String engineId = fillVars(embeederEngineIdInput);
						IModelEngine engine = Utility.getModel(engineId);
						Map<String,Object> paramMap = new HashMap<String, Object>();
						Object output = engine.embeddings(contentList, (Insight)parameters.get("insight"),paramMap);

						//convert output to json object
						Gson gson = new Gson();
						String jsonString = gson.toJson(output);
						JSONObject outputMap = new JSONObject(jsonString);

						//fetch response key
						Object responseObject = outputMap.get("response");

						//convert to array of arrays
						JSONArray responseArray = (JSONArray) responseObject;

						//map excel row id to embeddings array id
						//temp object will hold excel row data along with embedding
						Map<Integer, Map<String,Object>> dataToInsert = new HashMap<Integer, Map<String,Object>>();
						for(int j=0;j<responseArray.length();j++) {
							Object embedding = responseArray.get(j);
							Map<String, String> row = csvMap.get(j);
							Map<String,Object> tempMap = new HashMap<String, Object>();
							tempMap.put("embedding",embedding);
							tempMap.put("data", row);
							dataToInsert.put(j, tempMap);
						}


						//insert into table keys(map keys) values(associated values)
						//get engine using vector engine id
						String vectorEngineId = this.engineId;
						IEngine vectorEngine =  Utility.getEngine(vectorEngineId);

						//Get QueryUtil
						PGVectorQueryUtil queryUtil = new PGVectorQueryUtil();

						Map<String, Object> rowData = dataToInsert.get(0);
						Map<String, String> row = (Map<String, String>) rowData.get("data");

						String columnString = "embedding";
						for (String column : row.keySet()) {
							columnString += ", " + column + "";
						}

						String insertString = "?";
						int colNum = row.keySet().size();
						for (int j = 0; j<colNum; j++) {
							insertString+=", ?";
						}
						String sql = "INSERT INTO "+vectorTableName+" ("+columnString+") VALUES ("+insertString+")";

						PreparedStatement preparedStatement = null;
						try {
							preparedStatement = con.prepareStatement(sql);


							// Iterate over the data and add batch
							for (int key : dataToInsert.keySet()) {
								Map<String, Object> data = dataToInsert.get(key);
								Object embedding=data.get("embedding");

								float[] floatArray = null;
								// Check if the object is an instance of ArrayList<Float>
								if (embedding instanceof JSONArray) {

									JSONArray jsonArray =  (JSONArray)embedding;
									// Object index1=jsonArray.get(0);
									// Convert jsonArray to float[]
									floatArray= new float[jsonArray.length()];

									for (int dimension = 0; dimension < jsonArray.length(); dimension++) {
										try {
											floatArray[dimension] = ((Double) jsonArray.get(dimension)).floatValue();
										} catch (Exception e) {
											e.printStackTrace();
										}				                }
								} else {
									System.out.println("The object is not a JSONArray");
								}

								Map<String, String> metaData = (Map<String, String>) data.get("data");
								int index = 1;

								//TODO add check for float array here
								preparedStatement.setObject(index++, new PGvector(floatArray));		                    
								for (String columnName : metaData.keySet()) {
									preparedStatement.setObject(index++, metaData.get(columnName));
								}
								preparedStatement.addBatch();
							}
							preparedStatement.executeBatch();

						}catch(SQLException ex) {
							ex.printStackTrace();
						} finally {
							closeAutoClosable(preparedStatement, classLogger);
						}

					} catch (Exception e) {
						e.printStackTrace();
					}
				}
			}	
		} catch (SQLException e) {
			e.printStackTrace();
		}finally {
			closeAutoClosable(con, classLogger);
		}



	}


	public Map<String, Object> readCsv(Path filePath) throws Exception {
		List<String> contentList = new ArrayList<String>();
		Map<Integer, Map<String, String>> rowMap= new HashMap<Integer, Map<String, String>>();
		try (Reader reader = Files.newBufferedReader(filePath)) {
			try (CSVReader csvReader = new CSVReader(reader)) {
				String[] line;
				boolean start = true;
				Map<String,Integer> headersMap = new HashMap<String, Integer>();
				int rowId=0;
				while ((line = csvReader.readNext()) != null) {
					if(start) {
						for(int i=0;i<line.length;i++) {
							headersMap.put(line[i],i);
						}
						start = false;
					}else {
						contentList.add(line[headersMap.get("Content")]);
						Map<String, String> row = new HashMap<String, String>();
						row.put("source",line[headersMap.get("Source")]);
						row.put("modality",line[headersMap.get("Modality")]);
						row.put("divider",line[headersMap.get("Divider")]);
						row.put("part",line[headersMap.get("Part")]);
						row.put("tokens",line[headersMap.get("Tokens")]);
						row.put("content",line[headersMap.get("Content")]);
						rowMap.put(rowId, row);
						rowId++;
					}
				}
			}
		}
		Map<String, Object> retMap = new HashMap<String, Object>();
		retMap.put("textToEmbed", contentList);
		retMap.put("csvToMap",rowMap);
		return retMap;
	}

	@Override
	public void removeDocument(List<String> filePaths, Map<String, Object> parameters) {
		String indexClass = this.defaultIndexClass;
		if (parameters.containsKey("indexClass")) {
			indexClass = (String) parameters.get("indexClass");
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
			this.indexClasses.remove(indexClass);
		}

		//delete document embeddings from pgVector database
		//TODO: will optimize the code to reuse db connection
		Connection con = null;
		Statement statement = null;
		for (String document : filePaths) {
			PGVectorQueryUtil pgVectorQueryUtil = new PGVectorQueryUtil();
			String query = pgVectorQueryUtil.removeEmbeddings(vectorTableName,document);

			try {
				con = PGVectorDatabaseUtils.getDatabaseConnection(this.smss);			
				statement = con.createStatement();
				statement.execute(query);		
			} catch (SQLException e) {
				classLogger.error(Constants.STACKTRACE, e);
				String driverError = e.getMessage();
				String errorMessage = "Unable to establish connection given the connection details.\nDriver produced error: \" ";
				errorMessage += driverError;
				errorMessage += " \"";
				throw new SemossPixelException(new NounMetadata(errorMessage, PixelDataType.CONST_STRING, PixelOperationType.ERROR));
			} finally {
				closeAutoClosable(statement, classLogger);
				closeAutoClosable(con, classLogger);
			}
		}

		if (ClusterUtil.IS_CLUSTER) {
			Thread deleteFilesFromCloudThread = new Thread(new DeleteFilesFromEngineRunner(engineId, this.getCatalogType(), filesToRemoveFromCloud.stream().toArray(String[]::new)));
			deleteFilesFromCloudThread.start();
		}


	}

	@Override
	public Object nearestNeighbor(String question, Number limit, Map<String, Object> parameters) {
		//TODO: implement params

		checkSocketStatus();


		Insight insight = getInsight(parameters.get("insight"));
		if (insight == null) {
			throw new IllegalArgumentException("Insight must be provided to run Model Engine Encoder");
		}


		String indexClass = this.defaultIndexClass;	
		// make sure the database has docuemnts loaded / added
		if(!this.indexClasses.contains(indexClass)) {
			throw new IllegalArgumentException("There are no documents loaded in the index class of the vector database.");
		}

		String searchFilters = "None";
		if (parameters.containsKey("filters")) {}

		if (parameters.containsKey(VectorDatabaseParamOptionsEnum.COLUMNS_TO_RETURN.getKey())) {}

		if (parameters.containsKey(VectorDatabaseParamOptionsEnum.RETURN_THRESHOLD.getKey())) {}

		if (parameters.containsKey(VectorDatabaseParamOptionsEnum.ASCENDING.getKey())) {}


		//PG VECTOR LOGIC FOR SEARCH EMBEDDINGS

		//create embedding from question
		//TODO: redundant logic for creating embeddings - move this to utils
		String embeederEngineIdInput = "${EMBEDDER_ENGINE_ID}";
		String engineId = fillVars(embeederEngineIdInput);
		IModelEngine engine = Utility.getModel(engineId);
		Map<String,Object> paramMap = new HashMap<String, Object>();
		List<String> questionList = new ArrayList<String>();
		questionList.add(question);
		Object output = engine.embeddings(questionList,insight,paramMap);

		//convert output to json object
		Gson gson = new Gson();
		String jsonString = gson.toJson(output);
		JSONObject outputMap = new JSONObject(jsonString);

		//fetch response key
		Object responseObject = outputMap.get("response");

		//convert to array of arrays
		JSONArray responseArray = (JSONArray) responseObject;

		//get query to calculate nearest n vectors
		PGVectorQueryUtil pgVectorQueryUtil = new PGVectorQueryUtil();
		String vectorEngineId = this.getEngineId();
		IEngine vectorEngine = Utility.getEngine(vectorEngineId);

		Properties smssProperties = vectorEngine.getSmssProp();
		String sql="";
		if(smssProperties.getProperty("DISTANCE_METHOD").contains("cosine")) {
			sql = pgVectorQueryUtil.searchNearestNeighbour(vectorTableName,responseArray.get(0),limit);
		}

		//create statement
		Connection con = null;
		Statement statement = null;
		try {
			//get connection to database
			con = PGVectorDatabaseUtils.getDatabaseConnection(this.smss);
			statement = con.createStatement();
			statement.execute(sql);
			ResultSet resultset = statement.getResultSet();
			List<Map<String, Object>> resultList = new ArrayList<Map<String,Object>>();
			if(resultset != null) {
				ResultSetMetaData metadata = resultset.getMetaData();
				int numberOfColumns = metadata.getColumnCount();
				while (resultset.next()) {   
					Map<String, Object> resultMap = new HashMap<String, Object>();
					int i = 1;
					while(i <= numberOfColumns) {
						String colName = metadata.getColumnName(i);
						String newColName = colName.substring(0,1).toUpperCase()+colName.substring(1);
						resultMap.put(newColName, resultset.getString(i));
						i++;
					}
					resultList.add(resultMap);
				}
				return resultList;
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}finally {
			closeAutoClosable(statement, classLogger);
			closeAutoClosable(con, classLogger);
		}	
		return null;
	}

	@Override
	public List<Map<String, Object>> listDocuments(Map<String, Object> parameters) {
		String indexClass = this.defaultIndexClass;
		if (parameters.containsKey("indexClass")) {
			indexClass = (String) parameters.get("indexClass");
		}

		File documentsDir = new File(this.schemaFolder.getAbsolutePath() + DIR_SEPARATOR + indexClass + DIR_SEPARATOR + "documents");

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
	public void close() throws IOException {
		if (this.socketClient != null && this.socketClient.isConnected()) {
			this.socketClient.stopPyServe(this.pyDirectoryBasePath);
			this.socketClient.close();
			this.pyDirectoryBasePath = null;
		}
		if(this.p != null && this.p.isAlive()) {
			this.p.destroy();
		}

	}

	private void checkSocketStatus() {
		if(this.socketClient == null || !this.socketClient.isConnected()) {
			this.startServer();
		}
	}

	private Insight getInsight(Object insightObj) {
		if (insightObj instanceof String) {
			return InsightStore.getInstance().get((String) insightObj);
		} else {
			return (Insight) insightObj;
		}
	}

	/**
	 * Close a connection, statement, or result set
	 * @param closeable
	 */
	private void closeAutoClosable(AutoCloseable closeable, Logger logger) {
		if(closeable != null) {
			try {
				closeable.close();
			} catch (Exception e) {
				logger.error(Constants.STACKTRACE, e);
			}
		}
	}
}
