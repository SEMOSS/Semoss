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
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.commons.io.FileUtils;
import org.apache.commons.text.StringSubstitutor;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.algorithm.api.SemossDataType;
import prerna.ds.py.PyUtils;
import prerna.ds.py.TCPPyTranslator;
import prerna.engine.api.VectorDatabaseTypeEnum;
import prerna.engine.impl.model.ModelEngineConstants;
import prerna.query.interpreters.IQueryInterpreter;
import prerna.query.querystruct.SelectQueryStruct;
import prerna.query.querystruct.filters.AndQueryFilter;
import prerna.query.querystruct.filters.BetweenQueryFilter;
import prerna.query.querystruct.filters.FunctionQueryFilter;
import prerna.query.querystruct.filters.IQueryFilter;
import prerna.query.querystruct.filters.OrQueryFilter;
import prerna.query.querystruct.filters.SimpleQueryFilter;
import prerna.query.querystruct.filters.SimpleQueryFilter.FILTER_TYPE;
import prerna.query.querystruct.selectors.IQuerySelector;
import prerna.query.querystruct.selectors.QueryArithmeticSelector;
import prerna.query.querystruct.selectors.QueryColumnSelector;
import prerna.query.querystruct.selectors.QueryConstantSelector;
import prerna.query.querystruct.selectors.QueryFunctionSelector;
import prerna.query.querystruct.selectors.QueryIfSelector;
import prerna.query.querystruct.selectors.QueryOpaqueSelector;
import prerna.reactor.qs.SubQueryExpression;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.om.task.ITask;
import prerna.tcp.client.NativePySocketClient;
import prerna.util.Constants;
import prerna.util.Utility;
import prerna.util.sql.AbstractSqlQueryUtil;

public class FaissDatabaseEngine extends AbstractVectorDatabaseEngine {

	private static final Logger classLogger = LogManager.getLogger(FaissDatabaseEngine.class);
	
	private static final String DIR_SEPARATOR = "/";
	private static final String FILE_SEPARATOR = java.nio.file.FileSystems.getDefault().getSeparator();
	private static final String initScript = "import vector_database;${VECTOR_SEARCHER_NAME} = vector_database.FAISSDatabase(encoder_class = vector_database.get_encoder(encoder_type='${ENCODER_TYPE}', embedding_model='${ENCODER_NAME}', api_key = '${ENCODER_API_KEY}'))";
	
	protected String vectorDatabaseSearcher = null;
	
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
	//File indexFolder;
	//File documentsFolder;
	
	@Override
	public void open(Properties smssProp) throws Exception {
		super.open(smssProp);
		
		// highest directory (first layer inside vector db base folder)
		this.pyDirectoryBasePath = this.connectionURL + "py" + DIR_SEPARATOR;
		this.vectorDbFolder = new File(this.connectionURL);
		this.cacheFolder = new File(pyDirectoryBasePath.replace(FILE_SEPARATOR, DIR_SEPARATOR));

		// second layer - This holds all the different "tables". The reason we want this is to easily and quickly grab the sub folders
		this.schemaFolder = new File(this.connectionURL, "schema");
		if(!this.schemaFolder.exists()) {
			this.schemaFolder.mkdirs();
		}
		this.smssProp.put("WORKING_DIR", this.schemaFolder.getAbsolutePath());
		
		// third layer - All the separate tables,classes, or searchers that can be added to this db
		this.indexClasses = new ArrayList<>(Arrays.asList(this.schemaFolder.list()));
		
		//this.documentsFolder = new File(this.connectionURL + DIR_SEPARATOR + "documents");
		//this.indexFolder = new File(this.connectionURL + DIR_SEPARATOR + "indexed_files");
		
		this.vectorDatabaseSearcher = Utility.getRandomString(6);
		
		this.smssProp.put("VECTOR_SEARCHER_NAME", this.vectorDatabaseSearcher);	
	
		if (!this.smssProp.containsKey("ENCODER_API_KEY")) {
			this.smssProp.put("ENCODER_API_KEY", "");	
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
		String [] commands = initScript.split(ModelEngineConstants.PY_COMMAND_SEPARATOR);
		
		// need to iterate through and potential spin up tables themselves
		if (this.indexClasses.size() > 0) {
	        ArrayList<String> modifiedCommands = new ArrayList<>(Arrays.asList(commands));
			for (String indexClass : this.indexClasses) {
				File fileToCheck = new File(this.schemaFolder.getAbsolutePath() + DIR_SEPARATOR + indexClass, "dataset.pkl");
				modifiedCommands.add("${VECTOR_SEARCHER_NAME}.create_searcher(searcher_name = '"+indexClass+"', base_path = '"+fileToCheck.getParent().replace(FILE_SEPARATOR, DIR_SEPARATOR) + DIR_SEPARATOR +"')");
				if (fileToCheck.exists()) {
			        modifiedCommands.add("${VECTOR_SEARCHER_NAME}.searchers['"+indexClass+"'].load_dataset('"+fileToCheck.getParent().replace(FILE_SEPARATOR, DIR_SEPARATOR) + DIR_SEPARATOR +"' + 'dataset.pkl')");
			        modifiedCommands.add("${VECTOR_SEARCHER_NAME}.searchers['"+indexClass+"'].load_encoded_vectors('"+fileToCheck.getParent().replace(FILE_SEPARATOR, DIR_SEPARATOR) + DIR_SEPARATOR +"' + 'vectors.pkl')");
		        }
			}
            commands = modifiedCommands.stream().toArray(String[]::new);
		}

		// replace the Vars
		for(int commandIndex = 0; commandIndex < commands.length;commandIndex++) {
			commands[commandIndex] = fillVars(commands[commandIndex]);
		}
		
		port = Utility.findOpenPort();
		
		String timeout = "30";
		if(smssProp.containsKey(Constants.IDLE_TIMEOUT))
			timeout = smssProp.getProperty(Constants.IDLE_TIMEOUT);

		if(!this.cacheFolder.exists()) {
			this.cacheFolder.mkdirs();
		}
		
		Object [] outputs = Utility.startTCPServerNativePy(this.cacheFolder.getAbsolutePath(), port, timeout);
		this.p = (Process) outputs[0];
		this.prefix = (String) outputs[1];
		
		socketClient = new NativePySocketClient();
		socketClient.connect("127.0.0.1", Integer.parseInt(port), false);
		
		// connect the client
		connectClient();
		
		// create the py translator
		pyt = new TCPPyTranslator();
		pyt.setClient(socketClient);
	
		pyt.runEmptyPy(commands);
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
	public void addDocumet(List<String> filePaths, Map <String, Object> parameters) {
		String indexClass = this.defaultIndexClass;
		if (parameters.containsKey("indexClass")) {
			indexClass = (String) parameters.get("indexClass");
		}
		
		File parentDirectory = (File) parameters.get("temporaryFileDirectory");
		
		// first we need to extract the text from the document
		// TODO change this to json so we never have an encoding issue
		checkSocketStatus();
		
		List<String> filesToIndex;
		File tableDirectory = new File(this.schemaFolder, indexClass);
		File documentDir = new File(tableDirectory, "documents");
		if(!documentDir.exists()) {
			documentDir.mkdirs();
		}
		boolean filesAppoved = FaissDatabaseUtils.verifyFileTypes(filePaths, new ArrayList<>(Arrays.asList(documentDir.list())));
		if (!filesAppoved) {
			// delete them all
			try {
				FileUtils.forceDelete(parentDirectory);
			} catch (IOException e) {
				classLogger.error(Constants.STACKTRACE, e);
				throw new IllegalArgumentException("Unable to delete the temporary file directory");
			}
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
		List <String> extractedFiled = new ArrayList<String>();
		for (String fileName : filePaths) {
			// move the documents into documents folder
			File fileInTempFolder = new File(fileName);
			
			// TODO probably need to handle zips
			if (!fileInTempFolder.isFile()) {
				continue;
			}
			
			File destinationFile = new File(documentDir, fileInTempFolder.getName());
			
			// Check if the destination file exists, and if so, delete it
			try {
				if (destinationFile.exists()) {
					FileUtils.forceDelete(destinationFile);
	            }
				FileUtils.moveFileToDirectory(fileInTempFolder, documentDir, true);
			} catch (IOException e) {
				classLogger.error(Constants.STACKTRACE, e);
				throw new IllegalArgumentException("Unable to remove previously created file for " + destinationFile.getName() + " or move it to the document directory");
			}
			
			String documentName = destinationFile.getName().split("\\.")[0];
			File extractedFile = new File(tableIndexFolder.getAbsolutePath() + DIR_SEPARATOR + documentName + ".csv");
			try {
				if (extractedFile.exists()) {
					FileUtils.forceDelete(extractedFile);
				}
				if (!destinationFile.getName().toLowerCase().endsWith(".csv")) {
					FaissDatabaseUtils.convertFilesToCSV(extractedFile.getAbsolutePath(), this.contentLength, this.contentOverlap, destinationFile);
					columnsToIndex = "['Content']"; // this needs to match the column created in the new CSV
				} else {
					// copy csv over but make sure its only csvs
					FileUtils.copyFileToDirectory(destinationFile, tableIndexFolder);
					if (parameters.containsKey(VectorDatabaseTypeEnum.ParamValueOptions.COLUMNS_TO_INDEX.getKey())) {
						columnsToIndex = PyUtils.determineStringType(parameters.get(VectorDatabaseTypeEnum.ParamValueOptions.COLUMNS_TO_INDEX.getKey()));
					} else {
						columnsToIndex = "[]"; // this is so we pass an empty list
					}
				}
				extractedFiled.add(extractedFile.getAbsolutePath().replace(FILE_SEPARATOR, DIR_SEPARATOR));
			} catch (IOException e) {
				classLogger.error(Constants.STACKTRACE, e);
				throw new IllegalArgumentException("Unable to remove old or create new text extraction file for " + documentName);
			}
		}
		
		// create dataset
		StringBuilder addDocumentPyCommand = new StringBuilder();
		
		// get the relevant FAISS searcher object in python
		addDocumentPyCommand.append(vectorDatabaseSearcher)
							.append(".searchers['")
							.append(indexClass)
							.append("']");
		
		addDocumentPyCommand.append(".addDocumet(documentFileLocation = ['")
							.append(String.join("','", extractedFiled))
							.append("'], columns_to_index = ")
							.append(columnsToIndex);
		
		if (parameters.containsKey(VectorDatabaseTypeEnum.ParamValueOptions.COLUMNS_TO_REMOVE.getKey())) {
			// add the columns based in the vector db query
			addDocumentPyCommand.append(", ")
					 			.append("columns_to_remove")
					 			.append(" = ")
					 			.append(PyUtils.determineStringType(
								 parameters.get(
										 VectorDatabaseTypeEnum.ParamValueOptions.COLUMNS_TO_REMOVE.getKey()
										 )
								 ));
		}
							
							
		addDocumentPyCommand.append(")");
		
		String script = addDocumentPyCommand.toString();
		
		classLogger.info("Running >>>" + script);
		this.pyt.runScript(script);
		try {
			FileUtils.forceDelete(parentDirectory);
		} catch (IOException e) {
			classLogger.error(Constants.STACKTRACE, e);
			throw new IllegalArgumentException("Unable to delete the temporary file directory");
		}
	}

	@Override
	public void removeDocument(List<String> filePaths, Map <String, Object> parameters) {
		String indexClass = this.defaultIndexClass;
		if (parameters.containsKey("indexClass")) {
			indexClass = (String) parameters.get("indexClass");
		}
		
		if (!this.indexClasses.contains(indexClass)) {
			throw new IllegalArgumentException("Unable to remove documents from a directory that does not exist");
		}
		
		checkSocketStatus();
		
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
				} catch (IOException e) {
					classLogger.error(Constants.STACKTRACE, e);
					throw new IllegalArgumentException("Unable to remove file: " + entry.getFileName());
				}
                classLogger.info("Deleted: " + entry.toString());
            }
	        try {
				FileUtils.forceDelete(new File(this.schemaFolder.getAbsolutePath() + DIR_SEPARATOR + indexClass + DIR_SEPARATOR + "documents", document));
			} catch (IOException e) {
				classLogger.error(Constants.STACKTRACE, e);
				throw new IllegalArgumentException("Unable to delete " + document + "from documents directory");
			}
		}
		
		// this would mean the indexClass is now empty, we should delete it
		File indexedFolder = new File(indexedFilesPath);
		if (indexedFolder.list().length == 0) {
			try {
				FileUtils.forceDelete(new File(indexedFolder.getParent()));
			} catch (IOException e) {
				classLogger.error(Constants.STACKTRACE, e);
				throw new IllegalArgumentException("Unable to delete remove the index class folder");
			}
			this.pyt.runScript(this.vectorDatabaseSearcher + ".delete_searcher(searcher_name = '"+indexClass+"')");
			this.indexClasses.remove(indexClass);
		} else {
			String script = this.vectorDatabaseSearcher + ".searchers['"+indexClass+"'].createMasterFiles(path_to_files = '" + indexedFilesPath.replace(FILE_SEPARATOR, DIR_SEPARATOR) + "')";
			this.pyt.runScript(script);
		}
	}

	@SuppressWarnings("unchecked")
	@Override
	public Object nearestNeighbor(String question, int limit, Map <String, Object> parameters) {
		
		checkSocketStatus();
		
		StringBuilder callMaker = new StringBuilder();
		
		String indexClass = this.defaultIndexClass;
		if (parameters.containsKey("indexClass")) {
			Object indexClassObj = parameters.get("indexClass");
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
		
		// TODO we will need the insight when runnings encode call through IModelEngine
		//Insight insight = (Insight) parameters.get("insight");
		//if (insight == null) {
		//	throw new IllegalArgumentException("Insight must be provided to run Model Engine Encoder");
		//}
	
		// make the question arg
		callMaker.append("question=\"\"\"")
				 .append(question.replace("\"", "\\\""))
				 .append("\"\"\"");
		
		String searchFilters = "None";
		if (parameters.containsKey("filters")) {
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
		
		if (parameters.containsKey(VectorDatabaseTypeEnum.ParamValueOptions.COLUMNS_TO_RETURN.getKey())) {
			// add the columns based in the vector db query
			callMaker.append(", ")
					 .append("columns_to_return")
					 .append(" = ")
					 .append(PyUtils.determineStringType(
							 parameters.get(
									 VectorDatabaseTypeEnum.ParamValueOptions.COLUMNS_TO_RETURN.getKey()
									 )
							 ));
		}
		
		if (parameters.containsKey(VectorDatabaseTypeEnum.ParamValueOptions.RETURN_THRESHOLD.getKey())) {
			// add the return_threshold, it should be a long or double value
			Object thresholdValue =  parameters.get(VectorDatabaseTypeEnum.ParamValueOptions.RETURN_THRESHOLD.getKey());
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
		
		if (parameters.containsKey(VectorDatabaseTypeEnum.ParamValueOptions.ASCENDING.getKey())) {
			// This should be a True or False value
			String trueFalseString = (String) parameters.get(VectorDatabaseTypeEnum.ParamValueOptions.ASCENDING.getKey());
			String pythonTrueFalse = Character.toUpperCase(trueFalseString.charAt(0)) + trueFalseString.substring(1);
			
			callMaker.append(",")
					 .append("ascending = ")
					 .append(pythonTrueFalse);
		}
		
		// close the method
 		callMaker.append(")");
 		classLogger.info("Running >>>" + callMaker.toString());
		Object output = pyt.runScript(callMaker.toString());
		return output;
	}
	
	@Override
	public String[] listDocuments(Map<String, Object> parameters) {
		String indexClass = this.defaultIndexClass;
		if (parameters.containsKey("indexClass")) {
			indexClass = (String) parameters.get("indexClass");
		}
		
		File documentsDir = new File(this.schemaFolder.getAbsolutePath() + DIR_SEPARATOR + indexClass + DIR_SEPARATOR + "documents");
		return documentsDir.list();
	}

	@Override
	public VectorDatabaseTypeEnum getVectorDatabaseType() {
		return VectorDatabaseTypeEnum.FAISS;
	}
	
	private void checkSocketStatus() {
		if(this.socketClient == null || !this.socketClient.isConnected()) {
			this.startServer();
		}
	}
	
	@Override
	public void close() {
		if (this.socketClient != null && this.socketClient.isConnected()) {
			this.socketClient.stopPyServe(this.pyDirectoryBasePath);
			this.socketClient.disconnect();
			this.socketClient.setConnected(false);
			this.pyDirectoryBasePath = null;
		}
		if(this.p != null && this.p.isAlive()) {
			this.p.destroy();
		}
	}
	
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
	
	/**d
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
	
	public static void main(String[] args) throws Exception {
		Properties tempSmss = new Properties();
		tempSmss.put("CONNECTION_URL", "Semoss_Dev/vector/");
		tempSmss.put("VECTOR_TYPE", "FAISS");
		tempSmss.put("INDEX_CLASSES", "default");
		tempSmss.put("ENCODER_TYPE", "huggingface");
		tempSmss.put("ENCODER_NAME", "sentence-transformers/paraphrase-mpnet-base-v2");
		
		FaissDatabaseEngine engine = new FaissDatabaseEngine();
		engine.open(tempSmss);
		
		engine.close();
	}
}
