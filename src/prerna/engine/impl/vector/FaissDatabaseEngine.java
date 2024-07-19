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
import java.util.Set;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.FilenameUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.cluster.util.ClusterUtil;
import prerna.cluster.util.CopyFilesToEngineRunner;
import prerna.cluster.util.DeleteFilesFromEngineRunner;
import prerna.ds.py.PyUtils;
import prerna.engine.api.VectorDatabaseTypeEnum;
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
import prerna.util.Utility;
import prerna.util.sql.AbstractSqlQueryUtil;

public class FaissDatabaseEngine extends AbstractVectorDatabaseEngine {

	private static final Logger classLogger = LogManager.getLogger(FaissDatabaseEngine.class);
	
	public static final String VECTOR_SEARCHER_NAME = "VECTOR_SEARCHER_NAME";
	private static final String FAISS_INIT_SCRIPT = "${VECTOR_SEARCHER_NAME} = vector_database.FAISSDatabase(embedder_engine_id = '${EMBEDDER_ENGINE_ID}', tokenizer = cfg_tokenizer, keyword_engine_id = '${KEYWORD_ENGINE_ID}', distance_method = '${DISTANCE_METHOD}')";
	
	private HashMap<String, Boolean> indexClassHasDatasetLoaded = new HashMap<String, Boolean>();
	private String vectorDatabaseSearcher = null;

	@Override
	public void open(Properties smssProp) throws Exception {
		super.open(smssProp);
		
		this.vectorDatabaseSearcher = Utility.getRandomString(6);
		this.smssProp.put(VECTOR_SEARCHER_NAME, this.vectorDatabaseSearcher);
	}
	
	@Override
	protected String[] getServerStartCommands() {
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
		
		return commands;
	}

	@Override
	protected synchronized void startServer(int port) {
		super.startServer(port);
		
		if (this.indexClasses.size() > 0) {
			for (String indexClass : this.indexClasses) {
				StringBuilder checkForEmptyDatabase = new StringBuilder(this.vectorDatabaseSearcher)
					.append(".searchers['")
					.append(indexClass)
					.append("']")
					.append(".datasetsLoaded()");
				
				boolean datasetsLoaded = (boolean) pyt.runScript(checkForEmptyDatabase.toString());
				this.indexClassHasDatasetLoaded.put(indexClass, datasetsLoaded);
			}
		}
	}
	
	protected void addIndexClass(String indexClass) {
		this.indexClasses.add(indexClass);
		//TODO: do we really need base path for this?
		String basePath = this.schemaFolder.getAbsolutePath().replace(FILE_SEPARATOR, DIR_SEPARATOR) + DIR_SEPARATOR + indexClass + DIR_SEPARATOR;
		this.pyt.runScript(this.vectorDatabaseSearcher + ".create_searcher(searcher_name = '"+indexClass+"', base_path = '"+ basePath +"')");
	}
	
	@Override
	protected void cleanUpAddDocument(File indexFilesFolder) {
		// do nothing, we need these files for re-creating the master file index
//		try {
//			FileUtils.forceDelete(indexFilesFolder);
//		} catch (IOException e) {
//			classLogger.error(Constants.STACKTRACE, e);
//		}
	}
	
	@Override
	public void addEmbeddings(List<String> vectorCsvFiles, Insight insight, Map<String, Object> parameters) throws Exception {
		if (!modelPropsLoaded) {
			verifyModelProps();
		}
		
		checkSocketStatus();
		
		String indexClass = this.defaultIndexClass;
		if (parameters.containsKey("indexClass")) {
			indexClass = (String) parameters.get("indexClass");
		}
		if (!this.indexClasses.contains(indexClass)) {
			addIndexClass(indexClass);
		}
		
		File indexDirectory = new File(this.schemaFolder, indexClass);
		File documentDir = new File(indexDirectory, DOCUMENTS_FOLDER_NAME);
		File indexFilesDir = new File(indexDirectory, INDEXED_FOLDER_NAME);
		if(!documentDir.exists()) {
			documentDir.mkdirs();
		}
		if(!indexFilesDir.exists()) {
			indexFilesDir.mkdirs();
		}

		// track files to push to cloud
		Set<String> filesToCopyToCloud = new HashSet<String>();
		
		// check that the vectorCsvFiles are in the current engine folder
		// if not, move them
		for (int i = 0; i < vectorCsvFiles.size(); i++) {
			String vectorCsvFile = vectorCsvFiles.get(i);
			File vectorF = new File(Utility.normalizePath(vectorCsvFile));
			// double check that they are files and not directories
			if (!vectorF.isFile()) {
				continue;
			}
			
			if(!vectorF.getCanonicalPath().contains(documentDir.getCanonicalPath()+FILE_SEPARATOR)) {
				File documentDestinationFile = new File(documentDir, vectorF.getName());
				// check if the destination file exists, and if so, delete it 
				try {
					if (documentDestinationFile.exists()) {
						FileUtils.forceDelete(documentDestinationFile);
					}
					
					//only copy the csv if there is not already a file there with the same name
		             String baseName = FilenameUtils.getBaseName(vectorF.getName());

		             // Check if a file with the same base name but different extension exists
		             boolean fileWithSameBaseNameExists = Arrays.stream(documentDir.listFiles())
		                     .anyMatch(file -> FilenameUtils.getBaseName(file.getName()).equals(baseName));
		             if(!fileWithSameBaseNameExists) {
		            	 FileUtils.copyFileToDirectory(vectorF, documentDir, true);
						// store to move to cloud
						filesToCopyToCloud.add(documentDestinationFile.getAbsolutePath());
		             }

				} catch (IOException e) {
					classLogger.error(Constants.STACKTRACE, e);
					throw new IllegalArgumentException("Unable to remove previously created file for " + documentDestinationFile.getName() + " or move it to the document directory");
				}
			}
			if(!vectorF.getCanonicalPath().contains(indexFilesDir.getCanonicalPath()+FILE_SEPARATOR)) {
				File indexDestinationFile = new File(indexFilesDir, vectorF.getName());
				// check if the destination file exists, and if so, delete it
				try {
					if (indexDestinationFile.exists()) {
						FileUtils.forceDelete(indexDestinationFile);
					}
					FileUtils.copyFileToDirectory(vectorF, indexFilesDir, true);
					
					// store to move to cloud
					filesToCopyToCloud.add(indexDestinationFile.getAbsolutePath());
				} catch (IOException e) {
					classLogger.error(Constants.STACKTRACE, e);
					throw new IllegalArgumentException("Unable to remove previously created file for " + indexDestinationFile.getName() + " or move it to the document directory");
				}
				
				// also update the reference to this folder
				vectorCsvFiles.set(i, indexDestinationFile.getAbsolutePath());
			}
		}
		
		// now clean the paths for python
		{
			List<String> temp = new ArrayList<>(vectorCsvFiles.size());
			for(int i = 0; i < vectorCsvFiles.size(); i++) {
				temp.add(vectorCsvFiles.get(i).replace(FILE_SEPARATOR, DIR_SEPARATOR));
			}
			vectorCsvFiles = temp;
		}
		
		// assuming only content to index now
		// yes... the python code is more flexible and allows you to concat multiple values in the csv to encode
		String columnsToIndex = "['Content']"; 
		
		// create dataset
		StringBuilder addDocumentPyCommand = new StringBuilder();
		
		// get the relevant FAISS searcher object in python
		addDocumentPyCommand.append(vectorDatabaseSearcher)
							.append(".searchers['")
							.append(indexClass)
							.append("']");
		
		addDocumentPyCommand.append(".addDocumet(documentFileLocation = ['")
							.append(String.join("','", vectorCsvFiles))
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
			// this should already be handled, but just in case...
			filesToCopyToCloud.addAll(vectorCsvFiles);
			// and the return files (dataset/vector)
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
	
	@Override
	public void addEmbeddings(String vectorCsvFile, Insight insight, Map<String, Object> parameters) throws Exception {
		List<String> vectorCsvFiles = new ArrayList<>(1);
		vectorCsvFiles.add(vectorCsvFile);
		addEmbeddings(vectorCsvFiles, insight, parameters);
	}
	
	@Override
	public void addEmbeddingFiles(List<File> vectorCsvFiles, Insight insight, Map<String, Object> parameters) throws Exception {
		List<String> vectorCsvFilePaths = new ArrayList<>(vectorCsvFiles.size());
		for(int i = 0; i < vectorCsvFiles.size(); i++) {
			vectorCsvFilePaths.add(vectorCsvFiles.get(i).getAbsolutePath());
		}
		addEmbeddings(vectorCsvFilePaths, insight, parameters);
	}
	
	@Override
	public void addEmbeddingFile(File vectorCsvFile, Insight insight, Map<String, Object> parameters) throws Exception {
		List<String> vectorCsvFiles = new ArrayList<>(1);
		vectorCsvFiles.add(vectorCsvFile.getAbsolutePath());
		addEmbeddings(vectorCsvFiles, insight, parameters);
	}

	@Override
	public void addEmbeddings(VectorDatabaseCSVTable vectorCsvTable, Insight insight, Map<String, Object> parameters) throws Exception {
		// TODO Auto-generated method stub
		// TODO Auto-generated method stub
		// TODO Auto-generated method stub
		
		// to implement
		// write the vector csv table out to a file
		// and call the addEmbeddingFiles method
		
		throw new IllegalArgumentException("This method is not yet implemented for this engine");
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
	public List<Map<String, Object>> nearestNeighborCall(Insight insight, String question, Number limit, Map <String, Object> parameters) {
		if (insight == null) {
			throw new IllegalArgumentException("Insight must be provided to run Model Engine Encoder");
		}
		
		checkSocketStatus();
		String indexClass = this.defaultIndexClass;
		insight.getVarStore().put(LATEST_VECTOR_SEARCH_STATEMENT, new NounMetadata(question, PixelDataType.CONST_STRING));
		
		StringBuilder callMaker = new StringBuilder();
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

	////////////////////////////////////////////////////////////////////////
	////////////////////////////////////////////////////////////////////////
	////////////////////////////////////////////////////////////////////////
	////////////////////////////////////////////////////////////////////////
	////////////////////////////////////////////////////////////////////////
	////////////////////////////////////////////////////////////////////////

	
	/**
	 * Everything below is around filtering the faiss database
	 */
	
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
