package prerna.engine.impl.vector;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FilenameFilter;
import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
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
import prerna.engine.impl.vector.metadata.FaissDatabaseMetadataCSVRow;
import prerna.om.Insight;
import prerna.query.querystruct.filters.AndQueryFilter;
import prerna.query.querystruct.filters.IQueryFilter;
import prerna.query.querystruct.filters.OrQueryFilter;
import prerna.query.querystruct.filters.SimpleQueryFilter;
import prerna.query.querystruct.filters.SimpleQueryFilter.FILTER_TYPE;
import prerna.query.querystruct.selectors.IQuerySelector;
import prerna.reactor.vector.VectorDatabaseParamOptionsEnum;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Constants;
import prerna.util.Utility;

public class FaissDatabaseEngine extends AbstractVectorDatabaseEngine {

	private static final Logger classLogger = LogManager.getLogger(FaissDatabaseEngine.class);

	private String vectorDatabaseSearcher = null;

	@Override
	public void open(Properties smssProp) throws Exception {
		super.open(smssProp);
		this.vectorDatabaseSearcher = Utility.getRandomString(6);
	}

	@Override
	protected String[] getServerStartCommands() {
		String faissInitScript = this.vectorDatabaseSearcher + "=vector_database.FAISSDatabase("
				+ "embedder_engine_id = '${EMBEDDER_ENGINE_ID}', " + "tokenizer = cfg_tokenizer, "
				+ "keyword_engine_id = '${KEYWORD_ENGINE_ID}', " + "distance_method = '${DISTANCE_METHOD}')";
		String[] commands = (TOKENIZER_INIT_SCRIPT + faissInitScript).split(PyUtils.PY_COMMAND_SEPARATOR);

		if (this.indexClasses.size() > 0) {
			ArrayList<String> modifiedCommands = new ArrayList<>(Arrays.asList(commands));
			for (String indexClass : this.indexClasses) {
				File fileToCheck = new File(this.schemaFolder.getAbsolutePath() + FILE_SEPARATOR + indexClass,
						"dataset.pkl");
				modifiedCommands.add(this.vectorDatabaseSearcher + ".create_searcher(searcher_name = '" + indexClass
						+ "', base_path = '" + fileToCheck.getParent().replace("\\", FILE_SEPARATOR) + FILE_SEPARATOR
						+ "')");
				if (fileToCheck.exists()) {
					modifiedCommands.add(this.vectorDatabaseSearcher + ".searchers['" + indexClass + "'].load_dataset('"
							+ fileToCheck.getParent().replace("\\", FILE_SEPARATOR) + FILE_SEPARATOR
							+ "' + 'dataset.pkl')");
					modifiedCommands.add(this.vectorDatabaseSearcher + ".searchers['" + indexClass
							+ "'].load_encoded_vectors('" + fileToCheck.getParent().replace("\\", FILE_SEPARATOR)
							+ FILE_SEPARATOR + "' + 'vectors.pkl')");
				}
			}
			commands = modifiedCommands.stream().toArray(String[]::new);
		}
		return commands;
	}

	@Override
	protected String getDefaultDistanceMethod() {
		return "Cosine Similarity";
	}

	@Override
	protected void addIndexClass(String indexClass) {
		if (!modelPropsLoaded) {
			verifyModelProps();
		}
		checkSocketStatus();
		this.indexClasses.add(indexClass);
		String basePath = this.schemaFolder.getAbsolutePath().replace("\\", FILE_SEPARATOR) + FILE_SEPARATOR
				+ indexClass + FILE_SEPARATOR;
		this.pyTranslator.runScript(this.vectorDatabaseSearcher + ".create_searcher(searcher_name = '" + indexClass
				+ "', base_path = '" + basePath + "')");
	}

	@Override
	protected void cleanUpAddDocument(File indexFilesFolder) {
		// purposely left blank for FAISS
	}

	/*** ----- Core Embeddings & Metadata ----- ***/
	public List<FileEmbeddingStatus> addFaissEmbeddings(List<String> vectorCsvFiles, Insight insight,
			Map<String, Object> parameters, List<FaissDatabaseMetadataCSVRow> metadataRows) throws Exception {

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

		if (!documentDir.exists()) {
			documentDir.mkdirs();
		}
		if (!indexFilesDir.exists()) {
			indexFilesDir.mkdirs();
		}

		Set<String> filesToCopyToCloud = new HashSet<>();
		Map<String, Integer> fileRecordCountMap = new HashMap<>();

		// Move files into place if needed
		for (int i = 0; i < vectorCsvFiles.size(); i++) {
			String vectorCsvFile = vectorCsvFiles.get(i);
			File vectorF = new File(Utility.normalizePath(vectorCsvFile));
			if (!vectorF.isFile()) {
				continue;
			}

			int recordCount = countLines(vectorF);
			fileRecordCountMap.put(vectorF.getName(), recordCount);

			Path vectorFPath = vectorF.toPath().toAbsolutePath().normalize();
			Path documentDirPath = documentDir.toPath().toAbsolutePath().normalize();
			Path indexFilesDirPath = indexFilesDir.toPath().toAbsolutePath().normalize();

			if (!vectorFPath.startsWith(documentDirPath)) {
				File documentDestinationFile = new File(documentDir, vectorF.getName());
				try {
					if (documentDestinationFile.exists()) {
						FileUtils.forceDelete(documentDestinationFile);
					}
					String baseName = FilenameUtils.getBaseName(vectorF.getName());
					boolean fileWithSameBaseNameExists = Arrays.stream(documentDir.listFiles())
							.anyMatch(file -> FilenameUtils.getBaseName(file.getName()).equals(baseName));
					if (!fileWithSameBaseNameExists) {
						FileUtils.copyFileToDirectory(vectorF, documentDir, true);
						filesToCopyToCloud.add(documentDestinationFile.getAbsolutePath());
					}
				} catch (IOException e) {
					classLogger.error(Constants.STACKTRACE, e);
					throw new IllegalArgumentException("Unable to move file: " + documentDestinationFile.getName());
				}
			}

			if (!vectorFPath.startsWith(indexFilesDirPath)) {
				File indexDestinationFile = new File(indexFilesDir, vectorF.getName());
				try {
					if (indexDestinationFile.exists()) {
						FileUtils.forceDelete(indexDestinationFile);
					}
					FileUtils.copyFileToDirectory(vectorF, indexFilesDir, true);
					filesToCopyToCloud.add(indexDestinationFile.getAbsolutePath());
				} catch (IOException e) {
					classLogger.error(Constants.STACKTRACE, e);
					throw new IllegalArgumentException("Unable to move file: " + indexDestinationFile.getName());
				}
				vectorCsvFiles.set(i, indexDestinationFile.getAbsolutePath());
			}
		}

		addMetadata(metadataRows, indexClass);

		List<String> temp = new ArrayList<>(vectorCsvFiles.size());
		for (String path : vectorCsvFiles) {
			temp.add(path.replace("\\", FILE_SEPARATOR));
		}
		vectorCsvFiles = temp;

		String columnsToIndex = "['Content']";

		// Provide all IDs for this batch
		FaissDatabaseCSVTable faissTable = FaissDatabaseCSVTable.initCSVTable(new File(vectorCsvFiles.get(0)));
		List<String> rowIds = new ArrayList<>();
		for (FaissDatabaseCSVRow row : faissTable.getRows()) {
			rowIds.add(row.getId());
		}

		// Prepare Python command with record_ids list
		StringBuilder addDocumentPyCommand = new StringBuilder();
		addDocumentPyCommand.append(vectorDatabaseSearcher).append(".searchers['").append(indexClass).append("']");
		addDocumentPyCommand.append(".addDocument(documentFileLocation = ['").append(String.join("','", vectorCsvFiles))
				.append("']").append(", record_ids = [").append("'").append(String.join("','", rowIds)).append("'")
				.append("]").append(", insight_id = '").append(insight.getInsightId()).append("'")
				.append(", columns_to_index = ").append(columnsToIndex);

		if (parameters.containsKey(VectorDatabaseParamOptionsEnum.COLUMNS_TO_REMOVE.getKey())) {
			addDocumentPyCommand.append(", columns_to_remove = ").append(PyUtils
					.determineStringType(parameters.get(VectorDatabaseParamOptionsEnum.COLUMNS_TO_REMOVE.getKey())));
		}
		if (parameters.containsKey(VectorDatabaseParamOptionsEnum.KEYWORD_SEARCH_PARAM.getKey())) {
			addDocumentPyCommand.append(", keyword_search_params = ").append(PyUtils
					.determineStringType(parameters.get(VectorDatabaseParamOptionsEnum.KEYWORD_SEARCH_PARAM.getKey())));
		}
		addDocumentPyCommand.append(")");

		String script = addDocumentPyCommand.toString();
		classLogger.info("Running >>>" + script);
		Map<String, Object> pythonResponseAfterCreatingFiles = (Map<String, Object>) this.pyTranslator
				.runDirectPy(insight, script);

		if (ClusterUtil.IS_CLUSTER) {
			filesToCopyToCloud.addAll(vectorCsvFiles);
			filesToCopyToCloud.addAll((List<String>) pythonResponseAfterCreatingFiles.get("createdDocuments"));
			Thread copyFilesToCloudThread = new Thread(new CopyFilesToEngineRunner(engineId, this.getCatalogType(),
					filesToCopyToCloud.stream().toArray(String[]::new)));
			copyFilesToCloudThread.start();
		}

		// verify the index class loaded the dataset
		StringBuilder checkForEmptyDatabase = new StringBuilder();
		checkForEmptyDatabase.append(this.vectorDatabaseSearcher).append(".searchers['").append(indexClass).append("']")
				.append(".datasetsLoaded()");
		boolean datasetsLoaded = (boolean) pyTranslator.runDirectPy(insight, checkForEmptyDatabase.toString());

		List<FileEmbeddingStatus> fileStatusList = new ArrayList<>();
		for (Map.Entry<String, Integer> entry : fileRecordCountMap.entrySet()) {
			String file = entry.getKey();
			int totalRecords = entry.getValue();
			long inserted = datasetsLoaded ? totalRecords : 0;
			long failed = datasetsLoaded ? 0 : totalRecords;
			String status = datasetsLoaded ? "SUCCESS" : "FAILED";
			fileStatusList.add(new FileEmbeddingStatus(file, status, inserted, failed, totalRecords));
		}
		return fileStatusList;
	}

	public void addMetadata(List<FaissDatabaseMetadataCSVRow> metadataRows, String indexClass) {
		Map<String, Map<String, Object>> metadataMap = new HashMap<>();
		for (FaissDatabaseMetadataCSVRow row : metadataRows) {
			Map<String, Object> meta = new HashMap<>();
			meta.put("id", row.getId());
			meta.put("source", row.getSource());
			meta.put("attribute", row.getAttribute());
			meta.put("strValue", row.getStrValue());
			meta.put("intValue", row.getIntValue());
			meta.put("numValue", row.getNumValue());
			meta.put("boolValue", row.getBoolValue());
			meta.put("dateValue", row.getDateValue());
			meta.put("timestampValue", row.getTimestampValue());
			metadataMap.put(row.getId(), meta);
		}
		String pyCommand = vectorDatabaseSearcher + ".add_metadata('" + indexClass + "', "
				+ PyUtils.toPyDict(metadataMap) + ")";
		pyTranslator.runScript(pyCommand);
	}

	private int countLines(File file) {
		int lines = 0;
		try (BufferedReader reader = new BufferedReader(new FileReader(file))) {
			String line;
			boolean isFirstLine = true;
			while ((line = reader.readLine()) != null) {
				if (isFirstLine) {
					isFirstLine = false;
					continue;
				}
				if (!line.trim().isEmpty()) {
					lines++;
				}
			}
		} catch (IOException e) {
			classLogger.error("Error reading file for line count: {}", file.getName(), e);
		}
		return lines;
	}

	/* ---- Other methods from the parent hierarchy and your engine ---- */
	@Override
	public List<FileEmbeddingStatus> addEmbeddings(String vectorCsvFile, Insight insight,
			Map<String, Object> parameters) throws Exception {
		List<String> vectorCsvFiles = new ArrayList<>(1);
		vectorCsvFiles.add(vectorCsvFile);
		return addEmbeddings(vectorCsvFiles, insight, parameters, Collections.emptyList());
	}

	@Override
	public List<FileEmbeddingStatus> addEmbeddingFiles(List<File> vectorCsvFiles, Insight insight,
			Map<String, Object> parameters) throws Exception {
		List<String> vectorCsvFilePaths = new ArrayList<>(vectorCsvFiles.size());
		for (File file : vectorCsvFiles) {
			vectorCsvFilePaths.add(file.getAbsolutePath());
		}
		return addEmbeddings(vectorCsvFilePaths, insight, parameters, Collections.emptyList());
	}

	@Override
	public List<FileEmbeddingStatus> addEmbeddingFile(File vectorCsvFile, Insight insight,
			Map<String, Object> parameters) throws Exception {
		List<String> vectorCsvFiles = new ArrayList<>(1);
		vectorCsvFiles.add(vectorCsvFile.getAbsolutePath());
		return addEmbeddings(vectorCsvFiles, insight, parameters, Collections.emptyList());
	}

	@Override
	public List<FileEmbeddingStatus> addEmbeddings(VectorDatabaseCSVTable vectorCsvTable, Insight insight,
			Map<String, Object> parameters) throws Exception {
		List<String> vectorCsvFilePaths = new ArrayList<>(1);
		vectorCsvFilePaths.add(vectorCsvTable.getFile().getAbsolutePath());
		return addFaissEmbeddings(vectorCsvFilePaths, insight, parameters, Collections.emptyList());
	}

	// Remove document logic (unchanged, uses standard source col logic)
	@Override
	public void removeDocument(List<String> fileNames, Map<String, Object> parameters) throws IOException {
		String indexClass = this.defaultIndexClass;
		if (parameters.containsKey(INDEX_CLASS)) {
			indexClass = (String) parameters.get(INDEX_CLASS);
		}

		if (!this.indexClasses.contains(indexClass)) {
			throw new IllegalArgumentException("Unable to remove documents from a directory that does not exist");
		}

		checkSocketStatus();

		List<String> filesToRemoveFromCloud = new ArrayList<String>();
		String indexedFilesPath = this.schemaFolder.getAbsolutePath() + FILE_SEPARATOR + indexClass + FILE_SEPARATOR
				+ "indexed_files";
		Path indexDirectory = Paths.get(indexedFilesPath);
		DirectoryStream<Path> stream = null;
		try {
			List<String> sourceNames = new ArrayList<>();
			for (String document : fileNames) {
				String documentName = FilenameUtils.getName(document);
				File f = new File(document);
				if (f.exists() && f.getName().endsWith(".csv")) {
					sourceNames.addAll(VectorDatabaseCSVTable.pullSourceColumn(f));
				} else {
					sourceNames.add(documentName);
				}
			}

			for (String document : sourceNames) {
				String documentName = FilenameUtils.getName(document);
				String[] fileNamesToDelete = { documentName + "_dataset.pkl", documentName + "_vectors.pkl",
						documentName + ".csv" };

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
					File documentFile = new File(this.schemaFolder.getAbsolutePath() + FILE_SEPARATOR + indexClass
							+ FILE_SEPARATOR + "documents", document);
					if (documentFile.exists() && documentFile.isFile()) {
						FileUtils.forceDelete(documentFile);
						filesToRemoveFromCloud.add(documentFile.getAbsolutePath());
					}
				} catch (IOException e) {
					classLogger.error(Constants.STACKTRACE, e);
					throw new IllegalArgumentException("Unable to delete " + document + "from documents directory");
				}
			}
		} finally {
			if (stream != null) {
				try {
					stream.close();
				} catch (IOException e) {
					classLogger.error(Constants.STACKTRACE, e);
				}
			}
		}
		// this would mean the indexClass is now empty, we should delete it
		File indexedFolder = new File(indexedFilesPath);
		if (indexedFolder.list(new FilenameFilter() {
			@Override
			public boolean accept(File dir, String name) {
				return name.endsWith(".pkl");
			}
		}).length == 0) {
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
			this.pyTranslator
					.runScript(this.vectorDatabaseSearcher + ".delete_searcher(searcher_name = '" + indexClass + "')");
			this.indexClasses.remove(indexClass);
		} else {
			// Regenerate the master "dataset.pkl" and "vectors.pkl" files
			StringBuilder updateMasterFilesCommand = new StringBuilder();
			updateMasterFilesCommand.append(this.vectorDatabaseSearcher).append(".searchers['").append(indexClass)
					.append("']").append(".createMasterFiles(path_to_files = '")
					.append(indexDirectory.getParent().toString().replace("\\", FILE_SEPARATOR)).append("')");

			String script = updateMasterFilesCommand.toString();
			classLogger.info("Running >>>" + script);
			this.pyTranslator.runScript(script);
		}

		if (ClusterUtil.IS_CLUSTER) {
			Thread deleteFilesFromCloudThread = new Thread(new DeleteFilesFromEngineRunner(engineId,
					this.getCatalogType(), filesToRemoveFromCloud.stream().toArray(String[]::new)));
			deleteFilesFromCloudThread.start();
		}
	}

	@SuppressWarnings("unchecked")
	@Override
	public List<Map<String, Object>> nearestNeighborCall(Insight insight, String question, Number limit,
			Map<String, Object> parameters) {
		if (insight == null) {
			throw new IllegalArgumentException("Insight must be provided to run Model Engine Encoder");
		}
		checkSocketStatus();

		String indexClass = this.defaultIndexClass;
		if (parameters.containsKey(INDEX_CLASS)) {
			indexClass = (String) parameters.get(INDEX_CLASS);
		}

		insight.getVarStore().put(LATEST_VECTOR_SEARCH_STATEMENT,
				new NounMetadata(question, PixelDataType.CONST_STRING));

		final String TRIPLE_QUOTE = "\"\"\"";
		StringBuilder callMaker = new StringBuilder();
		if (parameters.containsKey(INDEX_CLASS)) {
			Object indexClassObj = parameters.get(INDEX_CLASS);
			if (indexClassObj instanceof String) {
				indexClass = (String) indexClassObj;
				// make the python method
				callMaker.append(this.vectorDatabaseSearcher).append(".nearestNeighbor(").append("indexClasses=['")
						.append(indexClass).append("'], ");
			} else if (indexClassObj instanceof Collection) {
				indexClass = PyUtils.determineStringType(indexClassObj);
				// make the python method
				callMaker.append(this.vectorDatabaseSearcher).append(".nearestNeighbor(").append("indexClasses=")
						.append(indexClass).append(", ");
			}
		} else {// make the python method
			callMaker.append(this.vectorDatabaseSearcher).append(".nearestNeighbor(").append("indexClasses=['")
					.append(indexClass).append("'], ");
		}

		if (question.startsWith("\"")) {
			question = " " + question;
		}
		if (question.endsWith("\"")) {
			question = question + " ";
		}
		question = question.replace(TRIPLE_QUOTE, "\\\"\\\"\\\"");
		callMaker.append("question=").append(TRIPLE_QUOTE).append(question).append(TRIPLE_QUOTE);
		callMaker.append(", insight_id='").append(insight.getInsightId()).append("'");

		String searchFilters = "None";
		if (parameters.containsKey("filters")) {
// TODO modify so query can come from py world
			List<IQueryFilter> filters = (List<IQueryFilter>) parameters.remove("filters");
			searchFilters = addFilters(filters);

			if (searchFilters.startsWith("\"")) {
				searchFilters = " " + searchFilters;
			}
			if (searchFilters.endsWith("\"")) {
				searchFilters = searchFilters + " ";
			}
			searchFilters = searchFilters.replace(TRIPLE_QUOTE, "\\\"\\\"\\\"");
			callMaker.append(", filter=").append(TRIPLE_QUOTE).append(searchFilters).append(TRIPLE_QUOTE);
		}

// make the limit, i.e. the number of responses we want
		callMaker.append(", ").append("results = ").append(limit);

		if (parameters.containsKey(VectorDatabaseParamOptionsEnum.COLUMNS_TO_RETURN.getKey())) {
			// add the columns based in the vector db query
			callMaker.append(", ").append("columns_to_return").append(" = ").append(PyUtils
					.determineStringType(parameters.get(VectorDatabaseParamOptionsEnum.COLUMNS_TO_RETURN.getKey())));
		}
		if (parameters.containsKey(VectorDatabaseParamOptionsEnum.RETURN_THRESHOLD.getKey())) {
			// add the return_threshold, it should be a long or double value
			Object thresholdValue = parameters.get(VectorDatabaseParamOptionsEnum.RETURN_THRESHOLD.getKey());
			Double returnThreshold;

			if (thresholdValue instanceof Long) {
				Long y = (Long) thresholdValue;
				returnThreshold = y.doubleValue();
			} else if ((thresholdValue instanceof Double)) {
				returnThreshold = (Double) thresholdValue;
			} else {
				throw new IllegalArgumentException("Please make sure the the return threshold is of type Double");
			}

			callMaker.append(", ").append("return_threshold = ").append(returnThreshold);
		}
		if (parameters.containsKey(VectorDatabaseParamOptionsEnum.ASCENDING.getKey())) {
			// This should be a True or False value
			String trueFalseString = (String) parameters.get(VectorDatabaseParamOptionsEnum.ASCENDING.getKey());
			String pythonTrueFalse = Character.toUpperCase(trueFalseString.charAt(0)) + trueFalseString.substring(1);

			callMaker.append(",").append("ascending = ").append(pythonTrueFalse);
		}

// close the method
		callMaker.append(")");
		classLogger.info("Running >>> " + callMaker.toString());
		List<Map<String, Object>> output = (List<Map<String, Object>>) pyTranslator.runDirectPy(insight,
				callMaker.toString());
		return output;
	}

	private String addFilters(List<IQueryFilter> filters) {
		List<String> filterStatements = new ArrayList<>();

		for (IQueryFilter filter : filters) {
			StringBuilder filterSyntax = processFilter(filter);
			if (filterSyntax != null) {
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

	private StringBuilder processOrQueryFilter(OrQueryFilter filter) {
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

	private StringBuilder processAndQueryFilter(AndQueryFilter filter) {
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

	private StringBuilder processSimpleQueryFilter(SimpleQueryFilter filter) {
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
		} else if(fType == FILTER_TYPE.LAMBDA_TO_COL) {// same logic as above, just switch the order and reverse the comparator if it is numeric
			throw new IllegalArgumentException("Filter of with a Filter Type of LAMBDA_TO_COL are not supported for FAISS vector databases");
		} else if(fType == FILTER_TYPE.VALUE_TO_VALUE) {
			// WHY WOULD YOU DO THIS!!!
			throw new IllegalArgumentException("Filter of with a Filter Type of VALUE_TO_VALUE are not supported for FAISS vector databases");
		}
		return null;
	}

	private StringBuilder addSelectorToValuesFilter(NounMetadata leftComp, NounMetadata rightComp,
			String thisComparator) {
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

		filterBuilder.append(PyUtils.determineStringType(rightComp.getValue()));

		if (needToClose) {
			filterBuilder.append(")");
		}

		return filterBuilder;
	}



	private StringBuilder addSelectorToSelectorFilter(NounMetadata leftComp, NounMetadata rightComp,
			String thisComparator) {
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

	@Override
	public List<Map<String, Object>> listDocuments(Map<String, Object> parameters) {
		String indexClass = this.defaultIndexClass;
		if (parameters.containsKey(INDEX_CLASS)) {
			indexClass = (String) parameters.get(INDEX_CLASS);
		}

		checkSocketStatus();

		StringBuilder listDocumentsCommand = new StringBuilder();
		listDocumentsCommand.append(this.vectorDatabaseSearcher).append(".searchers['").append(indexClass).append("']")
				.append(".list_documents()  if ").append(this.vectorDatabaseSearcher).append(".searcher_exists('")
				.append(indexClass).append("') else []");
		List<String> sources = (List<String>) pyTranslator.runDirectPy(listDocumentsCommand.toString());

		List<Map<String, Object>> fileList = new ArrayList<>();
		File documentsDir = new File(this.schemaFolder.getAbsolutePath() + FILE_SEPARATOR + indexClass + FILE_SEPARATOR
				+ AbstractVectorDatabaseEngine.DOCUMENTS_FOLDER_NAME);
		if (documentsDir.exists() && documentsDir.isDirectory()) {
			for (String fileName : sources) {
				Map<String, Object> fileInfo = new HashMap<>();
				fileInfo.put("fileName", fileName);
				File thisF = new File(documentsDir, fileName);
				if (thisF.exists() && thisF.isFile()) {
					long fileSizeInBytes = thisF.length();
					double fileSizeInMB = (double) fileSizeInBytes / (1024);
					SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
					String lastModified = dateFormat.format(new Date(thisF.lastModified()));

					// add file size and last modified into the map
					fileInfo.put("fileSize", fileSizeInMB);
					fileInfo.put("lastModified", lastModified);
				}
				fileList.add(fileInfo);
			}
		}
		return fileList;
	}

	@Override
	public List<Map<String, Object>> listAllRecords(Map<String, Object> parameters) {
		checkSocketStatus();

		StringBuilder getAllRecordsCommand = new StringBuilder();
		getAllRecordsCommand.append(this.vectorDatabaseSearcher).append(".list_all_records()");

		List<Map<String, Object>> allRecords = (List<Map<String, Object>>) pyTranslator
				.runDirectPy(getAllRecordsCommand.toString());
		return allRecords;
	}

	@Override
	public VectorDatabaseTypeEnum getVectorDatabaseType() {
		return VectorDatabaseTypeEnum.FAISS;
	}

	// You may re-add other override, filter, and data mutation methods as in your
	// source,
	// with updates for FaissDatabaseCSVTable or FaissDatabaseMetadataCSVTable as
	// needed.
	// (e.g. addFilters, processFilter, etc.)

	// Note: For brevity, most filtering-related methods, searchers, etc. are
	// unchanged
	// and can reference their superclass or remain as in your previous
	// implementation.

	/*
	 * -- You may uncomment the main for dev tests -- public static void
	 * main(String[] args) throws Exception { // Example Properties tempSmss = new
	 * Properties(); tempSmss.put("CONNECTION_URL", "Semoss_Dev/vector/");
	 * tempSmss.put("VECTOR_TYPE", "FAISS"); tempSmss.put("INDEX_CLASSES",
	 * "default"); tempSmss.put("ENCODER_TYPE", "huggingface");
	 * tempSmss.put("ENCODER_NAME",
	 * "sentence-transformers/paraphrase-mpnet-base-v2"); FaissDatabaseEngine engine
	 * = new FaissDatabaseEngine(); engine.open(tempSmss); engine.close(); }
	 */
}
