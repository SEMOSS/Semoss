/*******************************************************************************
 * Copyright 2015 Defense Health Agency (DHA)
 *
 * If your use of this software does not include any GPLv2 components:
 * 	Licensed under the Apache License, Version 2.0 (the "License");
 * 	you may not use this file except in compliance with the License.
 * 	You may obtain a copy of the License at
 *
 * 	  http://www.apache.org/licenses/LICENSE-2.0
 *
 * 	Unless required by applicable law or agreed to in writing, software
 * 	distributed under the License is distributed on an "AS IS" BASIS,
 * 	WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * 	See the License for the specific language governing permissions and
 * 	limitations under the License.
 * ----------------------------------------------------------------------------
 * If your use of this software includes any GPLv2 components:
 * 	This program is free software; you can redistribute it and/or
 * 	modify it under the terms of the GNU General Public License
 * 	as published by the Free Software Foundation; either version 2
 * 	of the License, or (at your option) any later version.
 *
 * 	This program is distributed in the hope that it will be useful,
 * 	but WITHOUT ANY WARRANTY; without even the implied warranty of
 * 	MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * 	GNU General Public License for more details.
 *******************************************************************************/
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

import com.google.gson.Gson;

import prerna.cluster.util.ClusterUtil;
import prerna.cluster.util.CopyFilesToEngineRunner;
import prerna.cluster.util.DeleteFilesFromEngineRunner;
import prerna.ds.py.PyUtils;
import prerna.engine.api.VectorDatabaseTypeEnum;
import prerna.om.Insight;
import prerna.query.querystruct.filters.IQueryFilter;
import prerna.reactor.vector.VectorDatabaseParamOptionsEnum;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Utility;

public class QdrantVectorDatabaseEngine extends AbstractVectorDatabaseEngine {

	private static final Logger classLogger = LogManager.getLogger(QdrantVectorDatabaseEngine.class);

	public static final String QDRANT_QUANTIZATION = "QDRANT_QUANTIZATION";
	public static final String QDRANT_HNSW_M = "QDRANT_HNSW_M";
	public static final String QDRANT_HNSW_EF_CONSTRUCT = "QDRANT_HNSW_EF_CONSTRUCT";
	public static final String QDRANT_ON_DISK_PAYLOAD = "QDRANT_ON_DISK_PAYLOAD";
	public static final String QDRANT_ENABLE_HYBRID_SEARCH = "QDRANT_ENABLE_HYBRID_SEARCH";
	public static final String QDRANT_SPARSE_MODEL = "QDRANT_SPARSE_MODEL";
	public static final String QDRANT_FUSION = "QDRANT_FUSION";
	public static final String QDRANT_INDEXED_FIELDS = "QDRANT_INDEXED_FIELDS";

	public static final String QUANTIZATION_NONE = "none";
	public static final String DEFAULT_SPARSE_MODEL = "Qdrant/bm25";
	public static final String QDRANT_STORAGE_FOLDER_NAME = "qdrant_storage";

	public static final String FILTERS_KEY = "filters";
	public static final String QDRANT_FILTER_KEY = "qdrantFilter";
	public static final String SCORE_THRESHOLD_KEY = "scoreThreshold";
	public static final String POSITIVE_IDS_KEY = "positiveIds";
	public static final String NEGATIVE_IDS_KEY = "negativeIds";

	private static final Gson GSON_LOCAL = new Gson();

	private String vectorDatabaseSearcher = null;
	private String qdrantQuantization = QUANTIZATION_NONE;
	private Integer qdrantHnswM = null;
	private Integer qdrantHnswEfConstruct = null;
	private boolean qdrantOnDiskPayload = false;
	private boolean qdrantHybridSearchEnabled = false;
	private String qdrantSparseModel = DEFAULT_SPARSE_MODEL;
	private String qdrantFusion = "rrf";
	private String qdrantIndexedFieldsJson = null;

	@Override
	public void open(Properties smssProp) throws Exception {
		super.open(smssProp);

		String quant = this.smssProp.getProperty(QDRANT_QUANTIZATION, QUANTIZATION_NONE);
		if (quant == null || (quant = quant.trim()).isEmpty()) {
			quant = QUANTIZATION_NONE;
		}
		this.qdrantQuantization = quant.toLowerCase();

		String hnswM = this.smssProp.getProperty(QDRANT_HNSW_M);
		if (hnswM != null && !(hnswM = hnswM.trim()).isEmpty()) {
			try {
				this.qdrantHnswM = Integer.parseInt(hnswM);
			} catch (NumberFormatException e) {
				classLogger.warn("Invalid QDRANT_HNSW_M value '{}', ignoring", hnswM);
			}
		}

		String hnswEf = this.smssProp.getProperty(QDRANT_HNSW_EF_CONSTRUCT);
		if (hnswEf != null && !(hnswEf = hnswEf.trim()).isEmpty()) {
			try {
				this.qdrantHnswEfConstruct = Integer.parseInt(hnswEf);
			} catch (NumberFormatException e) {
				classLogger.warn("Invalid QDRANT_HNSW_EF_CONSTRUCT value '{}', ignoring", hnswEf);
			}
		}

		this.qdrantOnDiskPayload = Boolean.parseBoolean(this.smssProp.getProperty(QDRANT_ON_DISK_PAYLOAD, "false"));
		this.qdrantHybridSearchEnabled = Boolean.parseBoolean(
				this.smssProp.getProperty(QDRANT_ENABLE_HYBRID_SEARCH, "false"));

		String sparseModel = this.smssProp.getProperty(QDRANT_SPARSE_MODEL, DEFAULT_SPARSE_MODEL);
		if (sparseModel == null || (sparseModel = sparseModel.trim()).isEmpty()) {
			sparseModel = DEFAULT_SPARSE_MODEL;
		}
		this.qdrantSparseModel = sparseModel;

		String fusion = this.smssProp.getProperty(QDRANT_FUSION, "rrf");
		if (fusion == null || (fusion = fusion.trim()).isEmpty()) {
			fusion = "rrf";
		}
		this.qdrantFusion = fusion.toLowerCase();

		String indexedFields = this.smssProp.getProperty(QDRANT_INDEXED_FIELDS);
		this.qdrantIndexedFieldsJson = (indexedFields != null && !indexedFields.trim().isEmpty())
				? indexedFields.trim() : null;

		if (this.vectorDatabaseSearcher == null
				|| (this.vectorDatabaseSearcher = this.vectorDatabaseSearcher.trim()).isEmpty()) {
			this.vectorDatabaseSearcher = Utility.getRandomString(6);
		}
	}

	@Override
	public void close() throws IOException {
		this.vectorDatabaseSearcher = null;
		super.close();
	}

	@Override
	protected String getDefaultDistanceMethod() {
		return "Cosine Similarity";
	}

	@Override
	protected String[] getServerStartCommands() {
		String locationLiteral = resolveStoragePathLiteral();
		String quantLiteral = PyUtils.determineStringType(this.qdrantQuantization);
		String hnswMLiteral = this.qdrantHnswM != null ? this.qdrantHnswM.toString() : "None";
		String hnswEfLiteral = this.qdrantHnswEfConstruct != null ? this.qdrantHnswEfConstruct.toString() : "None";
		String onDiskPayloadLiteral = this.qdrantOnDiskPayload ? "True" : "False";

		String hybridLiteral = this.qdrantHybridSearchEnabled ? "True" : "False";
		String sparseModelLiteral = PyUtils.determineStringType(this.qdrantSparseModel);
		String fusionLiteral = PyUtils.determineStringType(this.qdrantFusion);
		String indexedFieldsLiteral = this.qdrantIndexedFieldsJson != null
				? this.qdrantIndexedFieldsJson : "None";

		StringBuilder qdrantInit = new StringBuilder();
		qdrantInit.append(this.vectorDatabaseSearcher).append("=vector_database.QdrantDatabase(")
				.append("embedder_engine_id = '${EMBEDDER_ENGINE_ID}'")
				.append(", tokenizer = cfg_tokenizer")
				.append(", keyword_engine_id = '${KEYWORD_ENGINE_ID}'")
				.append(", distance_method = '${DISTANCE_METHOD}'")
				.append(", storage_path = ").append(locationLiteral)
				.append(", quantization = ").append(quantLiteral)
				.append(", hnsw_m = ").append(hnswMLiteral)
				.append(", hnsw_ef_construct = ").append(hnswEfLiteral)
				.append(", on_disk_payload = ").append(onDiskPayloadLiteral)
				.append(", enable_hybrid_search = ").append(hybridLiteral)
				.append(", sparse_model_name = ").append(sparseModelLiteral)
				.append(", fusion = ").append(fusionLiteral)
				.append(", indexed_fields = ").append(indexedFieldsLiteral)
				.append(")");

		String[] commands = (TOKENIZER_INIT_SCRIPT + qdrantInit.toString()).split(PyUtils.PY_COMMAND_SEPARATOR);

		if (this.indexClasses.size() > 0) {
			ArrayList<String> modifiedCommands = new ArrayList<>(Arrays.asList(commands));
			for (String indexClass : this.indexClasses) {
				File basePath = new File(this.schemaFolder.getAbsolutePath() + FILE_SEPARATOR + indexClass);
				modifiedCommands.add(this.vectorDatabaseSearcher + ".create_searcher(searcher_name = '" + indexClass
						+ "', base_path = '" + basePath.getAbsolutePath().replace("\\", FILE_SEPARATOR) + FILE_SEPARATOR
						+ "')");
			}
			commands = modifiedCommands.stream().toArray(String[]::new);
		}

		return commands;
	}

	private String resolveStoragePathLiteral() {
		File engineRoot = this.schemaFolder != null ? this.schemaFolder.getParentFile() : null;
		if (engineRoot == null) {
			classLogger.warn("Engine root unresolved at startup, falling back to in-memory Qdrant");
			return "':memory:'";
		}
		File storageDir = new File(engineRoot, QDRANT_STORAGE_FOLDER_NAME);
		if (!storageDir.exists()) {
			storageDir.mkdirs();
		}
		String normalized = storageDir.getAbsolutePath().replace("\\", FILE_SEPARATOR);
		return PyUtils.determineStringType(normalized);
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
		// noop
	}

	@Override
	public List<FileEmbeddingStatus> addEmbeddings(List<String> vectorCsvFiles, Insight insight,
			Map<String, Object> parameters) throws Exception {
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

		Set<String> filesToCopyToCloud = new HashSet<String>();
		Map<String, Integer> fileRecordCountMap = new HashMap<>();

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
					classLogger.error("Failed to copy CSV file '" + vectorF.getAbsolutePath()
							+ "' to vector documents directory '" + documentDir.getAbsolutePath() + "'", e);
					throw new IllegalArgumentException("Unable to remove previously created file for "
							+ documentDestinationFile.getName() + " or move it to the document directory");
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
					classLogger.error("Failed to copy CSV file '" + vectorF.getAbsolutePath()
							+ "' to indexed-files directory '" + indexFilesDir.getAbsolutePath() + "'", e);
					throw new IllegalArgumentException("Unable to remove previously created file for "
							+ indexDestinationFile.getName() + " or move it to the document directory");
				}
				vectorCsvFiles.set(i, indexDestinationFile.getAbsolutePath());
			}
		}

		{
			List<String> temp = new ArrayList<>(vectorCsvFiles.size());
			for (int i = 0; i < vectorCsvFiles.size(); i++) {
				temp.add(vectorCsvFiles.get(i).replace("\\", FILE_SEPARATOR));
			}
			vectorCsvFiles = temp;
		}

		String columnsToIndex = "['Content']";

		StringBuilder addDocumentPyCommand = new StringBuilder();
		addDocumentPyCommand.append(vectorDatabaseSearcher).append(".searchers['").append(indexClass).append("']");
		addDocumentPyCommand.append(".addDocument(documentFileLocation = ['").append(String.join("','", vectorCsvFiles))
				.append("'], insight_id = '").append(insight.getInsightId()).append("', columns_to_index = ")
				.append(columnsToIndex);

		if (parameters.containsKey(VectorDatabaseParamOptionsEnum.COLUMNS_TO_REMOVE.getKey())) {
			addDocumentPyCommand.append(", ").append("columns_to_remove").append(" = ").append(PyUtils
					.determineStringType(parameters.get(VectorDatabaseParamOptionsEnum.COLUMNS_TO_REMOVE.getKey())));
		}

		addDocumentPyCommand.append(")");

		String script = addDocumentPyCommand.toString();
		classLogger.info("Running >>> " + script);
		@SuppressWarnings("unchecked")
		Map<String, Object> pythonResponseAfterCreatingFiles = (Map<String, Object>) this.pyTranslator
				.runDirectPy(insight, script);

		if (ClusterUtil.IS_CLUSTER) {
			filesToCopyToCloud.addAll(vectorCsvFiles);
			if (pythonResponseAfterCreatingFiles != null
					&& pythonResponseAfterCreatingFiles.get("createdDocuments") != null) {
				@SuppressWarnings("unchecked")
				List<String> created = (List<String>) pythonResponseAfterCreatingFiles.get("createdDocuments");
				filesToCopyToCloud.addAll(created);
			}
			Thread.ofVirtual().start(new CopyFilesToEngineRunner(engineId, this.getCatalogType(),
					filesToCopyToCloud.stream().toArray(String[]::new)));
		}

		StringBuilder checkForEmptyDatabase = new StringBuilder();
		checkForEmptyDatabase.append(this.vectorDatabaseSearcher).append(".searchers['").append(indexClass).append("']")
				.append(".datasetsLoaded()");
		boolean datasetsLoaded = (boolean) pyTranslator.runDirectPy(insight, checkForEmptyDatabase.toString());

		List<FileEmbeddingStatus> fileStatusList = new ArrayList<>();
		for (Map.Entry<String, Integer> entry : fileRecordCountMap.entrySet()) {
			String file = entry.getKey();
			int totalRecords = entry.getValue();
			long inserted;
			long failed;
			String status;
			if (datasetsLoaded) {
				inserted = totalRecords;
				failed = 0;
				status = "SUCCESS";
			} else {
				inserted = 0;
				failed = totalRecords;
				status = "FAILED";
			}
			fileStatusList.add(new FileEmbeddingStatus(file, status, inserted, failed, totalRecords));
		}

		return fileStatusList;
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

	@Override
	public List<FileEmbeddingStatus> addEmbeddings(String vectorCsvFile, Insight insight,
			Map<String, Object> parameters) throws Exception {
		List<String> vectorCsvFiles = new ArrayList<>(1);
		vectorCsvFiles.add(vectorCsvFile);
		return addEmbeddings(vectorCsvFiles, insight, parameters);
	}

	@Override
	public List<FileEmbeddingStatus> addEmbeddingFiles(List<File> vectorCsvFiles, Insight insight,
			Map<String, Object> parameters) throws Exception {
		List<String> vectorCsvFilePaths = new ArrayList<>(vectorCsvFiles.size());
		for (int i = 0; i < vectorCsvFiles.size(); i++) {
			vectorCsvFilePaths.add(vectorCsvFiles.get(i).getAbsolutePath());
		}
		return addEmbeddings(vectorCsvFilePaths, insight, parameters);
	}

	@Override
	public List<FileEmbeddingStatus> addEmbeddingFile(File vectorCsvFile, Insight insight,
			Map<String, Object> parameters) throws Exception {
		List<String> vectorCsvFiles = new ArrayList<>(1);
		vectorCsvFiles.add(vectorCsvFile.getAbsolutePath());
		return addEmbeddings(vectorCsvFiles, insight, parameters);
	}

	@Override
	public List<FileEmbeddingStatus> addEmbeddings(VectorDatabaseCSVTable vectorCsvTable, Insight insight,
			Map<String, Object> parameters) throws Exception {
		List<String> vectorCsvFilePaths = new ArrayList<>(1);
		vectorCsvFilePaths.add(vectorCsvTable.getFile().getAbsolutePath());
		return addEmbeddings(vectorCsvFilePaths, insight, parameters);
	}

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
			String[] fileNamesToDelete = { documentName + ".csv" };
			DirectoryStream.Filter<Path> fileNameFilters = entry -> {
				String fileName = entry.getFileName().toString();
				for (String fileNameToDelete : fileNamesToDelete) {
					if (fileName.equals(fileNameToDelete)) {
						return true;
					}
				}
				return false;
			};

			try (DirectoryStream<Path> stream = Files.newDirectoryStream(indexDirectory, fileNameFilters)) {
				for (Path entry : stream) {
					try {
						Files.delete(entry);
						filesToRemoveFromCloud.add(entry.toString());
					} catch (IOException e) {
						classLogger.error("Failed to delete indexed file: " + entry, e);
						throw new IllegalArgumentException("Unable to remove file: " + entry.getFileName());
					}
				}
			} catch (IOException e) {
				classLogger.error("Failed to list indexed files in directory: " + indexDirectory, e);
				throw new IllegalArgumentException("Unable determine files in " + indexDirectory.getFileName());
			}

			try {
				File documentFile = new File(this.schemaFolder.getAbsolutePath() + FILE_SEPARATOR + indexClass
						+ FILE_SEPARATOR + "documents", document);
				if (documentFile.exists() && documentFile.isFile()) {
					FileUtils.forceDelete(documentFile);
					filesToRemoveFromCloud.add(documentFile.getAbsolutePath());
				}
			} catch (IOException e) {
				classLogger.error("Failed to delete document '" + document
						+ "' from documents directory for index class: " + indexClass, e);
				throw new IllegalArgumentException("Unable to delete " + document + "from documents directory");
			}

			StringBuilder removeFromQdrant = new StringBuilder();
			removeFromQdrant.append(this.vectorDatabaseSearcher).append(".searchers['").append(indexClass).append("']")
					.append(".removeDocument(source = ").append(PyUtils.determineStringType(documentName)).append(")");
			this.pyTranslator.runScript(removeFromQdrant.toString());
		}

		File indexedFolder = new File(indexedFilesPath);
		String[] remaining = indexedFolder.list(new FilenameFilter() {
			@Override
			public boolean accept(File dir, String name) {
				return name.endsWith(".csv");
			}
		});
		if (remaining == null || remaining.length == 0) {
			try {
				File indexClassDirectory = new File(indexedFolder.getParent());
				FileUtils.forceDelete(indexClassDirectory);
			} catch (IOException e) {
				classLogger.error("Failed to delete index class folder for index class: " + indexClass, e);
				throw new IllegalArgumentException("Unable to delete remove the index class folder");
			}
			this.pyTranslator
					.runScript(this.vectorDatabaseSearcher + ".delete_searcher(searcher_name = '" + indexClass + "')");
			this.indexClasses.remove(indexClass);
		}

		if (ClusterUtil.IS_CLUSTER) {
			Thread.ofVirtual().start(new DeleteFilesFromEngineRunner(engineId, this.getCatalogType(),
					filesToRemoveFromCloud.stream().toArray(String[]::new)));
		}
	}

	public void removePointsByIds(String indexClass, List<String> pointIds) {
		checkSocketStatus();
		if (indexClass == null || indexClass.trim().isEmpty()) {
			indexClass = this.defaultIndexClass;
		}
		if (!this.indexClasses.contains(indexClass)) {
			throw new IllegalArgumentException("Index class does not exist: " + indexClass);
		}
		StringBuilder script = new StringBuilder();
		script.append(this.vectorDatabaseSearcher).append(".searchers['").append(indexClass).append("']")
				.append(".removePoints(point_ids = ").append(PyUtils.determineStringType(pointIds)).append(")");
		this.pyTranslator.runScript(script.toString());
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
		insight.getVarStore().put(LATEST_VECTOR_SEARCH_STATEMENT,
				new NounMetadata(question, PixelDataType.CONST_STRING));

		final String TRIPLE_QUOTE = "\"\"\"";

		StringBuilder callMaker = new StringBuilder();
		if (parameters.containsKey(INDEX_CLASS)) {
			Object indexClassObj = parameters.get(INDEX_CLASS);
			if (indexClassObj instanceof String) {
				indexClass = (String) indexClassObj;
				callMaker.append(this.vectorDatabaseSearcher).append(".nearestNeighbor(").append("indexClasses=['")
						.append(indexClass).append("'], ");
			} else if (indexClassObj instanceof Collection) {
				indexClass = PyUtils.determineStringType(indexClassObj);
				callMaker.append(this.vectorDatabaseSearcher).append(".nearestNeighbor(").append("indexClasses=")
						.append(indexClass).append(", ");
			}
		} else {
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

		String qdrantFilterLiteral = resolveQdrantFilterLiteral(parameters);
		if (qdrantFilterLiteral != null) {
			callMaker.append(", qdrant_filter=").append(qdrantFilterLiteral);
		}

		callMaker.append(", ").append("limit = ").append(limit);

		if (parameters.containsKey(VectorDatabaseParamOptionsEnum.COLUMNS_TO_RETURN.getKey())) {
			callMaker.append(", ").append("columns_to_return").append(" = ").append(PyUtils
					.determineStringType(parameters.get(VectorDatabaseParamOptionsEnum.COLUMNS_TO_RETURN.getKey())));
		}

		Double scoreThreshold = extractScoreThreshold(parameters);
		if (scoreThreshold != null) {
			callMaker.append(", ").append("score_threshold = ").append(scoreThreshold);
		}

		Boolean useHybrid = extractUseHybridSearch(parameters);
		if (useHybrid != null) {
			callMaker.append(", ").append("use_hybrid_search = ").append(useHybrid ? "True" : "False");
		}

		callMaker.append(")");
		classLogger.info("Running >>> " + callMaker.toString());
		List<Map<String, Object>> output = (List<Map<String, Object>>) pyTranslator.runDirectPy(insight,
				callMaker.toString());
		return output;
	}

	private Boolean extractUseHybridSearch(Map<String, Object> parameters) {
		if (parameters == null) {
			return null;
		}
		Object raw = parameters.get(VectorDatabaseParamOptionsEnum.USE_HYBRID_SEARCH.getKey());
		if (raw == null) {
			return null;
		}
		if (raw instanceof Boolean) {
			return (Boolean) raw;
		}
		String s = raw.toString().trim().toLowerCase();
		if (s.equals("true") || s.equals("1") || s.equals("yes")) {
			return Boolean.TRUE;
		}
		if (s.equals("false") || s.equals("0") || s.equals("no")) {
			return Boolean.FALSE;
		}
		return null;
	}

	@SuppressWarnings("unchecked")
	public List<Map<String, Object>> recommend(Insight insight, List<String> positiveIds, List<String> negativeIds,
			Number limit, Map<String, Object> parameters) {
		if (insight == null) {
			throw new IllegalArgumentException("Insight must be provided");
		}
		if (positiveIds == null || positiveIds.isEmpty()) {
			throw new IllegalArgumentException("At least one positive id is required for recommend");
		}
		checkSocketStatus();

		String indexClass = this.defaultIndexClass;
		if (parameters != null && parameters.containsKey(INDEX_CLASS)) {
			indexClass = (String) parameters.get(INDEX_CLASS);
		}

		StringBuilder script = new StringBuilder();
		script.append(this.vectorDatabaseSearcher).append(".searchers['").append(indexClass).append("']")
				.append(".recommend(positive_ids = ").append(PyUtils.determineStringType(positiveIds))
				.append(", negative_ids = ")
				.append(PyUtils.determineStringType(negativeIds != null ? negativeIds : new ArrayList<String>()))
				.append(", limit = ").append(limit);

		String qdrantFilterLiteral = resolveQdrantFilterLiteral(parameters);
		if (qdrantFilterLiteral != null) {
			script.append(", qdrant_filter=").append(qdrantFilterLiteral);
		}

		Double scoreThreshold = extractScoreThreshold(parameters);
		if (scoreThreshold != null) {
			script.append(", score_threshold = ").append(scoreThreshold);
		}

		script.append(")");
		classLogger.info("Running >>> " + script.toString());
		return (List<Map<String, Object>>) pyTranslator.runDirectPy(insight, script.toString());
	}

	private String resolveQdrantFilterLiteral(Map<String, Object> parameters) {
		if (parameters == null) {
			return null;
		}
		if (parameters.containsKey(QDRANT_FILTER_KEY)) {
			Object raw = parameters.remove(QDRANT_FILTER_KEY);
			if (raw == null) {
				return null;
			}
			if (raw instanceof Map || raw instanceof Collection) {
				return GSON_LOCAL.toJson(raw);
			}
			return PyUtils.determineStringType(raw);
		}
		if (parameters.containsKey(FILTERS_KEY)) {
			@SuppressWarnings("unchecked")
			List<IQueryFilter> filters = (List<IQueryFilter>) parameters.remove(FILTERS_KEY);
			if (filters == null || filters.isEmpty()) {
				return null;
			}
			Map<String, Object> translated = QdrantFilterTranslator.translate(filters);
			if (translated == null || translated.isEmpty()) {
				return null;
			}
			return GSON_LOCAL.toJson(translated);
		}
		return null;
	}

	private Double extractScoreThreshold(Map<String, Object> parameters) {
		if (parameters == null) {
			return null;
		}
		Object raw = null;
		if (parameters.containsKey(SCORE_THRESHOLD_KEY)) {
			raw = parameters.get(SCORE_THRESHOLD_KEY);
		} else if (parameters.containsKey(VectorDatabaseParamOptionsEnum.RETURN_THRESHOLD.getKey())) {
			raw = parameters.get(VectorDatabaseParamOptionsEnum.RETURN_THRESHOLD.getKey());
		}
		if (raw == null) {
			return null;
		}
		if (raw instanceof Number) {
			return ((Number) raw).doubleValue();
		}
		try {
			return Double.parseDouble(raw.toString());
		} catch (NumberFormatException e) {
			return null;
		}
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

		@SuppressWarnings("unchecked")
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
					fileInfo.put("fileSize", fileSizeInMB);
					fileInfo.put("lastModified", lastModified);
				}
				fileList.add(fileInfo);
			}
		}

		return fileList;
	}

	@SuppressWarnings("unchecked")
	@Override
	public List<Map<String, Object>> listAllRecords(Map<String, Object> parameters) {
		checkSocketStatus();
		StringBuilder getAllRecordsCommand = new StringBuilder();
		getAllRecordsCommand.append(this.vectorDatabaseSearcher).append(".list_all_records()");
		return (List<Map<String, Object>>) pyTranslator.runDirectPy(getAllRecordsCommand.toString());
	}

	@Override
	public VectorDatabaseTypeEnum getVectorDatabaseType() {
		return VectorDatabaseTypeEnum.QDRANT;
	}

	public boolean isInMemory() {
		return false;
	}

	public boolean isPersistent() {
		return true;
	}

	public boolean isHybridSearchEnabled() {
		return this.qdrantHybridSearchEnabled;
	}

	public String getSparseModelName() {
		return this.qdrantSparseModel;
	}

	@SuppressWarnings("unchecked")
	public Map<String, Object> addPoints(Insight insight, List<Map<String, Object>> items,
			Map<String, Object> parameters) {
		if (insight == null) {
			throw new IllegalArgumentException("Insight must be provided");
		}
		if (items == null || items.isEmpty()) {
			return Map.of("upserted", 0, "skipped", 0);
		}
		checkSocketStatus();

		String indexClass = this.defaultIndexClass;
		if (parameters != null && parameters.containsKey(INDEX_CLASS)) {
			indexClass = (String) parameters.get(INDEX_CLASS);
		}
		if (!this.indexClasses.contains(indexClass)) {
			addIndexClass(indexClass);
		}

		String itemsJson = GSON_LOCAL.toJson(items);
		StringBuilder script = new StringBuilder();
		script.append(this.vectorDatabaseSearcher).append(".searchers['").append(indexClass).append("']")
				.append(".add_points(items = ").append(PyUtils.determineStringType(itemsJson))
				.append(", insight_id = '").append(insight.getInsightId()).append("'");
		if (parameters != null && parameters.containsKey("batch_size")) {
			script.append(", batch_size = ").append(parameters.get("batch_size"));
		}
		script.append(")");
		classLogger.info("Running >>> " + script);
		return (Map<String, Object>) pyTranslator.runDirectPy(insight, script.toString());
	}

	@SuppressWarnings("unchecked")
	public List<Map<String, Object>> hybridSearch(Insight insight, String question, Number limit,
			Map<String, Object> parameters) {
		if (parameters == null) {
			parameters = new HashMap<>();
		}
		parameters.put(VectorDatabaseParamOptionsEnum.USE_HYBRID_SEARCH.getKey(), Boolean.TRUE);
		return (List<Map<String, Object>>) nearestNeighborCall(insight, question, limit, parameters);
	}

	public int deleteByFilter(Insight insight, Map<String, Object> parameters) {
		if (insight == null) {
			throw new IllegalArgumentException("Insight must be provided");
		}
		checkSocketStatus();

		String indexClass = this.defaultIndexClass;
		if (parameters != null && parameters.containsKey(INDEX_CLASS)) {
			indexClass = (String) parameters.get(INDEX_CLASS);
		}
		String qdrantFilterLiteral = resolveQdrantFilterLiteral(parameters);
		if (qdrantFilterLiteral == null) {
			throw new IllegalArgumentException("A filter is required for deleteByFilter");
		}
		StringBuilder script = new StringBuilder();
		script.append(this.vectorDatabaseSearcher).append(".searchers['").append(indexClass).append("']")
				.append(".delete_by_filter(qdrant_filter = ").append(qdrantFilterLiteral).append(")");
		classLogger.info("Running >>> " + script);
		Object out = pyTranslator.runDirectPy(insight, script.toString());
		if (out instanceof Number) {
			return ((Number) out).intValue();
		}
		return 0;
	}

	@SuppressWarnings("unchecked")
	public Map<String, Object> listPoints(Insight insight, Map<String, Object> parameters) {
		if (insight == null) {
			throw new IllegalArgumentException("Insight must be provided");
		}
		checkSocketStatus();

		String indexClass = this.defaultIndexClass;
		if (parameters != null && parameters.containsKey(INDEX_CLASS)) {
			indexClass = (String) parameters.get(INDEX_CLASS);
		}
		int limit = 100;
		if (parameters != null && parameters.containsKey("limit")) {
			Object raw = parameters.get("limit");
			if (raw instanceof Number) {
				limit = ((Number) raw).intValue();
			} else {
				try {
					limit = Integer.parseInt(raw.toString());
				} catch (NumberFormatException ignored) {
				}
			}
		}
		StringBuilder script = new StringBuilder();
		script.append(this.vectorDatabaseSearcher).append(".searchers['").append(indexClass).append("']")
				.append(".list_points(limit = ").append(limit);
		String qdrantFilterLiteral = resolveQdrantFilterLiteral(parameters);
		if (qdrantFilterLiteral != null) {
			script.append(", qdrant_filter = ").append(qdrantFilterLiteral);
		}
		if (parameters != null && parameters.containsKey("offset")) {
			script.append(", offset = ").append(PyUtils.determineStringType(parameters.get("offset")));
		}
		script.append(")");
		classLogger.info("Running >>> " + script);
		return (Map<String, Object>) pyTranslator.runDirectPy(insight, script.toString());
	}
}
