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

import prerna.cluster.util.ClusterUtil;
import prerna.cluster.util.CopyFilesToEngineRunner;
import prerna.cluster.util.DeleteFilesFromEngineRunner;
import prerna.ds.py.PyUtils;
import prerna.engine.api.VectorDatabaseTypeEnum;
import prerna.engine.impl.vector.metadata.VectorDatabaseMetadataCSVWriter;
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
import prerna.util.Utility;
import prerna.util.sql.AbstractSqlQueryUtil;

public class FaissDatabaseEngine extends AbstractVectorDatabaseEngine {

	private static final Logger classLogger = LogManager.getLogger(FaissDatabaseEngine.class);

	public static final String ENABLE_HYBRID_SEARCH = "ENABLE_HYBRID_SEARCH";

	private String vectorDatabaseSearcher = null;
	private boolean enableHybridSearch = true;

	@Override
	public void open(Properties smssProp) throws Exception {
		super.open(smssProp);
		this.enableHybridSearch = Boolean.parseBoolean(this.smssProp.getProperty(ENABLE_HYBRID_SEARCH, "true"));

		// if we've already opened don't automatically drop the searcher variable
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
	protected String[] getServerStartCommands() {
		String faissInitScript = this.vectorDatabaseSearcher + "=vector_database.FAISSDatabase("
				+ "embedder_engine_id = '${EMBEDDER_ENGINE_ID}', tokenizer = cfg_tokenizer"
				+ ", keyword_engine_id = '${KEYWORD_ENGINE_ID}', distance_method = '${DISTANCE_METHOD}'"
				+ ", enable_hybrid_search=" + PyUtils.determineStringType(this.enableHybridSearch) + ")";
		String[] commands = (TOKENIZER_INIT_SCRIPT + faissInitScript).split(PyUtils.PY_COMMAND_SEPARATOR);

		// need to iterate through and potential spin up tables themselves
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
		// TODO: do we really need base path for this?
		String basePath = this.schemaFolder.getAbsolutePath().replace("\\", FILE_SEPARATOR) + FILE_SEPARATOR
				+ indexClass + FILE_SEPARATOR;
		this.pyTranslator.runScript(this.vectorDatabaseSearcher + ".create_searcher(searcher_name = '" + indexClass
				+ "', base_path = '" + basePath + "')");
	}

	@Override
	protected void cleanUpAddDocument(File indexFilesFolder) {
		// do nothing, we need these files for re-creating the master file index
//		try {
//			FileUtils.forceDelete(indexFilesFolder);
//		} catch (IOException e) {
//			classLogger.error("Failed to clean up indexed files folder: " + indexFilesFolder.getAbsolutePath(), e);
//		}
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

		// track files to push to cloud
		Set<String> filesToCopyToCloud = new HashSet<String>();
		Map<String, Integer> fileRecordCountMap = new HashMap<>();

		// check that the vectorCsvFiles are in the current engine folder
		// if not, move them
		for (int i = 0; i < vectorCsvFiles.size(); i++) {
			String vectorCsvFile = vectorCsvFiles.get(i);
			File vectorF = new File(Utility.normalizePath(vectorCsvFile));
			// double check that they are files and not directories
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
				// check if the destination file exists, and if so, delete it
				try {
					if (documentDestinationFile.exists()) {
						FileUtils.forceDelete(documentDestinationFile);
					}

					// only copy the csv if there is not already a file there with the same name
					String baseName = FilenameUtils.getBaseName(vectorF.getName());

					// Check if a file with the same base name but different extension exists
					boolean fileWithSameBaseNameExists = Arrays.stream(documentDir.listFiles())
							.anyMatch(file -> FilenameUtils.getBaseName(file.getName()).equals(baseName));
					if (!fileWithSameBaseNameExists) {
						FileUtils.copyFileToDirectory(vectorF, documentDir, true);
						// store to move to cloud
						filesToCopyToCloud.add(documentDestinationFile.getAbsolutePath());
					}
				} catch (IOException e) {
					classLogger.error("Failed to copy CSV file '{}' to vector documents directory '{}'",
							vectorF.getAbsolutePath(), documentDir.getAbsolutePath(), e);
					throw new IllegalArgumentException("Unable to remove previously created file for "
							+ documentDestinationFile.getName() + " or move it to the document directory");
				}
			}

			if (!vectorFPath.startsWith(indexFilesDirPath)) {
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
					classLogger.error("Failed to copy CSV file '{}' to indexed-files directory '{}'",
							vectorF.getAbsolutePath(), indexFilesDir.getAbsolutePath(), e);
					throw new IllegalArgumentException("Unable to remove previously created file for "
							+ indexDestinationFile.getName() + " or move it to the document directory");
				}

				// also update the reference to this folder
				vectorCsvFiles.set(i, indexDestinationFile.getAbsolutePath());
			}
		}

		// now clean the paths for python
		{
			List<String> temp = new ArrayList<>(vectorCsvFiles.size());
			for (int i = 0; i < vectorCsvFiles.size(); i++) {
				temp.add(vectorCsvFiles.get(i).replace("\\", FILE_SEPARATOR));
			}
			vectorCsvFiles = temp;
		}

		// assuming only content to index now
		// yes... the python code is more flexible and allows you to concat multiple
		// values in the csv to encode
		String columnsToIndex = "['Content']";

		// create dataset
		StringBuilder addDocumentPyCommand = new StringBuilder();

		// get the relevant FAISS searcher object in python
		addDocumentPyCommand.append(vectorDatabaseSearcher).append(".searchers['").append(indexClass).append("']");

		addDocumentPyCommand.append(".addDocument(documentFileLocation = ['").append(String.join("','", vectorCsvFiles))
				.append("'], insight_id = '").append(insight.getInsightId()).append("', columns_to_index = ")
				.append(columnsToIndex);

		if (parameters.containsKey(VectorDatabaseParamOptionsEnum.COLUMNS_TO_REMOVE.getKey())) {
			// add the columns based in the vector db query
			addDocumentPyCommand.append(", ").append("columns_to_remove").append(" = ").append(PyUtils
					.determineStringType(parameters.get(VectorDatabaseParamOptionsEnum.COLUMNS_TO_REMOVE.getKey())));
		}

		if (parameters.containsKey(VectorDatabaseParamOptionsEnum.KEYWORD_SEARCH_PARAM.getKey())) {
			// add the columns based in the vector db query
			addDocumentPyCommand.append(", ").append("keyword_search_params").append(" = ").append(PyUtils
					.determineStringType(parameters.get(VectorDatabaseParamOptionsEnum.KEYWORD_SEARCH_PARAM.getKey())));
		}

		addDocumentPyCommand.append(")");

		String script = addDocumentPyCommand.toString();

		classLogger.info("Running >>> " + script);
		Map<String, Object> pythonResponseAfterCreatingFiles = (Map<String, Object>) this.pyTranslator
				.runDirectPy(insight, script);

		if (ClusterUtil.IS_CLUSTER) {
			// this should already be handled, but just in case...
			filesToCopyToCloud.addAll(vectorCsvFiles);
			// and the return files (dataset/vector)
			filesToCopyToCloud.addAll((List<String>) pythonResponseAfterCreatingFiles.get("createdDocuments"));
			Thread.ofVirtual().start(new CopyFilesToEngineRunner(engineId, this.getCatalogType(),
					filesToCopyToCloud.stream().toArray(String[]::new)));
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

			long inserted = 0;
			long failed = 0;
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
	
	public List<Map<String, String>> addMetadataFaiss(List<String> validFiles, Insight insight, Map<String, Object> parameters) {
	    List<Map<String, String>> metadataStatusList = new ArrayList<>();
	    
	    checkSocketStatus();
	    
	    try {
	        String indexClass = this.defaultIndexClass;
	        File indexFilesDir = new File(this.schemaFolder + FILE_SEPARATOR + indexClass, INDEXED_FOLDER_NAME);
	        if (!indexFilesDir.exists()) {
	            indexFilesDir.mkdirs();
	        }
	 
	        for (String filePath : validFiles) {
	        	File file = new File(Utility.normalizePath(filePath));
	            String baseName = FilenameUtils.getBaseName(file.getName());
	            
	            String metadataFileName = baseName + "_metadata.csv";
	            File metadataFile = new File(indexFilesDir, metadataFileName);
	            
	            // if file already exits, delete it before writing new metadata
	            if(metadataFile.exists()) {
	            	metadataFile.delete();
	            }
	            
	            //Extract metadata map from paramValues
	            if (parameters != null && parameters.containsKey(AbstractVectorDatabaseEngine.METADATA)) {
	            	Map<String, Map<String, Object>> metadata = (Map<String, Map<String, Object>>) parameters
	    					.get(AbstractVectorDatabaseEngine.METADATA);
	            	
	            	// create CSV with columns only, no content
		            VectorDatabaseMetadataCSVWriter writer = new VectorDatabaseMetadataCSVWriter(metadataFile.getAbsolutePath());
					writer.bulkWriteRow(metadata);
					writer.close();
	            }
	            
	            // register metadata
	    		StringBuilder addMetadataPyCommand = new StringBuilder();
	    		addMetadataPyCommand.append(vectorDatabaseSearcher).append(".searchers['").append(indexClass).append("']")
	    				.append(".add_metadata_file('").append(metadataFile.getAbsolutePath().replace("\\", FILE_SEPARATOR)).append("')");
	            
	    		//run the command
	    		pyTranslator.runDirectPy(insight, addMetadataPyCommand.toString());
	    		
	            // return status
	            Map<String, String> fileStatus = new HashMap<>();
	            fileStatus.put("fileName", metadataFileName);
	            fileStatus.put("status", "SUCCESS");
	            metadataStatusList.add(fileStatus);
	        }
	 
	    } catch (Exception e) {
	        classLogger.error("Failed to create metadata CSV for Faiss: ", e);
	        throw new IllegalArgumentException("Failed to create metadata CSV for Faiss: "+ e.getMessage());
	    }
	 
	    return metadataStatusList;
	}

	/**
	 * 
	 * @param file
	 * @return
	 */
	private int countLines(File file) {
		int lines = 0;
		try (BufferedReader reader = new BufferedReader(new FileReader(file))) {
			String line;
			boolean isFirstLine = true;
			while ((line = reader.readLine()) != null) {
				if (isFirstLine) {
					isFirstLine = false; // Skip header
					continue;
				}
				if (!line.trim().isEmpty()) {
					lines++;
				}
			}
		} catch (IOException e) {
			classLogger.error("Failed to count records in file '{}'", file.getName(), e);
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
			// Include both new safe extensions and the legacy .pkl pair so this method
			// works against partially-migrated engines as well as freshly-written ones.
			String[] fileNamesToDelete = { documentName + "_dataset.parquet", documentName + "_vectors.npy",
					documentName + "_dataset.pkl", documentName + "_vectors.pkl", documentName + ".csv" };

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

			try (DirectoryStream<Path> stream = Files.newDirectoryStream(indexDirectory, fileNameFilters)) {
				for (Path entry : stream) {
					// Delete each file that matches the specified file name
					try {
						Files.delete(entry);
						filesToRemoveFromCloud.add(entry.toString());
					} catch (IOException e) {
						classLogger.error("Failed to delete indexed file '{}'", entry, e);
						throw new IllegalArgumentException("Unable to remove file: " + entry.getFileName());
					}
					classLogger.info("Deleted: " + entry.toString());
				}
			} catch (IllegalArgumentException e) {
				throw e;
			} catch (IOException e) {
				classLogger.error("Failed to list indexed files in directory '{}'", indexDirectory, e);
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
				classLogger.error("Failed to delete document '{}' from documents directory for index class '{}'",
						document, indexClass, e);
				throw new IllegalArgumentException("Unable to delete " + document + "from documents directory");
			}
		}

		// this would mean the indexClass is now empty, we should delete it
		File indexedFolder = new File(indexedFilesPath);
		if (indexedFolder.list(new FilenameFilter() {
			@Override
			public boolean accept(File dir, String name) {
				return name.endsWith(".parquet") || name.endsWith(".npy") || name.endsWith(".pkl");
			}
		}).length == 0) {
			try {
				File indexClassDirectory = new File(indexedFolder.getParent());

				// remove the master dataset and vector files (include legacy .pkl entries for
				// engines whose cloud copies have not yet been migrated)
				filesToRemoveFromCloud.add(new File(indexClassDirectory, "dataset.parquet").getAbsolutePath());
				filesToRemoveFromCloud.add(new File(indexClassDirectory, "vectors.npy").getAbsolutePath());
				filesToRemoveFromCloud.add(new File(indexClassDirectory, "dataset.pkl").getAbsolutePath());
				filesToRemoveFromCloud.add(new File(indexClassDirectory, "vectors.pkl").getAbsolutePath());

				// delete the entire folder
				FileUtils.forceDelete(indexClassDirectory);
			} catch (IOException e) {
				classLogger.error("Failed to delete index class folder for index class '{}'", indexClass, e);
				throw new IllegalArgumentException("Unable to delete remove the index class folder");
			}
			this.pyTranslator
					.runScript(this.vectorDatabaseSearcher + ".delete_searcher(searcher_name = '" + indexClass + "')");
			this.indexClasses.remove(indexClass);
		} else {
			// Regenerate the master dataset/vector files
			StringBuilder updateMasterFilesCommandBuilder = new StringBuilder();
			updateMasterFilesCommandBuilder.append(this.vectorDatabaseSearcher).append(".searchers['")
					.append(indexClass).append("']").append(".createMasterFiles(path_to_files = '")
					.append(indexDirectory.getParent().toString().replace("\\", FILE_SEPARATOR)).append("')");

			String updateFaissMaster = updateMasterFilesCommandBuilder.toString();
			classLogger.info("Running >>> " + updateFaissMaster);

			// also handle bm25 files
			String updateBM25 = null;
			if (this.enableHybridSearch) {
				StringBuilder updateBM25Builder = new StringBuilder();
				updateBM25Builder.append(this.vectorDatabaseSearcher).append(".rebuild_bm25_indexes(indexClasses=['")
						.append(indexClass).append("'])");

				updateBM25 = updateBM25Builder.toString();
				classLogger.info("Running >>> " + updateBM25);
			}
			this.pyTranslator.runScript(updateFaissMaster, updateBM25);
		}

		if (ClusterUtil.IS_CLUSTER) {
			Thread.ofVirtual().start(new DeleteFilesFromEngineRunner(engineId, this.getCatalogType(),
					filesToRemoveFromCloud.stream().toArray(String[]::new)));
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
		} else {
			// make the python method
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
		callMaker.append(", ").append("limit = ").append(limit);

		if (parameters.containsKey(VectorDatabaseParamOptionsEnum.USE_HYBRID_SEARCH.getKey())) {
			// add the columns based in the vector db query
			callMaker.append(", ").append("use_hybrid_search").append(" = ").append(PyUtils
					.determineStringType(parameters.get(VectorDatabaseParamOptionsEnum.USE_HYBRID_SEARCH.getKey())));
		}

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

		// close the method
		callMaker.append(")");
		classLogger.info("Running >>> " + callMaker.toString());
		List<Map<String, Object>> output = (List<Map<String, Object>>) pyTranslator.runDirectPy(insight,
				callMaker.toString());
		return output;
	}

	/**
	 * 
	 * @param indexClass
	 * @return
	 */
	public Map<String, String> removeCorruptedFiles(String indexClass) {
		checkSocketStatus();

		if (indexClass == null || indexClass.isEmpty()) {
			indexClass = this.defaultIndexClass;
		}

		File indexClassDirectory = new File(this.schemaFolder, indexClass);

		if (!indexClassDirectory.exists()) {
			throw new IllegalArgumentException("The FAISS Index Class called " + indexClass + " does not exist.");
		}

		StringBuilder executionScript = new StringBuilder();
		executionScript.append(vectorDatabaseSearcher).append(".searchers['").append(indexClass).append("']");

		executionScript.append(".removeCorruptedFiles(").append("path_to_files = '")
				.append(indexClassDirectory.getAbsolutePath().replace("\\", FILE_SEPARATOR)).append("')");

		@SuppressWarnings("unchecked")
		Map<String, String> corruptedFilesToReason = (Map<String, String>) this.pyTranslator
				.runDirectPy(executionScript.toString());

		if (ClusterUtil.IS_CLUSTER) {
			Thread.ofVirtual().start(new DeleteFilesFromEngineRunner(engineId, this.getCatalogType(),
					corruptedFilesToReason.keySet().stream().toArray(String[]::new)));
		}

		return corruptedFilesToReason;
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

	public List<Map<String, Object>> listAllMetadataRecords() {
		checkSocketStatus();

		StringBuilder getAllMetadataRecordsCommand = new StringBuilder();
		getAllMetadataRecordsCommand.append(this.vectorDatabaseSearcher).append(".list_all_metadata_records()");

		List<Map<String, Object>> allMetadataRecords = (List<Map<String, Object>>) pyTranslator
				.runDirectPy(getAllMetadataRecordsCommand.toString());
		return allMetadataRecords;
	}
	
	@Override
	public VectorDatabaseTypeEnum getVectorDatabaseType() {
		return VectorDatabaseTypeEnum.FAISS;
	}

	/**
	 * Starts the FAISS python server, ensuring any legacy {@code .pkl} index files
	 * on disk are migrated to the safe {@code .parquet}/{@code .npy} formats as
	 * part of startup. Legacy files are detected before Python boots so the
	 * resulting mapping can be used afterwards to reconcile the cloud copy of the
	 * engine in cluster mode.
	 *
	 * @param port the port the python server should bind to
	 */
	@Override
	protected synchronized void startServer(int port) {
		// Detect any legacy .pkl files BEFORE Python starts;
		// FAISSSearcher.__init__ will
		// auto-migrate them to .parquet/.npy (and delete the .pkl) as part of the init
		// script.
		Map<String, String> pendingPickleMigrations = detectLegacyPickleFiles();

		super.startServer(port);

		// In cluster mode the engine folder was just hydrated from cloud and likely
		// still contains the legacy .pkl entries we just converted locally. Push the
		// migrated counterparts up and remove the obsolete .pkl from cloud so peers
		// don't re-pull them.
		if (!pendingPickleMigrations.isEmpty()) {
			syncMigratedFilesToCloud(pendingPickleMigrations);
		}
	}

	/**
	 * Walks the schema folder looking for legacy pickle-format FAISS index files
	 * ({@code *_dataset.pkl}, {@code *_vectors.pkl}, and the master
	 * {@code dataset.pkl}/ {@code vectors.pkl} pair) under each index class and its
	 * {@code indexed_files} subdirectory.
	 *
	 * @return a map from the absolute path of each legacy {@code .pkl} file to the
	 *         absolute path of its predicted migrated counterpart
	 *         ({@code .parquet}/{@code .npy}); empty when no legacy files are
	 *         present or the schema folder is unavailable
	 */
	private Map<String, String> detectLegacyPickleFiles() {
		Map<String, String> migrations = new HashMap<>();
		if (this.schemaFolder == null || !this.schemaFolder.isDirectory()) {
			return migrations;
		}
		File[] indexClassDirs = this.schemaFolder.listFiles(File::isDirectory);
		if (indexClassDirs == null) {
			return migrations;
		}
		for (File indexClassDir : indexClassDirs) {
			collectLegacyPickleFiles(indexClassDir, migrations);
			File indexedFiles = new File(indexClassDir, AbstractVectorDatabaseEngine.INDEXED_FOLDER_NAME);
			if (indexedFiles.isDirectory()) {
				collectLegacyPickleFiles(indexedFiles, migrations);
			}
		}
		return migrations;
	}

	/**
	 * Scans a single directory (non-recursive) for {@code .pkl} files and adds an
	 * entry to {@code migrations} for each one whose name matches a known legacy
	 * pattern recognised by {@link #predictMigratedPath(File)}.
	 *
	 * @param dir        the directory to scan; may not exist or may not contain any
	 *                   pickle files, in which case the method returns without
	 *                   modifying {@code migrations}
	 * @param migrations accumulator mapping legacy pickle paths to their predicted
	 *                   safe-format counterparts
	 */
	private static void collectLegacyPickleFiles(File dir, Map<String, String> migrations) {
		File[] pklFiles = dir.listFiles((d, name) -> name.endsWith(".pkl"));
		if (pklFiles == null) {
			return;
		}
		for (File pkl : pklFiles) {
			String safePath = predictMigratedPath(pkl);
			if (safePath != null) {
				migrations.put(pkl.getAbsolutePath(), safePath);
			}
		}
	}

	/**
	 * Predicts the on-disk path that a legacy FAISS pickle file will have after the
	 * Python-side migration completes. Vector pickles ({@code *vectors.pkl}) become
	 * {@code .npy} and dataset pickles ({@code *dataset.pkl}) become
	 * {@code .parquet}, matching the conversion logic in {@code FAISSSearcher}.
	 *
	 * @param pklFile the legacy pickle file
	 * @return the absolute path of the migrated counterpart, or {@code null} if the
	 *         file name does not match a recognised legacy pattern
	 */
	private static String predictMigratedPath(File pklFile) {
		String name = pklFile.getName();
		String safeName;
		// Vector pickles -> .npy ; dataset pickles -> .parquet (matches FAISSSearcher
		// migration)
		if (name.endsWith("vectors.pkl")) {
			safeName = name.substring(0, name.length() - ".pkl".length()) + ".npy";
		} else if (name.endsWith("dataset.pkl")) {
			safeName = name.substring(0, name.length() - ".pkl".length()) + ".parquet";
		} else {
			return null;
		}
		return new File(pklFile.getParent(), safeName).getAbsolutePath();
	}

	/**
	 * Reconciles the cloud copy of the engine after a local pickle-to-safe-format
	 * migration: uploads the newly produced {@code .parquet}/{@code .npy} files and
	 * deletes the obsolete {@code .pkl} entries from cloud storage so peers don't
	 * re-hydrate them on their next startup. A no-op when not running in cluster
	 * mode.
	 *
	 * <p>
	 * If Python failed to produce a migrated file the cloud copy is left untouched
	 * for that entry so a subsequent startup can retry the migration.
	 *
	 * @param migrations map from legacy pickle paths to their migrated
	 *                   counterparts, as produced by
	 *                   {@link #detectLegacyPickleFiles()}
	 */
	private void syncMigratedFilesToCloud(Map<String, String> migrations) {
		if (!ClusterUtil.IS_CLUSTER || migrations.isEmpty()) {
			return;
		}
		List<String> safePaths = new ArrayList<>();
		List<String> legacyPathsToDelete = new ArrayList<>();
		for (Map.Entry<String, String> entry : migrations.entrySet()) {
			// Only push the migrated counterpart if Python actually produced it, and only
			// remove the matching .pkl from cloud in that same case. If something went
			// wrong with the conversion we leave the cloud copy alone so a future startup
			// can retry the migration.
			if (new File(entry.getValue()).exists()) {
				safePaths.add(entry.getValue());
				legacyPathsToDelete.add(entry.getKey());
			} else {
				classLogger.warn(
						"Expected migrated FAISS index file '{}' missing after Python init; leaving legacy '{}' in cloud so a future startup can retry the migration",
						entry.getValue(), entry.getKey());
			}
		}
		String[] toUpload = safePaths.toArray(new String[0]);
		String[] toDelete = legacyPathsToDelete.toArray(new String[0]);

		classLogger.info(
				"FAISS pickle migration: pushing {} safe-format file(s) to cloud and removing {} legacy .pkl entry/entries for engine '{}'",
				toUpload.length, toDelete.length, this.engineId);

		if (toUpload.length > 0) {
			Thread.ofVirtual().start(new CopyFilesToEngineRunner(engineId, this.getCatalogType(), toUpload));
		}
		if (toDelete.length > 0) {
			Thread.ofVirtual().start(new DeleteFilesFromEngineRunner(engineId, this.getCatalogType(), toDelete));
		}
	}

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

	/**
	 * 
	 * @param filter
	 * @return
	 */
	private StringBuilder processFilter(IQueryFilter filter) {
		// logic taken from SqlInterpreter.processFilter
		IQueryFilter.QUERY_FILTER_TYPE filterType = filter.getQueryFilterType();
		if (filterType == IQueryFilter.QUERY_FILTER_TYPE.SIMPLE) {
			return processSimpleQueryFilter((SimpleQueryFilter) filter);
		} else if (filterType == IQueryFilter.QUERY_FILTER_TYPE.AND) {
			return processAndQueryFilter((AndQueryFilter) filter);
		} else if (filterType == IQueryFilter.QUERY_FILTER_TYPE.OR) {
			return processOrQueryFilter((OrQueryFilter) filter);
		} else if (filterType == IQueryFilter.QUERY_FILTER_TYPE.FUNCTION) {
			throw new IllegalArgumentException(
					"Filters with a Query Filter Type of Function are not supported for FAISS vector databases");
		} else if (filterType == IQueryFilter.QUERY_FILTER_TYPE.BETWEEN) {
			throw new IllegalArgumentException(
					"Filters with a Query Filter Type of Between are not supported for FAISS vector databases");
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
		for (int i = 0; i < numAnds; i++) {
			if (i == 0) {
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
		for (int i = 0; i < numAnds; i++) {
			if (i == 0) {
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
	protected StringBuilder processBetweenQueryFilter(BetweenQueryFilter filter) {
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
		if (fType == FILTER_TYPE.COL_TO_COL) {
			return addSelectorToSelectorFilter(leftComp, rightComp, thisComparator);
		} else if (fType == FILTER_TYPE.COL_TO_VALUES) {
			return addSelectorToValuesFilter(leftComp, rightComp, thisComparator);
		} else if (fType == FILTER_TYPE.VALUES_TO_COL) {
			// same logic as above, just switch the order and reverse the comparator if it
			// is numeric
			return addSelectorToValuesFilter(rightComp, leftComp,
					IQueryFilter.getReverseNumericalComparator(thisComparator));
		} else if (fType == FILTER_TYPE.COL_TO_QUERY) {
			throw new IllegalArgumentException(
					"Filter of with a Filter Type of COL_TO_QUERY are not supported for FAISS vector databases");
		} else if (fType == FILTER_TYPE.QUERY_TO_COL) {
			throw new IllegalArgumentException(
					"Filter of with a Filter Type of QUERY_TO_COL are not supported for FAISS vector databases");
		} else if (fType == FILTER_TYPE.COL_TO_LAMBDA) {
			throw new IllegalArgumentException(
					"Filter of with a Filter Type of COL_TO_LAMBDA are not supported for FAISS vector databases");
		} else if (fType == FILTER_TYPE.LAMBDA_TO_COL) {
			// same logic as above, just switch the order and reverse the comparator if it
			// is numeric
			throw new IllegalArgumentException(
					"Filter of with a Filter Type of LAMBDA_TO_COL are not supported for FAISS vector databases");
		} else if (fType == FILTER_TYPE.VALUE_TO_VALUE) {
			// WHY WOULD YOU DO THIS!!!
			throw new IllegalArgumentException(
					"Filter of with a Filter Type of VALUE_TO_VALUE are not supported for FAISS vector databases");
		}
		return null;
	}

	/**
	 * Add filter for column to column
	 * 
	 * @param leftComp
	 * @param rightComp
	 * @param thisComparator
	 */
	protected StringBuilder addSelectorToSelectorFilter(NounMetadata leftComp, NounMetadata rightComp,
			String thisComparator) {
		// get the left side
		IQuerySelector leftSelector = (IQuerySelector) leftComp.getValue();
		IQuerySelector rightSelector = (IQuerySelector) rightComp.getValue();

		/*
		 * Add the filter syntax here once we have the correct physical names
		 */

		StringBuilder filterBuilder = new StringBuilder();
		filterBuilder.append(leftSelector.getQueryStructName());
		if (thisComparator.equals("<>")) {
			thisComparator = "!=";
		}
		filterBuilder.append(" ").append(thisComparator).append(" ").append(rightSelector.getQueryStructName());

		return filterBuilder;
	}

	/**
	 * Add filter for a column to values
	 * 
	 * @param filters
	 * @param leftComp
	 * @param rightComp
	 * @param thisComparator
	 */
	protected StringBuilder addSelectorToValuesFilter(NounMetadata leftComp, NounMetadata rightComp,
			String thisComparator) {
		StringBuilder filterBuilder = new StringBuilder();

		// get the left side
		IQuerySelector leftSelector = (IQuerySelector) leftComp.getValue();
		String leftDataType = leftSelector.getDataType();
		if (leftDataType == null) {
			String leftConceptProperty = leftSelector.getQueryStructName();
			filterBuilder.append(leftConceptProperty);
		}

		boolean needToClose = false;
		thisComparator = thisComparator.trim();
		switch (thisComparator) {
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

	/**
	 * Method is used to generate the appropriate syntax for each type of selector
	 * Note, this returns everything without the alias since this is called again
	 * from the base methods it calls to allow for complex math expressions
	 * 
	 * @param selector
	 * @return
	 */
	protected String processSelector(IQuerySelector selector, boolean addProcessedColumn) {
		IQuerySelector.SELECTOR_TYPE selectorType = selector.getSelectorType();
		if (selectorType == IQuerySelector.SELECTOR_TYPE.CONSTANT) {
			return processConstantSelector((QueryConstantSelector) selector);
		} else if (selectorType == IQuerySelector.SELECTOR_TYPE.COLUMN) {
			return processColumnSelector((QueryColumnSelector) selector, addProcessedColumn);
		} else if (selectorType == IQuerySelector.SELECTOR_TYPE.ARITHMETIC) {
			throw new IllegalArgumentException("Not supported.");
		} else if (selectorType == IQuerySelector.SELECTOR_TYPE.OPAQUE) {
			throw new IllegalArgumentException(
					"Filter of with an Opaque Selector Type are unsupported for FAISS vector databases");
		} else if (selectorType == IQuerySelector.SELECTOR_TYPE.IF_ELSE) {
			throw new IllegalArgumentException(
					"Filter of with an If Else Selector Type are unsupported for FAISS vector databases");
		}
		return null;
	}

	protected String processConstantSelector(QueryConstantSelector selector) {
		Object constant = selector.getConstant();
		if (constant instanceof SubQueryExpression) {
			throw new IllegalArgumentException("Sub Query Expressions are not supported");
		} else if (constant instanceof Number) {
			return constant.toString();
		} else if (constant instanceof Boolean) {
			String boolString = constant.toString();
			String pythonTrueFalse = Character.toUpperCase(boolString.charAt(0)) + boolString.substring(1);
			return pythonTrueFalse;
		} else {
			return "'" + AbstractSqlQueryUtil.escapeForSQLStatement(constant + "") + "'";
		}
	}

	/**
	 * The second
	 * 
	 * @param selector
	 * @param isTrueColumn
	 * @return
	 */
	protected String processColumnSelector(QueryColumnSelector selector, boolean notEmbeddedColumn) {
		String colName = selector.getColumn();
		return colName;
	}

}
