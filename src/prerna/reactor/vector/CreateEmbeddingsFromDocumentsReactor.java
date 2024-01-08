package prerna.reactor.vector;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.commons.io.FileUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.auth.utils.SecurityEngineUtils;
import prerna.engine.api.IVectorDatabaseEngine;
import prerna.engine.api.VectorDatabaseTypeEnum;
import prerna.engine.impl.vector.FaissDatabaseEngine;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.execptions.SemossPixelException;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Constants;
import prerna.util.Utility;
import prerna.util.ZipUtils;

public class CreateEmbeddingsFromDocumentsReactor extends AbstractReactor {

	private static final Logger classLogger = LogManager.getLogger(CreateEmbeddingsFromDocumentsReactor.class);
	private static final String pathToUnzipFiles = "zipFileExtractFolder";
	private String insightFolder;
	
	public CreateEmbeddingsFromDocumentsReactor() {
		this.keysToGet = new String[] {ReactorKeysEnum.ENGINE.getKey(), "filePaths", ReactorKeysEnum.PARAM_VALUES_MAP.getKey()};
		this.keyRequired = new int[] {1, 1, 0};
	}
	
	
	@Override
	public NounMetadata execute() {
		organizeKeys();
		String engineId = this.keyValue.get(this.keysToGet[0]);
		if(!SecurityEngineUtils.userCanEditEngine(this.insight.getUser(), engineId)) {
			throw new IllegalArgumentException("Vector db " + engineId + " does not exist or user does not have access to this engine");
		}
		
		IVectorDatabaseEngine eng = Utility.getVectorDatabase(engineId);
		if (eng == null) {
			throw new SemossPixelException("Unable to find engine");
		}
		
		Map<String, Object> paramMap = getMap();
		if(paramMap == null) {
			paramMap = new HashMap<String, Object>();
		}
		
		// for FAISS, make sure the user has access to the embedder model as well
		if (eng.getVectorDatabaseType() == VectorDatabaseTypeEnum.FAISS) {
			String embeddingsEngineId = eng.getSmssProp().getProperty(Constants.EMBEDDER_ENGINE_ID);
			if(!SecurityEngineUtils.userCanViewEngine(this.insight.getUser(), embeddingsEngineId)) {
				throw new IllegalArgumentException("Embeddings model " + embeddingsEngineId + " does not exist or user does not have access to this model");
			}
			
			Object keywordArgs = paramMap.getOrDefault(VectorDatabaseTypeEnum.ParamValueOptions.KEYWORD_SEARCH_PARAM.getKey(), null);
			if (keywordArgs != null) {
				// we also need to make sure they have access to the keyword engine id
				String keywordEngineId = eng.getSmssProp().getProperty(FaissDatabaseEngine.KEYWORD_ENGINE_ID);
				if(!SecurityEngineUtils.userCanViewEngine(this.insight.getUser(), keywordEngineId)) {
					throw new IllegalArgumentException("Keyword model " + keywordEngineId + " does not exist or user does not have access to this model");
				}
			}
		}
		
		
		insightFolder = this.insight.getInsightFolder();
		
		// this is coming from an insight so i assume its just the file names
		List<String> filePaths = getFiles();
		if (filePaths.isEmpty()) {
			throw new IllegalArgumentException("Please provide input files using \"filePaths\"");
		}
		String engineBaseFolder = eng.getSmssProp().getProperty("WORKING_DIR");
		
		// TODO some of this logic might move once we define other vector databases and their requirements
		// create a temporary directory to store the files. Its up to the engine to decide what should be done the files
		File tempDirectory = new File(engineBaseFolder, "temp");
		if(!tempDirectory.exists()) {
			tempDirectory.mkdirs();
		}
		
		List<String> filesToIndex = new ArrayList<String>();
		if (filePaths != null && !filePaths.isEmpty()) {
			for (String fileName : filePaths) {
				String filePath = insightFolder  + DIR_SEPARATOR + fileName;
				try {
					File sourceFile = new File(filePath);
					File destinationFile = new File(tempDirectory, sourceFile.getName());
					
					// Check if the destination file exists, and if so, delete it
		            if (destinationFile.exists()) {
		            	FileUtils.forceDelete(destinationFile);
		            }
		            
					FileUtils.moveFileToDirectory(sourceFile, tempDirectory, true);
					if (fileName.startsWith(pathToUnzipFiles)) {
						filesToIndex.add(tempDirectory.getAbsolutePath() + DIR_SEPARATOR + fileName.split("\\/")[1]);
					} else {
						filesToIndex.add(tempDirectory.getAbsolutePath() + DIR_SEPARATOR + sourceFile.getName());
					}
				} catch (IOException e) {
					classLogger.error(Constants.STACKTRACE, e);
					throw new IllegalArgumentException("Unable to move " + fileName + " to database");
				}
			}
		}
		
		VectorDatabaseTypeEnum vectorDbType = eng.getVectorDatabaseType();
		if (vectorDbType == VectorDatabaseTypeEnum.FAISS) {
			// send the insight so it can be used with IModelEngineCall
			paramMap.put("insight", this.insight);
			// send the temp directory as a param so it does not have to be recreated
			paramMap.put("temporaryFileDirectory", tempDirectory);
		}
		
		if (tempDirectory.list().length > 0) {
			eng.addDocument(filesToIndex, paramMap);
		}
		return new NounMetadata(true, PixelDataType.BOOLEAN, PixelOperationType.OPERATION);	
	}
	
	/**
	 * Get inputs
	 * @return list of engines to delete
	 */
	private List<String> getFiles() {
		Set<String> filePaths = new HashSet<>();
		
		// see if added as key
		GenRowStruct grs = this.store.getNoun(this.keysToGet[1]);
		if (grs != null && !grs.isEmpty()) {
			int size = grs.size();
			for (int i = 0; i < size; i++) {
				String fileName = grs.get(i).toString();
				if (fileName.endsWith(".zip")) {
					File zipFileExtractFolder = unzipFile(fileName);
					String [] filesInZipLocation = zipFileExtractFolder.list();
					for (int j = 0; j < filesInZipLocation.length; j++) {
						filesInZipLocation[j] = pathToUnzipFiles  + "/" + filesInZipLocation[j];
					}
					filePaths.addAll(Arrays.asList(filesInZipLocation));
				} else {
					filePaths.add(grs.get(i).toString());
				}
			}
		}
		return new ArrayList<>(filePaths);
	}
	
	private Map<String, Object> getMap() {
        GenRowStruct mapGrs = this.store.getNoun(keysToGet[2]);
        if(mapGrs != null && !mapGrs.isEmpty()) {
            List<NounMetadata> mapInputs = mapGrs.getNounsOfType(PixelDataType.MAP);
            if(mapInputs != null && !mapInputs.isEmpty()) {
                return (Map<String, Object>) mapInputs.get(0).getValue();
            }
        }
        List<NounMetadata> mapInputs = this.curRow.getNounsOfType(PixelDataType.MAP);
        if(mapInputs != null && !mapInputs.isEmpty()) {
            return (Map<String, Object>) mapInputs.get(0).getValue();
        }
        return null;
    }
	
	private File unzipFile(String fileName) {
		// if security enables, you need proper permissions
				// this takes in the insight and does a user check that the user has access to perform the operations
		String zipFileLocation = (insightFolder + "/" + fileName).replace('\\', '/');
		File zipFile = new File(zipFileLocation);
		File zipFileExtractFolder = new File(zipFile.getParent(), pathToUnzipFiles);
		if (!zipFileExtractFolder.exists()) {
			zipFileExtractFolder.mkdir();
		}
		if(zipFile.exists() && !zipFile.isFile()) {
			throw new IllegalArgumentException("Cannot find zip file '" + fileName + "')");
		}

		try {
			ZipUtils.unzip(zipFileLocation, zipFileExtractFolder.getAbsolutePath());
		} catch (IOException e) {
			throw new IllegalArgumentException("Unable to unzip file. Detailed error = " + e.getMessage());
		}
		return zipFileExtractFolder;
	}
	
	@Override
	protected String getDescriptionForKey(String key) {
		if(key.equals(ReactorKeysEnum.PARAM_VALUES_MAP.getKey())) {
			StringBuilder finalDescription = new StringBuilder("Param Options depend on the engine implementation");
			
			HashMap<String, List<String[]>> implementations = new HashMap<String, List<String []>>();
			
			// what are the key options for a given engine implementation and whether or not those keys are required or optional
			implementations.put(
				VectorDatabaseTypeEnum.FAISS.getVectorDatabaseName(), 
				Arrays.asList(
					new String [] {VectorDatabaseTypeEnum.ParamValueOptions.CHUNK_UNIT.getKey(), "Optional"}, 
					new String [] {VectorDatabaseTypeEnum.ParamValueOptions.COLUMNS_TO_INDEX.getKey(), "Optional"}, 
					new String [] {VectorDatabaseTypeEnum.ParamValueOptions.COLUMNS_TO_REMOVE.getKey(), "Optional"},
					new String [] {VectorDatabaseTypeEnum.ParamValueOptions.CONTENT_LENGTH.getKey(), "Optional"},
					new String [] {VectorDatabaseTypeEnum.ParamValueOptions.CONTENT_OVERLAP.getKey(), "Optional"},
					new String [] {VectorDatabaseTypeEnum.ParamValueOptions.EXTRACTION_METHOD.getKey(), "Optional"},
					new String [] {VectorDatabaseTypeEnum.ParamValueOptions.KEYWORD_SEARCH_PARAM.getKey(), "Optional"}
				)
			);
			
			for (Entry<String, List<String[]>> entry : implementations.entrySet()) {
				finalDescription.append("\n")
								.append("\t\t\t\t\t")
								.append(entry.getKey())
								.append(":");
				
				for (String[] option : entry.getValue()) {
					
					String paramKey = option[0];
					
					finalDescription.append("\n")
									.append("\t\t\t\t\t\t")
									.append(paramKey)
									.append("\t")
									.append("-")
									.append("\t")
									.append("(").append(option[1]).append(")")
									.append(" ")
									.append(VectorDatabaseTypeEnum.ParamValueOptions.getDescriptionFromKey(paramKey));
				}
			}
			return finalDescription.toString();
		}
	
		return super.getDescriptionForKey(key);
	}
}
