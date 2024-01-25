package prerna.reactor.vector;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

import org.apache.commons.io.FileUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.tika.Tika;
import org.apache.tika.metadata.Metadata;

import prerna.auth.utils.SecurityEngineUtils;
import prerna.engine.api.IVectorDatabaseEngine;
import prerna.engine.api.VectorDatabaseTypeEnum;
import prerna.engine.impl.vector.FaissDatabaseEngine;
import prerna.reactor.AbstractReactor;
import prerna.reactor.vector.VectorDatabaseParamOptionsEnum.CreateEmbeddingsParamOptions;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.execptions.SemossPixelException;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Constants;
import prerna.util.Utility;

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
			
			Object keywordArgs = paramMap.getOrDefault(VectorDatabaseParamOptionsEnum.KEYWORD_SEARCH_PARAM.getKey(), null);
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
		List<String> filePaths;
		try {
			filePaths = getFiles();
			
			if (filePaths.isEmpty()) {
				throw new IllegalArgumentException("Please provide valid input files using \"filePaths\". File types supported are pdf, word, ppt, or txt files");
			}
			
			for (String filePath: filePaths) {
		    	File file = new File(filePath);
		    	
		    	// Check if the file exists
		        if (!file.exists()) {
		        	throw new IllegalArgumentException("File path for " + file.getName() + " does not exist within the insight.");
		        }
			}
			
			VectorDatabaseTypeEnum vectorDbType = eng.getVectorDatabaseType();
			if (vectorDbType == VectorDatabaseTypeEnum.FAISS) {
				// send the insight so it can be used with IModelEngine call
				paramMap.put("insight", this.insight);
			}
			

			eng.addDocument(filePaths, paramMap);
			
			File zipFileExtractionDir = new File(insightFolder + File.separator + pathToUnzipFiles);
			if (zipFileExtractionDir.exists()) {
				FileUtils.forceDelete(zipFileExtractionDir);
			}
		} catch (IOException ioe) {
			classLogger.error(Constants.STACKTRACE, ioe);
			throw new IllegalArgumentException("The following IO error occured: " + ioe.getMessage());
		}

		return new NounMetadata(true, PixelDataType.BOOLEAN, PixelOperationType.OPERATION);	
	}
	
	/**
	 * Get inputs
	 * @return list of engines to delete
	 * @throws IOException 
	 */
	private List<String> getFiles() throws IOException {
		Set<String> filePaths = new HashSet<>();
		
		// see if added as key
		GenRowStruct grs = this.store.getNoun(this.keysToGet[1]);
		if (grs != null && !grs.isEmpty()) {
			int size = grs.size();
			for (int i = 0; i < size; i++) {
				String filePath = insightFolder + File.separator + grs.get(i).toString();
				if (isZipFile(filePath)) {
					String zipFileLocation = filePath.replace('\\', '/');
					File zipFileExtractFolder = new File(insightFolder, pathToUnzipFiles);
					List<String> validFilesInZip = unzipAndFilter(zipFileLocation, zipFileExtractFolder.getAbsolutePath());
					filePaths.addAll(validFilesInZip);
					new File(zipFileLocation).delete();
				} else {
					 //String filePath = destDirectory + File.separator + entry.getName();
					 if(isSupportedFileType(filePath)) {
						filePaths.add(filePath);
					 }
				}
			}
		}
		return new ArrayList<>(filePaths);
	}
	
	/**
	 * Get the map from the paramValues noun store
	 * @return list of engines to delete
	 */
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
	
	
	/**
	 * Recursively go through all the zips, directories and files in a zip file and save the paths of 
	 * valid file types
	 * 
	 * @param {@code String} zipFilePath 	
	 * 
	 * @return {@code List<String>}		sdfs
	 * @throws IOException 
	 */
	private List<String> unzipAndFilter(String zipFilePath, String destDirectory) throws IOException {
        List<String> validFilePaths = new ArrayList<>();
        File destDir = new File(destDirectory);
        if (!destDir.exists()) {
            destDir.mkdir();
        }

        try (ZipInputStream zipIn = new ZipInputStream(new FileInputStream(zipFilePath))) {
            ZipEntry entry = zipIn.getNextEntry();

            while (entry != null) {
                String filePath = destDirectory + File.separator + entry.getName();
                if (!entry.isDirectory() && isSupportedFileType(filePath)) {
                    extractFile(zipIn, filePath);
                    validFilePaths.add(filePath);
                } else if (entry.isDirectory()) {
                    File dir = new File(filePath);
                    dir.mkdirs();
                } else if (isZipFile(filePath)) {
                    // Handle nested zip file
                	this.extractFile(zipIn, filePath);
					
					// Check if the entry is not in the root directory
					String parentPath = null;
					if(filePath.contains("/")) { // ZIP entries use "/" as a separator
					    parentPath = filePath.substring(0, filePath.lastIndexOf('/'));
					}

					// Extract the last part of the path (file name + extension)
					String fileNameWithExtension = filePath.contains("/") 
					                                ? filePath.substring(filePath.lastIndexOf('/') + 1) 
					                                : filePath;

					// Remove the extension
					String baseName = fileNameWithExtension.contains(".") 
					                  ? fileNameWithExtension.substring(0, fileNameWithExtension.lastIndexOf('.')) 
					                  : fileNameWithExtension;
          
					List<String> nestedValidPaths = unzipAndFilter(filePath, parentPath + File.separator + baseName);
                    validFilePaths.addAll(nestedValidPaths);
                }

                zipIn.closeEntry();
                entry = zipIn.getNextEntry();
            }
        }

        return validFilePaths;
    }

    private void extractFile(ZipInputStream zipIn, String filePath) throws IOException {
        try (FileOutputStream fos = new FileOutputStream(filePath)) {
            byte[] buffer = new byte[1024];
            int bytesRead;
            while ((bytesRead = zipIn.read(buffer)) != -1) {
                fos.write(buffer, 0, bytesRead);
            }
        }
    }

    private boolean isSupportedFileType(String filePath) {

    	// Find the last index of '.'
        int dotIndex = filePath.lastIndexOf('.');
        
        if (dotIndex > 0 && dotIndex < filePath.length() - 1) {
            // Extract the extension and convert it to lower case
        	String extension = filePath.substring(dotIndex + 1).toLowerCase();
        	
        	return extension.equals("pdf") || extension.equals("pptx") || extension.equals("ppt")
                    || extension.equals("doc") || extension.equals("docx") || extension.equals("txt") || extension.equals("csv");
        } else {
        	// do a mime type check
        	Tika tika = new Tika();
        	File file = new File(filePath);
        	try (FileInputStream inputstream = new FileInputStream(file)) {
                String mimeType = tika.detect(inputstream, new Metadata());

                switch (mimeType) {
                    case "application/pdf":
                    case "application/vnd.openxmlformats-officedocument.wordprocessingml.document": // .docx
                    case "application/vnd.ms-powerpoint": // .ppt
                    case "application/vnd.openxmlformats-officedocument.presentationml.presentation": // .pptx
                    case "text/plain":
                        return true;
                    default:
                        return false;
                }
            } catch (IOException e) {
                classLogger.error(Constants.ERROR_MESSAGE, e);
                return false;
            }
        }
    }

    private boolean isZipFile(String filePath) {        
    	// Find the last index of '.'
        int dotIndex = filePath.lastIndexOf('.');
        
        if (dotIndex > 0 && dotIndex < filePath.length() - 1) {
            // Extract the extension and convert it to lower case
        	String extension = filePath.substring(dotIndex + 1).toLowerCase();
        	
        	return extension.equals("zip");
        } else {
        	// do a mime type check
        	Tika tika = new Tika();
        	File file = new File(filePath);
        	try (FileInputStream inputstream = new FileInputStream(file)) {
                String mimeType = tika.detect(inputstream, new Metadata());
                
                if (mimeType != null) {
            		if (mimeType.equalsIgnoreCase("application/zip")) {
            			return true;
                    }
            	} 
                
                return false;
            } catch (IOException e) {
                classLogger.error(Constants.ERROR_MESSAGE, e);
                return false;
            }
        }
    }
	
	@Override
	protected String getDescriptionForKey(String key) {
		if(key.equals(ReactorKeysEnum.PARAM_VALUES_MAP.getKey())) {
			StringBuilder finalDescription = new StringBuilder("Param Options depend on the engine implementation");
									
			for (CreateEmbeddingsParamOptions entry : CreateEmbeddingsParamOptions.values()) {
				finalDescription.append("\n")
								.append("\t\t\t\t\t")
								.append(entry.getVectorDbType().getVectorDatabaseName())
								.append(":");
				
				for (String paramKey : entry.getParamOptionsKeys()) {
					finalDescription.append("\n")
									.append("\t\t\t\t\t\t")
									.append(paramKey)
									.append("\t")
									.append("-")
									.append("\t")
									.append("(").append(entry.getRequirementStatus(paramKey)).append(")")
									.append(" ")
									.append(VectorDatabaseParamOptionsEnum.getDescriptionFromKey(paramKey));
				}
			}
			return finalDescription.toString();
		}
	
		return super.getDescriptionForKey(key);
	}
}
