package prerna.reactor.project;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.auth.User;
import prerna.auth.utils.AbstractSecurityUtils;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.AssetUtility;
import prerna.util.Utility;

public class ReplaceInaccessibleEnginesReactor extends AbstractReactor{
	
	private static final Logger classLogger = LogManager.getLogger(ReplaceInaccessibleEnginesReactor.class);

	public ReplaceInaccessibleEnginesReactor() {
		this.keysToGet = new String[] {ReactorKeysEnum.FILE_PATH.getKey(), ReactorKeysEnum.SPACE.getKey(), ReactorKeysEnum.MAP.getKey()};
		this.keyRequired = new int[] {1, 1, 1};
	}
	
	@Override
	public NounMetadata execute() {
		organizeKeys();
		User user = this.insight.getUser();
		// check if user is logged in
		if (AbstractSecurityUtils.anonymousUsersEnabled() && user.isAnonymous()) {
			throwAnonymousUserError();
		}
		
		String fileRelativePath = Utility.normalizePath(keyValue.get(keysToGet[0]));
		String space = this.keyValue.get(this.keysToGet[1]);
		
		// getting the full folder path where the UUIDs need to be replaced
		String baseFolder = AssetUtility.getRootFolderPath(this.insight, space, true);
		String assetsFileLocation = (baseFolder + "/" + fileRelativePath).replace('\\', '/');
		File finalProjectAssetFolder = new File(assetsFileLocation);
		
		// map of inaccessible engineIds to their accessible replacements
		Map<String, String> replacementMap = getReplacementMap();
        if (replacementMap == null || replacementMap.isEmpty()) {
            throw new IllegalArgumentException("Replacement map is missing or empty.");
        }
 
        // tracking replacement success/failure per UUID
        Map<String, Boolean> uuidSuccessStatus = new HashMap<>();
        Map<String, Set<String>> uuidFailedFiles = new HashMap<>();
         
        replacementMap.keySet().forEach(k -> {
           uuidSuccessStatus.put(k, true);
           uuidFailedFiles.put(k, new HashSet<>());
        });
        
        try (Stream<Path> stream = Files.walk(Paths.get(finalProjectAssetFolder.getAbsolutePath()))) {
            stream.filter(Files::isRegularFile).forEach(path -> {
                try {
                    List<String> lines = Files.readAllLines(path);
                    List<String> updatedLines = new ArrayList<>();
                    
                    // tracking the files in which replacement was unsuccessful
                    Map<String, Boolean> replacedInThisFile = new HashMap<>();
                    replacementMap.keySet().forEach(k -> replacedInThisFile.put(k, false));
 
                    for (String line : lines) {
                        String updatedLine = line;
                        for (Map.Entry<String, String> entry : replacementMap.entrySet()) {
                            String inaccessibleEngineId = entry.getKey();
                            String accessibleEngineId = entry.getValue();
                            if (updatedLine.contains(inaccessibleEngineId)) {
                                updatedLine = updatedLine.replace(inaccessibleEngineId, accessibleEngineId);
                                
                                replacedInThisFile.put(inaccessibleEngineId, true);
                            }
                        }
                        updatedLines.add(updatedLine);
                    }
 
                    // write modified lines back to the file
                    Files.write(path, updatedLines);
                    
                    // log files where UUID was present but not successfully replaced
                    for (Map.Entry<String, Boolean> entry : replacedInThisFile.entrySet()) {
                    	String uuid = entry.getKey();
                    	boolean wasReplaced = entry.getValue();
                    	if (!wasReplaced) {
                    		// check if uuid even existed in the file
                    		boolean uuidWasPresent = lines.stream().anyMatch(line -> line.contains(uuid));
                    		if(uuidWasPresent) {
                    			uuidSuccessStatus.put(entry.getKey(), false);
                                uuidFailedFiles.get(entry.getKey()).add(path.getFileName().toString());
                                classLogger.warn("Inaccessible engine ID {} was not replaced in file: {}", uuid, path.toString());
                    		}
                        }
                    }
                    
                } catch (Exception e) {
                	throw new IllegalArgumentException("Error processing file: {}"+ path, e);
                    //classLogger.error("Error processing file: {}", path, e);
                }
            });
        } catch (Exception e) {
        	throw new IllegalArgumentException("Error walking through project folder: {}"+ finalProjectAssetFolder, e);
            //classLogger.error("Error walking through project folder: {}", finalProjectAssetFolder, e);
        }
		
        // final success and failure results
        Set<String> successList = new HashSet<>();
        Map<String, Set<String>> failedList = new HashMap<>();
        
        for (Map.Entry<String, Boolean> entry : uuidSuccessStatus.entrySet()) {
        	String uuid = entry.getKey();
        	if (entry.getValue()) {
        		successList.add(uuid);
        	}else {
        		failedList.put(uuid, uuidFailedFiles.get(uuid));
        	}
        }
        
        Map<String, Object> retMap = new HashMap<>();
        retMap.put("success", successList);
        retMap.put("failed", failedList);
        
        return new NounMetadata(retMap, PixelDataType.UPLOAD_RETURN_MAP, PixelOperationType.MARKET_PLACE_ADDITION);
	}
	
	private Map<String, String> getReplacementMap() {
	    GenRowStruct mapGrs = this.store.getNoun(ReactorKeysEnum.MAP.getKey());
	    if (mapGrs != null && !mapGrs.isEmpty()) {
	        List<NounMetadata> mapInputs = mapGrs.getNounsOfType(PixelDataType.MAP);
	        if (mapInputs != null && !mapInputs.isEmpty()) {
	            return (Map<String, String>) mapInputs.get(0).getValue();
	        }
	    }
	    return null;
	}
	
	@Override
	public String getReactorDescription() {
	    return "Replaces inaccessible engine Ids with corresponding accessible engine Ids in the project folder files.";
	}
	
	@Override
	protected String getDescriptionForKey(String key) {
	    if(key.equals(ReactorKeysEnum.FILE_PATH.getKey())) {
	        return "This is a required value containing the relative file path of the unzipped folder.";
	    } else if(key.equals(ReactorKeysEnum.SPACE.getKey())) {
	        return "This is a required field which is used to resolve the full folder path of the uploaded project.";
	    } else if(key.equals(ReactorKeysEnum.MAP.getKey())) {
	    	return "This is a required field which contains the key-value map of inaccessible engine Ids to their accessible replacements.";
	    }
	    return super.getDescriptionForKey(key);
	}
	
}
