package prerna.reactor.engine;

import java.io.File;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.io.FilenameUtils;

import prerna.auth.User;
import prerna.auth.utils.AbstractSecurityUtils;
import prerna.auth.utils.SecurityEngineUtils;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.EngineUtility;

public class BrowseEngineAssetsReactor extends AbstractReactor {

	public BrowseEngineAssetsReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.ENGINE.getKey(), 
				ReactorKeysEnum.FILE_PATH.getKey() };
		this.keyRequired = new int[] {1,0};
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();

		User user = this.insight.getUser();
        String engineId = this.keyValue.get(ReactorKeysEnum.ENGINE.getKey());
        // check if user is logged in
 		if (AbstractSecurityUtils.anonymousUsersEnabled() && user.isAnonymous()) {
 			throwAnonymousUserError();
 		}
     		
        if (!SecurityEngineUtils.userCanViewEngine(user, engineId)) {
            throw new IllegalArgumentException("Engine " + engineId + " does not exist or user does not have access to this engine");
        }
		String relativeFilePath = this.keyValue.get(ReactorKeysEnum.FILE_PATH.getKey());
		if(relativeFilePath != null) {
			relativeFilePath = relativeFilePath.trim();
			if(!relativeFilePath.isEmpty()) {
				relativeFilePath = relativeFilePath.replace('\\', '/');
				if(!relativeFilePath.startsWith("/")) {
					relativeFilePath = "/" + relativeFilePath;
				}
			}
		}
		
		String pathSubstring = EngineUtility.getLocalEngineBaseDirectory(engineId);
		int pathSubstringIndex = pathSubstring.length();
		String filePath = EngineUtility.getSpecificEngineBaseFolder(engineId);
		if(relativeFilePath != null && !relativeFilePath.isEmpty()) {
			filePath += relativeFilePath;
		}
		
		File directory = new File(filePath);
		if(!directory.exists()) {
			throw new IllegalArgumentException("The directory " + relativeFilePath + " does not exist within the engine folder");
		}
		if(!directory.isDirectory()) {
			throw new IllegalArgumentException("The path " + relativeFilePath + " exists within the engine folder but is not a directory");
		}
		
		SimpleDateFormat dateFormat = new SimpleDateFormat("MM/dd/yyyy HH:mm:ss");
		
		List<Map<String, Object>> retObj = new ArrayList<>();
		File[] allFiles = directory.listFiles();
		for(File f : allFiles) {
			if(f.getName().startsWith(".") && f.isDirectory()) {
				// we dont want to show this
				continue;
			}
			Map<String, Object> fileMap = new HashMap<>();
			fileMap.put("name", f.getName());
			fileMap.put("type", FilenameUtils.getExtension(f.getName()));
			fileMap.put("lastModified", dateFormat.format(f.lastModified()));
			fileMap.put("path", f.getAbsolutePath().substring(pathSubstringIndex));
			retObj.add(fileMap);
		}

		NounMetadata retNoun = new NounMetadata(retObj, PixelDataType.CUSTOM_DATA_STRUCTURE, PixelOperationType.OPERATION);
		return retNoun;
	}

	@Override
	public String getReactorDescription() {
		return "List the files and directories from a relative filePath input from within the engine folder";
	}

	@Override
	protected String getDescriptionForKey(String key) {
		if(key.equals(ReactorKeysEnum.ENGINE.getKey())) {
			return "The unique id for the engine";
		} else if(key.equals(ReactorKeysEnum.FILE_PATH.getKey())) {
			return "The relative file path to list contents from.";
		}
		return super.getDescriptionForKey(key);
	}

}

