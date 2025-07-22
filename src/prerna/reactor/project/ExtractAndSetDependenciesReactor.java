package prerna.reactor.project;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.auth.User;
import prerna.auth.utils.AbstractSecurityUtils;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.AssetUtility;
import prerna.util.UploadInputUtility;
import prerna.util.Utility;

public class ExtractAndSetDependenciesReactor extends AbstractReactor {

	private static final Logger classLogger = LogManager.getLogger(ExtractAndSetDependenciesReactor.class);

	public ExtractAndSetDependenciesReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.FILE_PATH.getKey(), ReactorKeysEnum.SPACE.getKey() };
		this.keyRequired = new int[] { 1, 1 };
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

		// getting the asset folder path where UUIDs are present.
		String baseFolder = AssetUtility.getRootFolderPath(this.insight, space, true);
		String assetsFileLocation = (baseFolder + "/" + fileRelativePath).replace('\\', '/');
		File finalProjectAssetFolder = new File(assetsFileLocation);
		
		// extract engineIds and file mapping from project
		Map<String, Map<String, Object>> uuidToFiles = UploadInputUtility.getEngineIdsFromProject(finalProjectAssetFolder);
		 
		// process engineIds and set project dependencies
		String[] engineIds = uuidToFiles.keySet().toArray(new String[0]);
		Map<String, Object> engineInfo = UploadInputUtility.processAndSetProjectDependencies(engineIds, space, user);
		
		Map<String, Map<String, String>> successMap = (Map<String, Map<String, String>>)engineInfo.get("success");
		
		Set<String> failedSet = (Set<String>)engineInfo.get("failed");
		
		// final success list of engineIds
		Map<String, Map<String, Object>> successResult = new HashMap<>();
		
		for (Map.Entry<String, Map<String, String>> entry : successMap.entrySet()) {
		    String engineId = entry.getKey();
		    Map<String, String> engineMeta = entry.getValue();
		 
		    Map<String, Object> value = new HashMap<>();
		    value.put("engineType", engineMeta.get("engineType"));
		    value.put("engineName", engineMeta.get("engineName"));
		    value.put("files", uuidToFiles.get(engineId).get("files"));
		 
		    successResult.put(engineId, value);
		}
		 
		// final failed list of engineIds
		Map<String, Map<String, Object>> failureResult = new HashMap<>();
		 
		for (String engineId : failedSet) {
		    Map<String, Object> value = new HashMap<>();
		    value.put("files", uuidToFiles.containsKey(engineId) ? uuidToFiles.get(engineId).get("files") : new ArrayList<>());
		 
		    failureResult.put(engineId, value);
		}
		 
		// final return map
		Map<String, Object> retMap = new HashMap<>();

		Map<String, Object> engineIdMap = new HashMap<>();
		engineIdMap.put("success", successResult);
		engineIdMap.put("failed", failureResult);
		
		// sending the success and failed list of engineIds to FE
		retMap.put("engineIds", engineIdMap);
		return new NounMetadata(retMap, PixelDataType.UPLOAD_RETURN_MAP, PixelOperationType.MARKET_PLACE_ADDITION);
	}

	@Override
	public String getReactorDescription() {
		return "Extract engine Ids from the updated project's asset folder and adds the engine Ids into projectdependencies table.";
	}

	@Override
	protected String getDescriptionForKey(String key) {
		if (key.equals(ReactorKeysEnum.FILE_PATH.getKey())) {
			return "This is a required value containing the relative file path of the unzipped folder.";
		} else if (key.equals(ReactorKeysEnum.SPACE.getKey())) {
			return "This is a required field which is used to resolve the full folder path of the uploaded project.";
		}
		return super.getDescriptionForKey(key);
	}

}
