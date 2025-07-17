package prerna.reactor.project;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

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

	private static final String[] DEPENDENCIES_FILE_EXTENSIONS = { ".js", ".jsx", ".java", ".env", ".py", ".ts", ".tsx" };
	
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

		// check if any dependency file is present in the uploaded project
		String folderPath = finalProjectAssetFolder.getAbsolutePath();
		boolean hasDependencyFile;
		try {
			hasDependencyFile = Files.walk(Paths.get(folderPath)).anyMatch(p -> Files.isRegularFile(p)
					&& Arrays.stream(DEPENDENCIES_FILE_EXTENSIONS).anyMatch(ext -> p.toString().endsWith(ext)));
		} catch (IOException e) {
			classLogger.error("Error while checking for dependency files", e);
		} 
		
		// extract engineIds from project
		// then process and set project dependencies
		String[] engineIds = UploadInputUtility.getEngineIdsFromProject(finalProjectAssetFolder);
		Map<String, Object> engineInfo = UploadInputUtility.processAndSetProjectDependencies(engineIds, space, user);

		Map<String, Object> retMap = new HashMap<>();
		retMap.put("engineIds", engineInfo);

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
