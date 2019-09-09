package prerna.util.git.reactors;

import java.util.List;
import java.util.Map;

import prerna.auth.User;
import prerna.auth.utils.AbstractSecurityUtils;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.reactor.AbstractReactor;
import prerna.util.git.GitRepoUtils;

public class GetAssetCommentReactor extends AbstractReactor {

	public GetAssetCommentReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.FILE_PATH.getKey(), ReactorKeysEnum.IN_APP.getKey() };
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();
		
		// check if user is logged in
		User user = this.insight.getUser();
		if (AbstractSecurityUtils.securityEnabled()) {
			if (AbstractSecurityUtils.anonymousUsersEnabled() && user.isAnonymous()) {
				throwAnonymousUserError();
			}
		}

		// get the asset folder path
		boolean app = keyValue.containsKey(keysToGet[1]) || (keyValue.containsKey(keysToGet[0]) && keyValue.get(keysToGet[0]).startsWith("app_assets")); ;
		// get the asset folder path
		String assetFolder = this.insight.getInsightFolder(); 
		if(app)
			assetFolder = this.insight.getAppFolder();
		assetFolder = assetFolder.replaceAll("\\\\", "/");

		// specify a file
		String filePath = this.keyValue.get(this.keysToGet[0]);
		//filePath = filePath.replaceAll("app_assets", "");

		// get comments
		List<Map<String, Object>> comments = GitRepoUtils.getCommits(assetFolder, filePath);
		return new NounMetadata(comments, PixelDataType.CUSTOM_DATA_STRUCTURE, PixelOperationType.OPERATION);
	}
}
