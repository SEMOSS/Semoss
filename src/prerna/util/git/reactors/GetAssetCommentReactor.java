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
import prerna.util.AssetUtility;
import prerna.util.Utility;
import prerna.util.git.GitRepoUtils;

public class GetAssetCommentReactor extends AbstractReactor {

	public GetAssetCommentReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.FILE_PATH.getKey(), ReactorKeysEnum.SPACE.getKey() };
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();
		// check if user is logged in
		User user = this.insight.getUser();
		if (AbstractSecurityUtils.anonymousUsersEnabled() && user.isAnonymous()) {
			throwAnonymousUserError();
		}

		String space = this.keyValue.get(this.keysToGet[1]);
		if(space == null || space.trim().isEmpty() || space.equals(AssetUtility.INSIGHT_SPACE_KEY)) {
			// if we are in the insight space
			// it must be a saved insight
			if(!this.insight.isSavedInsight()) {
				return NounMetadata.getWarningNounMessage("Unable to get comments the insight must be saved to allow commenting.");
			}
		}
		String assetFolder = AssetUtility.getAssetVersionBasePath(this.insight, space, false);
		String relativePath = AssetUtility.getAssetRelativePath(this.insight, space);

		// specify a file
		String filePath = relativePath + "/" + Utility.normalizePath(this.keyValue.get(this.keysToGet[0]));

		// get comments
		List<Map<String, Object>> comments = GitRepoUtils.getCommits(assetFolder, filePath);
		return new NounMetadata(comments, PixelDataType.CUSTOM_DATA_STRUCTURE, PixelOperationType.OPERATION);
	}
}
