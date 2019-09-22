package prerna.util.git.reactors;

import java.io.File;
import java.io.IOException;

import org.apache.commons.io.FileUtils;

import prerna.auth.User;
import prerna.auth.utils.AbstractSecurityUtils;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.execptions.SemossPixelException;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.reactor.AbstractReactor;
import prerna.util.AssetUtility;
import prerna.util.Utility;

public class SaveAssetReactor extends AbstractReactor {

	public SaveAssetReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.FILE_NAME.getKey(), ReactorKeysEnum.CONTENT.getKey(),
				ReactorKeysEnum.COMMENT_KEY.getKey(), ReactorKeysEnum.SPACE.getKey() };
		this.keyRequired = new int[] { 1, 1, 0, 0 };
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();
		User user = this.insight.getUser();
		// check if user is logged in
		if (AbstractSecurityUtils.securityEnabled()) {
			if (AbstractSecurityUtils.anonymousUsersEnabled() && user.isAnonymous()) {
				throwAnonymousUserError();
			}
		}

		String space = this.keyValue.get(this.keysToGet[3]);
		String assetFolder = AssetUtility.getAssetBasePath(this.insight, space, true);
		String fileName = keyValue.get(keysToGet[0]);
		String filePath = assetFolder + "/" + fileName;
		String content = keyValue.get(keysToGet[1]);

		// write content to file
		content = Utility.decodeURIComponent(content);
		File file = new File(filePath);
		try {
			FileUtils.writeStringToFile(file, content);
		} catch (IOException e) {
			e.printStackTrace();
			NounMetadata error = NounMetadata.getErrorNounMessage("Unable to save file");
			SemossPixelException exception = new SemossPixelException(error);
			exception.setContinueThreadOfExecution(false);
			throw exception;
		}

		return NounMetadata.getSuccessNounMessage("Success!");
	}

}
