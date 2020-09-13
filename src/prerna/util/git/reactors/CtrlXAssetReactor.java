package prerna.util.git.reactors;

import prerna.auth.User;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.reactor.AbstractReactor;
import prerna.util.AssetUtility;

public class CtrlXAssetReactor extends AbstractReactor {

	public CtrlXAssetReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.FILE_PATH.getKey(), ReactorKeysEnum.SPACE.getKey() };
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();
		User user = this.insight.getUser();
		
		if(user == null)
			return NounMetadata.getErrorNounMessage("You have to be logged in to perform this action ");

		String filePath = this.keyValue.get(this.keysToGet[0]);
		String space = this.keyValue.get(this.keysToGet[1]);
		String assetFolder = AssetUtility.getAssetVersionBasePath(this.insight, space);
		String relativePath = AssetUtility.getAssetRelativePath(this.insight, space);
		
		// file / folder to be moved
		if(relativePath == null)
			relativePath = "";
		else
			relativePath=relativePath + DIR_SEPARATOR;

		// file / folder to be moved
		String copySource = assetFolder + DIR_SEPARATOR + relativePath  + filePath;
		String showSource = space + DIR_SEPARATOR + filePath;
		user.ctrlC(copySource, showSource);
		

		return NounMetadata.getSuccessNounMessage("Cut " + showSource);

	}
}
