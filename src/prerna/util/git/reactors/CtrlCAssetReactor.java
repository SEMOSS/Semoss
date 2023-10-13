package prerna.util.git.reactors;

import prerna.auth.User;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.AssetUtility;
import prerna.util.Utility;

public class CtrlCAssetReactor extends AbstractReactor {

	public CtrlCAssetReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.FILE_PATH.getKey(), ReactorKeysEnum.SPACE.getKey() };
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();
		User user = this.insight.getUser();
		
		if(user == null)
			return NounMetadata.getErrorNounMessage("You have to be logged in to perform this action ");
			
		
		String filePath = Utility.normalizePath(this.keyValue.get(this.keysToGet[0]));
		String space = this.keyValue.get(this.keysToGet[1]);
		String assetFolder = AssetUtility.getAssetVersionBasePath(this.insight, space, false);
		String relativePath = AssetUtility.getAssetRelativePath(this.insight, space);
		
		if(space == null)
			space = "INSIGHT";
		
		// file / folder to be moved
		if(relativePath == null)
			relativePath = "";
		else
			relativePath=relativePath + DIR_SEPARATOR;
		String copySource = assetFolder + DIR_SEPARATOR + relativePath + filePath;
		String showSource = space + DIR_SEPARATOR + filePath;
		
		
		user.ctrlC(copySource, showSource);
		

		return NounMetadata.getSuccessNounMessage("Copied " + showSource);

	}
}
