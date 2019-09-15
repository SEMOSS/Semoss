package prerna.util.git.reactors;

import java.util.List;
import java.util.Vector;

import prerna.auth.AccessToken;
import prerna.auth.User;
import prerna.auth.utils.AbstractSecurityUtils;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.reactor.AbstractReactor;
import prerna.util.AssetUtility;
import prerna.util.git.GitRepoUtils;

public class CtrlCAssetReactor extends AbstractReactor {

	public CtrlCAssetReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.FILE_PATH.getKey(), ReactorKeysEnum.SPACE.getKey() };
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();
		User user = this.insight.getUser();
		String filePath = this.keyValue.get(this.keysToGet[0]);
		String space = this.keyValue.get(this.keysToGet[1]);
		String assetFolder = AssetUtility.getAssetVersionBasePath(this.insight, space);
		String relativePath = AssetUtility.getAssetRelativePath(this.insight, space);
		
		if(space == null)
			space = "INSIGHT";
		
		// file / folder to be moved
		String copySource = assetFolder + DIR_SEPARATOR + relativePath + DIR_SEPARATOR + filePath;
		String showSource = space + DIR_SEPARATOR + filePath;
		user.ctrlC(copySource, showSource);
		

		return NounMetadata.getSuccessNounMessage("Copied " + showSource);

	}
}
