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

public class EscapeAssetReactor extends AbstractReactor {

	public EscapeAssetReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.FILE_PATH.getKey(), ReactorKeysEnum.SPACE.getKey() };
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();
		User user = this.insight.getUser();
		user.escapeCopy();
		return NounMetadata.getSuccessNounMessage("Clipboard cleared");

	}
}
