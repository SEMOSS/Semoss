package prerna.util.git.reactors;

import prerna.auth.User;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.reactor.AbstractReactor;

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
