package prerna.util.git.reactors;

import prerna.auth.User;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class ClipboardReactor extends AbstractReactor {

	public ClipboardReactor() {
		this.keysToGet = new String[] { };
	}

	@Override
	public NounMetadata execute() {
		User user = this.insight.getUser();
		String showSource = null;
		if(user.getCtrlC() != null)
			showSource = user.getCtrlC().showSource;
		
		if(showSource != null)
			return NounMetadata.getSuccessNounMessage("Copied " + showSource);
		else
			return NounMetadata.getSuccessNounMessage("Clipboard is empty ");

	}
}
