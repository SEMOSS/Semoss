package prerna.util.git.reactors;

import java.io.File;
import java.io.IOException;

import org.apache.commons.io.FileUtils;

import prerna.auth.User;
import prerna.om.CopyObject;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.reactor.AbstractReactor;
import prerna.util.AssetUtility;

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
