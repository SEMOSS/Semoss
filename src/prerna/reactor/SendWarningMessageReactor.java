package prerna.reactor;

import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class SendWarningMessageReactor extends AbstractReactor {

	public SendWarningMessageReactor() {
		this.keysToGet = new String[] {ReactorKeysEnum.MESSAGE.getKey()};
	}
	
	@Override
	public NounMetadata execute() {
		organizeKeys();
		String message = this.keyValue.get(this.keysToGet[0]);
		return getWarning(message);
	}

}
