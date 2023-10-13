package prerna.reactor.panel.external;

import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class OpenTabReactor extends AbstractReactor {
	
	public OpenTabReactor() {
		this.keysToGet = new String[]{"webURL"};
	}

	@Override
	public NounMetadata execute() {
		String url = this.curRow.get(0).toString();
		return new NounMetadata(url, PixelDataType.CONST_STRING, PixelOperationType.OPEN_TAB);
	}
	
	///////////////////////// KEYS /////////////////////////////////////

	@Override
	protected String getDescriptionForKey(String key) {
		if (key.equals("webURL")) {
			return "The web address for the page that you would like to open in a new browser";
		} else {
			return super.getDescriptionForKey(key);
		}
	}

}
