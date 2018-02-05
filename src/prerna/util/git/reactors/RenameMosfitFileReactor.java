package prerna.util.git.reactors;

import java.io.File;

import org.apache.log4j.Logger;

import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.reactor.AbstractReactor;
import prerna.util.MosfetSyncHelper;

public class RenameMosfitFileReactor extends AbstractReactor {

	public RenameMosfitFileReactor() {
		this.keysToGet = new String[]{"mosfet", ReactorKeysEnum.INSIGHT_NAME.getKey()};
	}
	
	@Override
	public NounMetadata execute() {
		Logger logger = getLogger(this.getClass().getName());
		organizeKeys();
		MosfetSyncHelper.renameMosfit(new File(this.keyValue.get(this.keysToGet[0])), this.keyValue.get(this.keysToGet[1]), logger);
		return new NounMetadata(true, PixelDataType.BOOLEAN, PixelOperationType.MARKET_PLACE);
	}
	
	///////////////////////// KEYS /////////////////////////////////////

	@Override
	protected String getDescriptionForKey(String key) {
		if (key.equals("mosfet")) {
			return "The name of the file containing the recipe used for syncing to git";
		} else {
			return super.getDescriptionForKey(key);
		}
	}
}