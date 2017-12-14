package prerna.sablecc2.reactor.git;

import java.io.File;

import org.apache.log4j.Logger;

import prerna.sablecc2.om.NounMetadata;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.reactor.AbstractReactor;
import prerna.util.MosfitSyncHelper;

public class GitRenameMosfitFileReactor extends AbstractReactor {

	public GitRenameMosfitFileReactor() {
		this.keysToGet = new String[]{"mosfet", ReactorKeysEnum.INSIGHT_NAME.getKey()};
	}
	
	@Override
	public NounMetadata execute() {
		Logger logger = getLogger(this.getClass().getName());
		organizeKeys();
		MosfitSyncHelper.renameMosfit(new File(this.keyValue.get(this.keysToGet[0])), this.keyValue.get(this.keysToGet[1]), logger);
		return new NounMetadata(true, PixelDataType.BOOLEAN, PixelOperationType.MARKET_PLACE);
	}

}