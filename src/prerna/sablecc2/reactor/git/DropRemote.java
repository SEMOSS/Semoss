package prerna.sablecc2.reactor.git;

import org.apache.log4j.Logger;

import prerna.sablecc2.om.NounMetadata;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.reactor.AbstractReactor;
import prerna.util.DIHelper;
import prerna.util.git.GitRepoUtils;

public class DropRemote extends AbstractReactor {

	public DropRemote() {
		this.keysToGet = new String[]{"remoteapp", "app"};
	}
	
	@Override
	public NounMetadata execute() {
		organizeKeys();
		
		Logger logger = getLogger(this.getClass().getName());
		logger.info("Removing remote...");
		String remoteApp = this.keyValue.get(this.keysToGet[0]);
		String appName = this.keyValue.get(this.keysToGet[1]);
		
		String baseFolder = DIHelper.getInstance().getProperty("BaseFolder");
		String appFolder = baseFolder + "/" + appName;
		GitRepoUtils.removeRemote(appFolder, remoteApp);
		return new NounMetadata(true, PixelDataType.CONST_STRING, PixelOperationType.MARKET_PLACE);
	}

}
