package prerna.util.git.reactors;

import org.apache.log4j.Logger;

import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.reactor.AbstractReactor;
import prerna.util.DIHelper;
import prerna.util.git.GitRepoUtils;

public class DropAppRepo extends AbstractReactor {

	public DropAppRepo() {
		this.keysToGet = new String[]{ReactorKeysEnum.APP.getKey(), ReactorKeysEnum.REPOSITORY.getKey()};
	}
	
	@Override
	public NounMetadata execute() {
		organizeKeys();
		
		Logger logger = getLogger(this.getClass().getName());
		logger.info("Removing remote...");
		String appName = this.keyValue.get(this.keysToGet[0]);
		String repository = this.keyValue.get(this.keysToGet[1]);
		
		String baseFolder = DIHelper.getInstance().getProperty("BaseFolder");
		String appFolder = baseFolder + "/db/" + appName + "/version";
		GitRepoUtils.removeRemote(appFolder, repository);
		return new NounMetadata(true, PixelDataType.CONST_STRING, PixelOperationType.MARKET_PLACE);
	}

}
