package prerna.sablecc2.reactor.git;

import org.apache.log4j.Logger;

import prerna.sablecc2.om.NounMetadata;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.reactor.AbstractReactor;
import prerna.util.DIHelper;
import prerna.util.GitHelper;

public class IsGit extends AbstractReactor {

	public IsGit()
	{
		this.keysToGet = new String[]{"app"};
	}
	
	@Override
	public NounMetadata execute() {
		// TODO Auto-generated method stub
		Logger logger = getLogger(this.getClass().getName());
		try {
			organizeKeys();
			
			logger.info("Checking - Please wait");
			String baseFolder = DIHelper.getInstance().getProperty("BaseFolder");
			//helper.synchronize(baseFolder + "/db/Mv2Git4", "Mv4", userName, password, true);

			String dbName = baseFolder + "/db/" + keyValue.get(keysToGet[0]);	

			boolean isGit = GitHelper.isGit(dbName);
			// if nothing is sent it means it is dual
			logger.info("Complete");

			return new NounMetadata(isGit, PixelDataType.BOOLEAN, PixelOperationType.MARKET_PLACE);
		} catch (Exception e) {
			e.printStackTrace();
			logger.fatal(e.getMessage());
		}
		return null;
	}
	
}
