package prerna.sablecc2.reactor.git;

import org.apache.log4j.Logger;

import prerna.sablecc2.om.NounMetadata;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.reactor.AbstractReactor;
import prerna.util.DIHelper;
import prerna.util.GitHelper;

public class MakeApp extends AbstractReactor {

	// clone from remote
	// this assumes the local directory is not there
	
	public MakeApp()
	{
		super.keysToGet = new String[]{"app", "remote", "username", "password", "smss", "type"};
	}
	
	@Override
	public NounMetadata execute() {
		// TODO Auto-generated method stub
		// creates a remote repository
		Logger logger = getLogger(this.getClass().getName());

		logger.info("Welcome to App IF ANY : SEMOSS Marketplace");
		organizeKeys();
		GitHelper helper = new GitHelper();
		
		logger.info("Initialized");
		
		String baseFolder = DIHelper.getInstance().getProperty("BaseFolder");
		String smss = keyValue.get(keysToGet[4]);
		
		// later pass SMSS
		smss = smss.contains("null")?null:smss;

		logger.info("Making your app global");
		logger.info("This can take several minutes");

		helper.makeRemoteFromApp(baseFolder, keyValue.get(keysToGet[0]), keyValue.get(keysToGet[1]), true, keyValue.get(keysToGet[2]), keyValue.get(keysToGet[3]));

		logger.info("Congratulations - you have successfully created your app " + keyValue.get(keysToGet[1]));

		return new NounMetadata("Success", PixelDataType.CONST_STRING, PixelOperationType.MARKET_PLACE);
	}

}
