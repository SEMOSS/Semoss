package prerna.sablecc2.reactor.git;

import java.io.StringWriter;

import org.apache.log4j.Logger;

import prerna.sablecc2.om.NounMetadata;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.reactor.AbstractReactor;
import prerna.util.DIHelper;
import prerna.util.GitHelper;

public class Replicate extends AbstractReactor {

	// clone from remote
	// this assumes the local directory is not there
	
	public Replicate()
	{
		super.keysToGet = new String[]{"remoteapp", "app", "type"};
	}
	
	@Override
	public NounMetadata execute() {
		// TODO Auto-generated method stub
		// creates a remote repository
		organizeKeys();
		Logger logger = getLogger(this.getClass().getName());
		
		logger.info("Starting copy from remote : " + keyValue.get(keysToGet[0]));
		logger.info("This can take a few minutes depending on the size of your app");
		GitHelper helper = new GitHelper();

		logger.info("Initialized");

		String baseFolder = DIHelper.getInstance().getProperty("BaseFolder");
		String dbName = keyValue.get(keysToGet[1]);	

		logger.info("Creating Repo At " + dbName);

		helper.makeAppFromRemote(baseFolder, dbName, keyValue.get(keysToGet[0]));

		logger.info("Copy Complete");
		
		StringWriter sw = new StringWriter();
		
		sw.write("Success \n Repository At: " + dbName ); 
		

		return new NounMetadata(sw.getBuffer().toString(), PixelDataType.CONST_STRING, PixelOperationType.MARKET_PLACE);
	}

}
