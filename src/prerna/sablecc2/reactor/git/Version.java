package prerna.sablecc2.reactor.git;

import java.io.IOException;

import org.apache.log4j.Logger;
import org.eclipse.jgit.api.errors.GitAPIException;

import prerna.sablecc2.om.NounMetadata;
import prerna.sablecc2.reactor.AbstractReactor;
import prerna.util.DIHelper;
import prerna.util.GitHelper;

public class Version extends AbstractReactor {

	// versionize
	
	
	public Version()
	{
		this.keysToGet = new String[]{"app"};
	}
	
	@Override
	public NounMetadata execute() {
		// TODO Auto-generated method stub
		organizeKeys();
		
		Logger logger = getLogger(this.getClass().getName());
		try {
			
			String baseFolder = DIHelper.getInstance().getProperty("BaseFolder");
			String dbName = baseFolder + "/" + keyValue.get(keysToGet[0]);	
			logger.info("Converting " + dbName + " to a versionable app");
			
			logger.info("Checking to see if it is already versioned");
			
			GitHelper helper = new GitHelper();

			if(helper.checkLocalRepository(dbName))
				helper.makeLocalRepository(dbName);
			logger.info("Creating initial version");
			helper.commitAll(dbName, true);
			logger.info("Complete");

		} catch (GitAPIException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			logger.fatal("API" + e.getMessage());
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			logger.fatal(e.getMessage());
		}
		
		return null;
	}

}
