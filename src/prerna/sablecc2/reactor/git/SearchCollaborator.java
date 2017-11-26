package prerna.sablecc2.reactor.git;

import java.util.Hashtable;

import org.apache.log4j.Logger;

import prerna.sablecc2.om.NounMetadata;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.reactor.AbstractReactor;
import prerna.util.GitHelper;

public class SearchCollaborator extends AbstractReactor {

	// clone from remote
	// this assumes the local directory is not there
	
	
	public SearchCollaborator()
	{
		this.keysToGet = new String[]{"search","username", "password"};
	}
	
	@Override
	public NounMetadata execute() {
		// TODO Auto-generated method stub
		// creates a remote repository
		Logger logger = getLogger(this.getClass().getName());

		organizeKeys();
		logger.info("Establishing connection ");
		logger.info("This can take several minutes depending on the speed of your internet ");
		GitHelper helper = new GitHelper();

		logger.info("Validating User ");

		Hashtable <String, String> collabList = helper.searchUsers(keyValue.get(keysToGet[0]),keyValue.get(keysToGet[1]), keyValue.get(keysToGet[2]));
		logger.info("Search Complete");
		
		return new NounMetadata(collabList, PixelDataType.MAP);
	}

}
