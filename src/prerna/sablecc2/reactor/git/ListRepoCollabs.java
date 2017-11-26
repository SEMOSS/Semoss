package prerna.sablecc2.reactor.git;

import java.util.Vector;

import org.apache.log4j.Logger;

import prerna.sablecc2.om.NounMetadata;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.reactor.AbstractReactor;
import prerna.util.GitHelper;

public class ListRepoCollabs extends AbstractReactor {

	// clone from remote
	// this assumes the local directory is not there
	
	
	public ListRepoCollabs()
	{
		this.keysToGet = new String[]{"repository", "username", "password"};
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

		logger.info("Listing Collaborators");

		Vector <String> collabs = helper.listCollaborators(keyValue.get(keysToGet[0]),keyValue.get(keysToGet[1]), keyValue.get(keysToGet[2]));
		
		return new NounMetadata(collabs, PixelDataType.VECTOR);
	}

}
