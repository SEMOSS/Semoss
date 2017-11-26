package prerna.sablecc2.reactor.git;

import java.util.Hashtable;

import org.apache.log4j.Logger;

import prerna.sablecc2.om.NounMetadata;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.reactor.AbstractReactor;
import prerna.util.GitHelper;

public class AddCollaborator extends AbstractReactor {

	// clone from remote
	// this assumes the local directory is not there
	
	
	public AddCollaborator()
	{
		this.keysToGet = new String[]{"repository", "collaborator","username", "password"};
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

		logger.info("Adding Collaborator ");

		helper.addCollaborator(keyValue.get(keysToGet[0]),keyValue.get(keysToGet[2]), keyValue.get(keysToGet[3]), keyValue.get(keysToGet[1]));
		logger.info("Collaborator Added");
		
		return new NounMetadata("SUCCESS", PixelDataType.CONST_STRING);
	}

}
