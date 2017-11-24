package prerna.sablecc2.reactor.git;

import java.util.Vector;

import org.apache.log4j.Logger;

import prerna.sablecc2.om.NounMetadata;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.reactor.AbstractReactor;
import prerna.util.GitHelper;

public class List4User extends AbstractReactor {

	// clone from remote
	// this assumes the local directory is not there
	
	
	public List4User()
	{
		this.keysToGet = new String[]{"username", "password"};
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
		String password = keyValue.get(keysToGet[1]);
		password = password.equalsIgnoreCase("null") ? null: password;

		logger.info("Validating User ");

		Vector <String> repoList = helper.listRemotes(keyValue.get(keysToGet[0]), password);
		logger.info("Repo List Complete");
		
		return new NounMetadata(repoList, PixelDataType.VECTOR);
	}

}
