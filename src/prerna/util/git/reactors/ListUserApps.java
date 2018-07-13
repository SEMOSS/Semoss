package prerna.util.git.reactors;

import java.util.List;

import org.apache.log4j.Logger;

import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.git.GitRepoUtils;

public class ListUserApps extends GitBaseReactor {

	public ListUserApps() {
		this.keysToGet = new String[]{ReactorKeysEnum.USERNAME.getKey(), ReactorKeysEnum.PASSWORD.getKey()};
	}
	
	@Override
	public NounMetadata execute() {
		organizeKeys();
		Logger logger = getLogger(this.getClass().getName());

		String username = this.keyValue.get(this.keysToGet[0]);
		String password = this.keyValue.get(this.keysToGet[1]);
		
		List<String> repoList = null;
		
		logger.info("Establishing connection ");
		logger.info("This can take several minutes depending on the speed of your internet ");
		logger.info("Validating User ");
		if(keyValue.size() == 2)
			repoList = GitRepoUtils.listRemotesForUser(username, password);
		else
		{
			String token = getToken();
			repoList = GitRepoUtils.listRemotesForUser(token);
		}
		logger.info("Repo List Complete");
		return new NounMetadata(repoList, PixelDataType.VECTOR, PixelOperationType.MARKET_PLACE);
	}

}
