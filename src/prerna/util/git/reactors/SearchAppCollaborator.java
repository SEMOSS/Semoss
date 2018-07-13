package prerna.util.git.reactors;

import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;

import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.git.GitCollaboratorUtils;

public class SearchAppCollaborator extends GitBaseReactor {

	/**
	 * Search for another user
	 */
	
	public SearchAppCollaborator() {
		this.keysToGet = new String[]{ReactorKeysEnum.COLLABORATOR.getKey(), 
				ReactorKeysEnum.USERNAME.getKey(), ReactorKeysEnum.PASSWORD.getKey()};
	}
	
	@Override
	public NounMetadata execute() {
		organizeKeys();

		String searchTerm = this.keyValue.get(this.keysToGet[0]);

		Logger logger = getLogger(this.getClass().getName());
		logger.info("Establishing connection");
		logger.info("This can take several minutes depending on the speed of your internet");
		logger.info("Validating user");
		logger.info("Searching for " + searchTerm);

		List<Map<String, String>> collabList = null;
		if(keyValue.size() == 3)
		{
			String username = this.keyValue.get(this.keysToGet[1]);
			String password = this.keyValue.get(this.keysToGet[2]);
		
			collabList = GitCollaboratorUtils.searchUsers(searchTerm, username, password);
		}
		else
		{
			String token = getToken();
			collabList = GitCollaboratorUtils.searchUsers(searchTerm, token);
			
		}
		logger.info("Search Complete");
		return new NounMetadata(collabList, PixelDataType.VECTOR, PixelOperationType.MARKET_PLACE);
	}
}
