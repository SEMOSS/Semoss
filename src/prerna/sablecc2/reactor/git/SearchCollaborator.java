package prerna.sablecc2.reactor.git;

import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;

import prerna.sablecc2.om.NounMetadata;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.reactor.AbstractReactor;
import prerna.util.GitHelper;

public class SearchCollaborator extends AbstractReactor {

	/**
	 * Search for another user
	 */
	
	public SearchCollaborator() {
		this.keysToGet = new String[]{"search", "username", "password"};
	}
	
	@Override
	public NounMetadata execute() {
		organizeKeys();

		String searchTerm = this.keyValue.get(this.keysToGet[0]);
		String username = this.keyValue.get(this.keysToGet[1]);
		String password = this.keyValue.get(this.keysToGet[2]);
		
		// creates a remote repository
		GitHelper helper = new GitHelper();

		Logger logger = getLogger(this.getClass().getName());
		logger.info("Establishing connection");
		logger.info("This can take several minutes depending on the speed of your internet");
		logger.info("Validating user");
		logger.info("Searching for " + searchTerm);
		List<Map<String, String>> collabList = helper.searchUsers(searchTerm, username, password);
		logger.info("Search Complete");
		return new NounMetadata(collabList, PixelDataType.VECTOR, PixelOperationType.MARKET_PLACE);
	}
}
