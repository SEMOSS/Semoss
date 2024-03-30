package prerna.util.git.reactors;

import org.apache.logging.log4j.Logger;

import prerna.masterdatabase.utility.MasterDatabaseUtility;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.AssetUtility;
import prerna.util.git.GitRepoUtils;

public class DeleteAppRepo extends GitBaseReactor {

	public DeleteAppRepo() {
		this.keysToGet = new String[]{
				ReactorKeysEnum.DATABASE.getKey(), ReactorKeysEnum.REPOSITORY.getKey(), 
				ReactorKeysEnum.USERNAME.getKey(), ReactorKeysEnum.PASSWORD.getKey()};
	}
	
	@Override
	public NounMetadata execute() {
		organizeKeys();
		
		Logger logger = getLogger(this.getClass().getName());
		logger.info("Removing remote...");
		String databaseId = this.keyValue.get(this.keysToGet[0]);
		String databaseName = MasterDatabaseUtility.getDatabaseAliasForId(databaseId);
		String repository = this.keyValue.get(this.keysToGet[1]);
		String databaseFolder = AssetUtility.getProjectVersionFolder(databaseName, databaseId);

		// remove it from remote
		// take it out from local in case the global fails since they have removed the repository
		GitRepoUtils.deleteRemoteRepositorySettings(databaseFolder, repository);

		if(keyValue.size() == 4)
		{
			String username = this.keyValue.get(this.keysToGet[2]);
			String password = this.keyValue.get(this.keysToGet[3]);
			// drop it from external
			GitRepoUtils.deleteRemoteRepository(repository, username, password);
		}
		else
		{
			String oauth = getToken();
			GitRepoUtils.deleteRemoteRepository(repository, oauth);
		}
	
		return new NounMetadata(true, PixelDataType.CONST_STRING, PixelOperationType.MARKET_PLACE);
	}

	
	
}
