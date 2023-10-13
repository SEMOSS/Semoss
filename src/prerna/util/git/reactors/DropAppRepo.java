package prerna.util.git.reactors;

import org.apache.logging.log4j.Logger;

import prerna.nameserver.utility.MasterDatabaseUtility;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.AssetUtility;
import prerna.util.git.GitRepoUtils;

public class DropAppRepo extends AbstractReactor {

	public DropAppRepo() {
		this.keysToGet = new String[]{ReactorKeysEnum.DATABASE.getKey(), ReactorKeysEnum.REPOSITORY.getKey()};
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
		GitRepoUtils.removeRemote(databaseFolder, repository);
		return new NounMetadata(true, PixelDataType.CONST_STRING, PixelOperationType.MARKET_PLACE);
	}

}
