package prerna.util.git.reactors;

import org.apache.logging.log4j.Logger;

import prerna.auth.utils.AbstractSecurityUtils;
import prerna.auth.utils.SecurityAppUtils;
import prerna.auth.utils.SecurityQueryUtils;
import prerna.engine.impl.SmssUtilities;
import prerna.nameserver.utility.MasterDatabaseUtility;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.reactor.AbstractReactor;
import prerna.util.DIHelper;
import prerna.util.git.GitUtils;

public class IsGit extends AbstractReactor {

	public IsGit() {
		this.keysToGet = new String[]{ReactorKeysEnum.DATABASE.getKey()};
	}

	@Override
	public NounMetadata execute() {
		Logger logger = getLogger(this.getClass().getName());
		organizeKeys();
		String databaseId = keyValue.get(keysToGet[0]);
		if(databaseId == null || databaseId.isEmpty()) {
			throw new IllegalArgumentException("Need to specify the database id");
		}
		String databaseName = null;
		
		if(AbstractSecurityUtils.securityEnabled()) {
			databaseId = SecurityQueryUtils.testUserDatabaseIdForAlias(this.insight.getUser(), databaseId);
			if(!SecurityAppUtils.userCanEditDatabase(this.insight.getUser(), databaseId)) {
				throw new IllegalArgumentException("Database does not exist or user does not have access to edit database");
			}
			databaseName = SecurityQueryUtils.getDatabaseAliasForId(databaseId);
		} else {
			databaseName = MasterDatabaseUtility.getDatabaseAliasForId(databaseId);
		}
		
		logger.info("Checking - Please wait");
		// get the path of the git location
		String baseFolder = DIHelper.getInstance().getProperty("BaseFolder");
		String appFolder = baseFolder + "/db/" + SmssUtilities.getUniqueName(databaseName, databaseId);
		boolean isGit = GitUtils.isGit(appFolder);
		logger.info("Complete");
		return new NounMetadata(isGit, PixelDataType.BOOLEAN, PixelOperationType.MARKET_PLACE);
	}

}
