package prerna.sablecc2.reactor.masterdatabase;

import java.util.Properties;

import org.apache.logging.log4j.Logger;

import prerna.auth.utils.SecurityEngineUtils;
import prerna.auth.utils.SecurityQueryUtils;
import prerna.nameserver.AddToMasterDB;
import prerna.nameserver.DeleteFromMasterDB;
import prerna.nameserver.utility.MasterDatabaseUtility;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.reactor.AbstractReactor;
import prerna.util.Constants;
import prerna.util.DIHelper;
import prerna.util.Utility;

public class SyncDatabaseWithLocalMasterReactor extends AbstractReactor {

	public static final String CLASS_NAME = SyncDatabaseWithLocalMasterReactor.class.getName();
	
	public SyncDatabaseWithLocalMasterReactor() {
		this.keysToGet = new String[]{ReactorKeysEnum.DATABASE.getKey()};
	}
	
	@Override
	public NounMetadata execute() {
		organizeKeys();
		String databaseId = this.keyValue.get(this.keysToGet[0]);
		// we may have the alias
		databaseId = SecurityQueryUtils.testUserEngineIdForAlias(this.insight.getUser(), databaseId);
		if(!SecurityEngineUtils.userCanEditEngine(this.insight.getUser(), databaseId)) {
			throw new IllegalArgumentException("Database " + databaseId + " does not exist or user does not have access to app");
		}
				
		Logger logger = getLogger(CLASS_NAME);
		logger.info("Starting to synchronize metadata");
		
		logger.info("Starting to remove existing metadata");
		DeleteFromMasterDB remover = new DeleteFromMasterDB();
		remover.deleteEngineRDBMS(databaseId);
		logger.info("Finished removing existing metadata");

		logger.info("Starting to add metadata");
		String smssFile = (String) DIHelper.getInstance().getEngineProperty(databaseId + "_" + Constants.STORE);
		Properties prop = Utility.loadProperties(smssFile);
		AddToMasterDB adder = new AddToMasterDB();
		adder.registerEngineLocal(prop);
		logger.info("Done adding new metadata");

		logger.info("Synchronization complete");
		NounMetadata noun = new NounMetadata(true, PixelDataType.BOOLEAN);
		noun.addAdditionalReturn(new NounMetadata("Successfully synchronized " + MasterDatabaseUtility.getDatabaseAliasForId(databaseId) + "'s metadata", 
				PixelDataType.CONST_STRING, PixelOperationType.SUCCESS));
		return noun;
	}
}
