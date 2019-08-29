package prerna.sablecc2.reactor.utils;

import java.io.File;
import java.util.UUID;

import org.apache.log4j.Logger;

import prerna.auth.User;
import prerna.auth.utils.AbstractSecurityUtils;
import prerna.auth.utils.SecurityAdminUtils;
import prerna.auth.utils.SecurityAppUtils;
import prerna.auth.utils.SecurityQueryUtils;
import prerna.engine.api.IEngine;
import prerna.nameserver.utility.MasterDatabaseUtility;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.reactor.AbstractReactor;
import prerna.util.DIHelper;
import prerna.util.Utility;
import prerna.util.ZipDatabase;

public class ExportAppReactor extends AbstractReactor {

	private static final String CLASS_NAME = ExportAppReactor.class.getName();
	
	public ExportAppReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.APP.getKey() };
	}
	
	@Override
	public NounMetadata execute() {
		Logger logger = getLogger(CLASS_NAME);
		organizeKeys();
		String appId = this.keyValue.get(this.keysToGet[0]);
		
		User user = this.insight.getUser();
		if(AbstractSecurityUtils.securityEnabled()) {
			appId = SecurityQueryUtils.testUserEngineIdForAlias(this.insight.getUser(), appId);
			boolean isAdmin = SecurityAdminUtils.userIsAdmin(user);
			if(!isAdmin) {
				boolean isOwner = SecurityAppUtils.userIsOwner(user, appId);
				if(!isOwner) {
					throw new IllegalArgumentException("App " + appId + " does not exist or user does not have permissions to database. User must be the owner to perform this function.");
				}
			}
		} else {
			appId = MasterDatabaseUtility.testEngineIdIfAlias(appId);
			if(!MasterDatabaseUtility.getAllEngineIds().contains(appId)) {
				throw new IllegalArgumentException("App " + appId + " does not exist");
			}
		}		
		
		File zip = null;
		try {
			logger.info("Exporting Database Now... ");
			logger.info("Stopping the engine ... ");
			// remove the app
			IEngine engine = Utility.getEngine(appId);
			engine.closeDB();
			zip = ZipDatabase.zipEngine(appId, engine.getEngineName());			
			DIHelper.getInstance().removeLocalProperty(appId);
			logger.info("Synchronize Database Complete");
		} finally {
			// open it back up
			logger.info("Opening the engine again ... ");
			Utility.getEngine(appId);
		}
		
		// store it in the insight so the FE can download it
		// only from the given insight
		String randomKey = UUID.randomUUID().toString();
		this.insight.addExportFile(randomKey, zip.getAbsolutePath());
		return new NounMetadata(randomKey, PixelDataType.CONST_STRING, PixelOperationType.FILE_DOWNLOAD);
	}

}
