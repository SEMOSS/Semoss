package prerna.sablecc2.reactor.utils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.auth.User;
import prerna.auth.utils.AbstractSecurityUtils;
import prerna.auth.utils.SecurityAdminUtils;
import prerna.auth.utils.SecurityEngineUtils;
import prerna.auth.utils.SecurityQueryUtils;
import prerna.auth.utils.WorkspaceAssetUtils;
import prerna.cluster.util.ClusterUtil;
import prerna.cluster.util.DeleteEngineRunner;
import prerna.engine.api.IEngine;
import prerna.nameserver.DeleteFromMasterDB;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.reactor.AbstractReactor;
import prerna.usertracking.UserTrackingUtils;
import prerna.util.Constants;
import prerna.util.EngineSyncUtility;
import prerna.util.Utility;
import prerna.util.upload.UploadUtilities;

public class DeleteEngineReactor extends AbstractReactor {

	private static final Logger classLogger = LogManager.getLogger(DeleteEngineReactor.class);

	public DeleteEngineReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.ENGINE.getKey() };
	}

	@Override
	public NounMetadata execute() {
		List<String> engineIds = getEngineIds();
		// first validate all the inputs
		User user = this.insight.getUser();
		boolean isAdmin = SecurityAdminUtils.userIsAdmin(user);
		if(!isAdmin) {
			if(AbstractSecurityUtils.adminOnlyEngineDelete()) {
				throwFunctionalityOnlyExposedForAdminsError();
			}
			for (String engineId : engineIds) {
				if(WorkspaceAssetUtils.isAssetOrWorkspaceProject(engineId)) {
					throw new IllegalArgumentException("Users are not allowed to delete your workspace or asset database.");
				}
				// we may have the alias
				engineId = SecurityQueryUtils.testUserEngineIdForAlias(this.insight.getUser(), engineId);
				boolean isOwner = SecurityEngineUtils.userIsOwner(user, engineId);
				if(!isOwner) {
					throw new IllegalArgumentException("Engine " + engineId + " does not exist or user does not have permissions to delete the engine. User must be the owner to perform this function.");
				}
			} 
		}
		
		// once all are good, we can delete
		for (String engineId : engineIds) {
			// we may have the alias
			engineId = SecurityQueryUtils.testUserEngineIdForAlias(this.insight.getUser(), engineId);
			IEngine engine = Utility.getEngine(engineId);
			IEngine.CATALOG_TYPE engineType = engine.getCatalogType();
			
			deleteEngines(engine, engineType);
			EngineSyncUtility.clearEngineCache(engineId);
			UserTrackingUtils.deleteEngine(engineId);
			// Run the delete thread in the background for removing from cloud storage
			if (ClusterUtil.IS_CLUSTER) {
				Thread deleteAppThread = new Thread(new DeleteEngineRunner(engineId, engineType));
				deleteAppThread.start();
			}
		}
		
		return new NounMetadata(true, PixelDataType.BOOLEAN, PixelOperationType.DELETE_ENGINE);
	}

	/**
	 * 
	 * @param engine
	 * @return
	 */
	private boolean deleteEngines(IEngine engine, IEngine.CATALOG_TYPE engineType) {
		String engineId = engine.getEngineId();
		UploadUtilities.removeEngineFromDIHelper(engineId);
		// remove from local master if database
		if(IEngine.CATALOG_TYPE.DATABASE == engineType) {
			DeleteFromMasterDB remover = new DeleteFromMasterDB();
			remover.deleteEngineRDBMS(engineId);
		}
		// remove from security
		SecurityEngineUtils.deleteEngine(engineId);
		// remove from user tracking
		UserTrackingUtils.deleteEngine(engineId);
		
		// now try to actually remove from disk
		try {
			engine.delete();
		} catch (IOException e) {
			classLogger.error(Constants.STACKTRACE, e);
		}
		
		return true;
	}

	/**
	 * Get inputs
	 * @return list of engines to delete
	 */
	public List<String> getEngineIds() {
		List<String> engineIds = new ArrayList<>();

		// see if added as key
		GenRowStruct grs = this.store.getNoun(this.keysToGet[0]);
		if (grs != null && !grs.isEmpty()) {
			int size = grs.size();
			for (int i = 0; i < size; i++) {
				engineIds.add(grs.get(i).toString());
			}
			return engineIds;
		}

		// no key is added, grab all inputs
		int size = this.curRow.size();
		for (int i = 0; i < size; i++) {
			engineIds.add(this.curRow.get(i).toString());
		}
		return engineIds;
	}
}
