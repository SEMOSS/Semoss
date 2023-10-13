package prerna.reactor.utils;

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
import prerna.engine.api.IEngine;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Constants;
import prerna.util.Utility;

public class CloseEngineReactor extends AbstractReactor {
	
	private static final Logger classLogger = LogManager.getLogger(CloseEngineReactor.class);
	
	public CloseEngineReactor() {
		this.keysToGet = new String[]{ReactorKeysEnum.ENGINE.getKey()};
		this.keyRequired = new int[] {1};
	}
	
	@Override
	public NounMetadata execute() {
		List<String> engineIds = getEngineIds();
		
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
		
		// once all are good, we can close
		for (String engineId : engineIds) {
			// we may have the alias
			engineId = SecurityQueryUtils.testUserEngineIdForAlias(this.insight.getUser(), engineId);
			IEngine engine = Utility.getEngine(engineId);
			try {
				engine.close();
			} catch (IOException e) {
				classLogger.error(Constants.STACKTRACE, e);
			}
		}
		return new NounMetadata(true, PixelDataType.BOOLEAN, PixelOperationType.ENGINE_INFO);
	}

	/**
	 * Get inputs
	 * @return list of engines to close
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
