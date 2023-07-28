package prerna.sablecc2.reactor.utils;

import java.util.List;
import java.util.Vector;

import prerna.auth.User;
import prerna.auth.utils.AbstractSecurityUtils;
import prerna.auth.utils.SecurityAdminUtils;
import prerna.auth.utils.SecurityEngineUtils;
import prerna.auth.utils.SecurityQueryUtils;
import prerna.auth.utils.WorkspaceAssetUtils;
import prerna.cluster.util.ClusterUtil;
import prerna.cluster.util.DeleteAppRunner;
import prerna.engine.api.IDatabase;
import prerna.nameserver.DeleteFromMasterDB;
import prerna.nameserver.utility.MasterDatabaseUtility;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.reactor.AbstractReactor;
import prerna.usertracking.UserTrackingUtils;
import prerna.util.Constants;
import prerna.util.DIHelper;
import prerna.util.EngineSyncUtility;
import prerna.util.Utility;

public class DeleteDatabaseReactor extends AbstractReactor {

	public DeleteDatabaseReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.DATABASE.getKey() };
	}

	@Override
	public NounMetadata execute() {
		List<String> databaseIds = getDatabaseIds();
		for (String databaseId : databaseIds) {
			if(WorkspaceAssetUtils.isAssetOrWorkspaceProject(databaseId)) {
				throw new IllegalArgumentException("Users are not allowed to delete your workspace or asset database.");
			}
			User user = this.insight.getUser();
			
			// we may have the alias
			if(AbstractSecurityUtils.securityEnabled()) {
				databaseId = SecurityQueryUtils.testUserDatabaseIdForAlias(this.insight.getUser(), databaseId);
				boolean isAdmin = SecurityAdminUtils.userIsAdmin(user);
				if(!isAdmin) {
					if(AbstractSecurityUtils.adminOnlyDbDelete()) {
						throwFunctionalityOnlyExposedForAdminsError();
					}
					
					boolean isOwner = SecurityEngineUtils.userIsOwner(user, databaseId);
					if(!isOwner) {
						throw new IllegalArgumentException("Database " + databaseId + " does not exist or user does not have permissions to delete the database. User must be the owner to perform this function.");
					}
				}
			} else {
				databaseId = MasterDatabaseUtility.testDatabaseIdIfAlias(databaseId);
				if(!MasterDatabaseUtility.getAllDatabaseIds().contains(databaseId)) {
					throw new IllegalArgumentException("Database " + databaseId + " does not exist");
				}
			}

			IDatabase database = Utility.getDatabase(databaseId);
			deleteDatabase(database);
			EngineSyncUtility.clearEngineCache(databaseId);
			
			UserTrackingUtils.deleteDatabase(databaseId);

			// Run the delete thread in the background for removing from cloud storage
			if (ClusterUtil.IS_CLUSTER) {
				Thread deleteAppThread = new Thread(new DeleteAppRunner(databaseId));
				deleteAppThread.start();
			}
		}
		
		return new NounMetadata(true, PixelDataType.BOOLEAN, PixelOperationType.DELETE_ENGINE);
	}

	/**
	 * 
	 * @param database
	 * @return
	 */
	private boolean deleteDatabase(IDatabase database) {
		String databaseId = database.getEngineId();
		database.deleteDB();

		// remove from dihelper... this is absurd
		String databaseIds = (String) DIHelper.getInstance().getEngineProperty(Constants.ENGINES);
		databaseIds = databaseIds.replace(";" + databaseId, "");
		// in case it was the first databases loaded
		databaseIds = databaseIds.replace(databaseId + ";", "");
		DIHelper.getInstance().setEngineProperty(Constants.ENGINES, databaseIds);

		DeleteFromMasterDB remover = new DeleteFromMasterDB();
		remover.deleteEngineRDBMS(databaseId);
		SecurityEngineUtils.deleteEngine(databaseId);
		return true;
	}

	/**
	 * Get inputs
	 * @return list of databases to delete
	 */
	public List<String> getDatabaseIds() {
		List<String> databaseIds = new Vector<String>();

		// see if added as key
		GenRowStruct grs = this.store.getNoun(this.keysToGet[0]);
		if (grs != null && !grs.isEmpty()) {
			int size = grs.size();
			for (int i = 0; i < size; i++) {
				databaseIds.add(grs.get(i).toString());
			}
			return databaseIds;
		}

		// no key is added, grab all inputs
		int size = this.curRow.size();
		for (int i = 0; i < size; i++) {
			databaseIds.add(this.curRow.get(i).toString());
		}
		return databaseIds;
	}
}
