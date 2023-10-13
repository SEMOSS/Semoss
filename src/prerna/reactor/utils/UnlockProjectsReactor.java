package prerna.reactor.utils;

import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.locks.ReentrantLock;

import prerna.auth.User;
import prerna.auth.utils.SecurityAdminUtils;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.ProjectSyncUtility;

public class UnlockProjectsReactor extends AbstractReactor {

	public UnlockProjectsReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.PROJECT.getKey() };
	}
	
	@Override
	public NounMetadata execute() {
		User user = this.insight.getUser();
		SecurityAdminUtils adminUtils = SecurityAdminUtils.getInstance(user);
		if(adminUtils == null) {
			throw new IllegalArgumentException("User must be an admin to perform this function");
		}
		
		organizeKeys();
		String projectId = this.keyValue.get(this.keysToGet[0]);
		
		boolean retBool = false;
		ConcurrentMap<String, ReentrantLock> locks = ProjectSyncUtility.getAllLocks(this.insight.getUser());
		if(projectId == null) {
			// unlock any current locks in use
			for(String key : locks.keySet()) {
				locks.get(key).unlock();
			}
			locks.clear();
			retBool = true;
		} else {
			ReentrantLock lock = locks.remove(projectId);
			lock.unlock();
			retBool = true;
		}

		return new NounMetadata(retBool, PixelDataType.BOOLEAN);
	}
	
}
