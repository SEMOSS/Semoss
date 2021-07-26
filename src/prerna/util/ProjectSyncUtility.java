package prerna.util;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.locks.ReentrantLock;

import prerna.auth.User;
import prerna.auth.utils.SecurityAdminUtils;

public class ProjectSyncUtility {

	private static ConcurrentMap<String, ReentrantLock> prorjectLocks = new ConcurrentHashMap<>();

	public static ReentrantLock getProjectLock(String projectId) {
		prorjectLocks.putIfAbsent(projectId, new ReentrantLock());
		return prorjectLocks.get(projectId);
	}
	
	public static ConcurrentMap<String, ReentrantLock> getAllLocks(User user) {
		if(!SecurityAdminUtils.userIsAdmin(user)) {
			throw new IllegalArgumentException("User must be an admin to perform this method");
		}
		return prorjectLocks;
	}
	
}
