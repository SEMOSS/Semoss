package prerna.util;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.locks.ReentrantLock;

import prerna.auth.User;
import prerna.auth.utils.SecurityAdminUtils;

public class EngineSyncUtility {

	private static ConcurrentMap<String, ReentrantLock> engineLocks = new ConcurrentHashMap<>();

	public static ReentrantLock getEngineLock(String engineName) {
		engineLocks.putIfAbsent(engineName, new ReentrantLock());
		return engineLocks.get(engineName);
	}
	
	public static ConcurrentMap<String, ReentrantLock> getAllLocks(User user) {
		if(!SecurityAdminUtils.userIsAdmin(user)) {
			throw new IllegalArgumentException("User must be an admin to perform this method");
		}
		return engineLocks;
	}
}
