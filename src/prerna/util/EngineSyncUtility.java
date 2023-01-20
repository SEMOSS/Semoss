package prerna.util;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.locks.ReentrantLock;

import prerna.auth.User;
import prerna.auth.utils.SecurityAdminUtils;

public class EngineSyncUtility {

	private static ConcurrentMap<String, ReentrantLock> engineLocks = new ConcurrentHashMap<>();

	// store some database metadata as well
	public static ConcurrentMap<String, List<Object[]>> databaseStructureCache = new ConcurrentHashMap<>();
	public static ConcurrentMap<String, Map<String, Object>> metamodelCache = new ConcurrentHashMap<>();
	public static ConcurrentMap<String, Map<String, List<String>>> metamodelLogicalNamesCache = new ConcurrentHashMap<>();
	public static ConcurrentMap<String, Map<String, String>> metamodelDescriptionsCache = new ConcurrentHashMap<>();
	public static ConcurrentMap<String, Map<String, Object>> metamodelPositionsCache = new ConcurrentHashMap<>();

	public static ReentrantLock getEngineLock(String engineId) {
		engineLocks.putIfAbsent(engineId, new ReentrantLock());
		return engineLocks.get(engineId);
	}
	
	public static ConcurrentMap<String, ReentrantLock> getAllLocks(User user) {
		if(!SecurityAdminUtils.userIsAdmin(user)) {
			throw new IllegalArgumentException("User must be an admin to perform this method");
		}
		return engineLocks;
	}
	
	/**
	 * Get the database structure cache
	 * @param engineId
	 * @return
	 */
	public static List<Object[]> getDatabaseStructureCache(String engineId) {
		return databaseStructureCache.get(engineId);
	}
	
	/**
	 * Set the database structure cache
	 * @param engineId
	 * @param cacheObj
	 */
	public static void setDatabaseStructureCache(String engineId, List<Object[]> cacheObj) {
		databaseStructureCache.put(engineId, cacheObj);
	}
	
	/**
	 * Get the engine metamodel
	 * @param engineId
	 * @return
	 */
	public static Map<String, Object> getMetamodel(String engineId) {
		return metamodelCache.get(engineId);
	}
	
	/**
	 * Set the engine metamodel
	 * @param engineId
	 * @param cacheObj
	 */
	public static void setMetamodel(String engineId, Map<String, Object> cacheObj) {
		metamodelCache.put(engineId, cacheObj);
	}
	
	/**
	 * Get the engine logical names
	 * @param engineId
	 * @return
	 */
	public static Map<String, List<String>> getMetamodelLogicalNamesCache(String engineId) {
		return metamodelLogicalNamesCache.get(engineId);
	}
	
	/**
	 * Set the engine logical names
	 * @param engineId
	 * @param cacheObj
	 */
	public static void setMetamodelLogicalNames(String engineId, Map<String, List<String>> cacheObj) {
		metamodelLogicalNamesCache.put(engineId, cacheObj);
	}
	
	/**
	 * Get the engine descriptions
	 * @param engineId
	 * @return
	 */
	public static Map<String, String> getMetamodelDescriptionsCache(String engineId) {
		return metamodelDescriptionsCache.get(engineId);
	}
	
	/**
	 * Set the engine descriptions
	 * @param engineId
	 * @param cacheObj
	 */
	public static void setMetamodelDescriptions(String engineId, Map<String, String> cacheObj) {
		metamodelDescriptionsCache.put(engineId, cacheObj);
	}
	
	/**
	 * 
	 * @param engineId
	 * @return
	 */
	public static Map<String, Object> getMetamodelPositions(String engineId) {
		return metamodelPositionsCache.get(engineId);
	}
	
	/**
	 * 
	 * @param engineId
	 * @param cacheObj
	 */
	public static void setMetamodelPositions(String engineId, Map<String, Object> cacheObj) {
		metamodelPositionsCache.put(engineId, cacheObj);
	}
	
	/**
	 * Clear all the cached data for an engine
	 * @param engineId
	 */
	//TODO: should make individual specific methods for more optimization
	public static void clearEngineCache(String engineId) {
		databaseStructureCache.remove(engineId);
		// metamodel
		metamodelCache.remove(engineId);
		metamodelLogicalNamesCache.remove(engineId);
		metamodelDescriptionsCache.remove(engineId);
		metamodelPositionsCache.remove(engineId);
	}
	
}
